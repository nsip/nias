# cons-prod-sif-parser.rb

# simple consumer that reads sif xml messages from an input stream
# assumes that sif messages have already been validated for xml
# 
# parses message to find refid & type of message and to build index of
# all other references contained in the xml message
# 
# Extracts GUID id (RefID), other ids, type and [links] from each message 
#
# e.g. <refid> [OtherIdType => OtherId] <StudentSchoolEnrolment> [<StudentPersonalRefId><SchoolInfoRefId>] 
# 
# this  [ 'tuple' id - {otherids} - type - [links] ]
# 
# is then passed on to the sms indexing service
# 
# this is done so that indexer only deals with abstract tuples of this type, which can therefore come
# from ANY parsed input; doesn't have to be SIF messages, cna be IMS, CSV etc. etc.
# 

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'

@inbound = 'sifxml.validated'
@outbound = 'sms.indexer'

@servicename = 'cons-prod-sif-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
                                           @inbound, 0, :latest_offset)


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
@pool = producers.cycle

loop do

  begin
  		messages = []
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|

	    	# create 'empty' index tuple
			idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => []}      	

		header = m.value.lines[0]
                payload = m.value.lines[1..-1].join

      		# read xml message
      		nodes = Nokogiri::XML( payload ) do |config|
        		config.nonet.noblanks
			end      		

			# for rare nodes like StudentContactRelationship can be no mandatory refid
			# optional refid will already be captured in [links] as child node
			# but need to parse for the object type and assign the optional refid back to the object
			
			# type is always first node
			idx[:type] = nodes.root.name

			# concatenate name and see id refid exists, if not create a random one
			refs = nodes.css( "#{nodes.root.name}RefId" )
			idx[:id] = refs.children.first unless refs.empty?

			# ...now deal with vast majority of normal sif xml types

			# get any pure refids
			root_types = nodes.xpath("//@RefId")  
			root_types.each do | node |
				# puts node.parent.name
				# puts node.child
				# puts "\n\nType: #{node.parent.name} - ID: #{node.child}\n\n"
				idx[:type] = node.parent.name
				idx[:id] = node.child
			end

			# any node attributes that have refid suffix
			references = nodes.xpath( "//@*[substring(name(), string-length(name()) - 4) = 'RefId']" )
			references.each do | node |
				# puts node.name
				# puts node.content
				idx[:links] << node.content
			end

			# any nodes that have refid suffix
			references = nodes.xpath( "//*[substring(name(), string-length(name()) - 4) = 'RefId']" )
			references.each do | node |
				# puts node.name
				# puts node.content
				idx[:links] << node.content
			end

			# any objects that are reference objects
			ref_objects = nodes.xpath("//@SIF_RefObject")
			ref_objects.each do | node |
				# puts node.child
				# puts node.parent.content
				idx[:links] << node.parent.content
			end

			# any LocalIds
			localids = nodes.xpath("//LocalId")
			localids.each do |node|
				idx[:otherids][:localid] = node.child
			end

			# any StateProvinceIds
			stateprovinceids = nodes.xpath("//StateProvinceId")
			stateprovinceids.each do |node|
				idx[:otherids][:stateprovinceids] = node.child
			end

			# any ACARAIds
			acaraids = nodes.xpath("//ACARAId")
			acaraids.each do |node|
				idx[:otherids][:acaraids] = node.child
			end

			# any Electronic IDs
			electronicids = nodes.xpath("//ElectronicIdList/ElectronicId")
			electronicids.each do |node|
				idx[:otherids]["electronicid"+node.attribute("Type")] = node.child
			end

			# any Other IDs
			otherids = nodes.xpath("//OtherIdList/OtherId")
			otherids.each do |node|
				idx[:otherids][node.attribute("Type")] = node.child
			end

			puts "\nParser Index = #{idx.to_json}\n\n"

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
  		
  		end

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
			@pool.next.send_messages( batch )
	   	end


      # puts "cons-prod-sif-parser: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@servicename} service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end





