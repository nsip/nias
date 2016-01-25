# cons-prod-sif-ingest.rb

require 'poseidon'
require 'nokogiri' # xml support

# Consumer of bulk ingest SIF/XML messages. 
# Input stream sifxml/ingest consists of XML payload. Payload
# is reassembled and then validated, following the SIF-AU 3.4 schema.
# Two streams of SIF created:
# * error stream sifxml/errors, with malformed objects and associated error messages
# * validated stream sifxml/validated of parsed SIF objects
# The key of the received message is "topic"."stream", reflecting the original topic and stream of the message. 


@inbound = 'sifxml.ingest'
@outbound1 = 'sifxml.validated'
@outbound2 = 'sifxml.errors'

@servicename = 'cons-prod-sif-ingest-validate'

@xsd = Nokogiri::XML::Schema(File.open("#{__dir__}/xsd/sif3.4/SIF_Message3.4.xsd"))
@namespace = 'http://www.sifassociation.org/au/datamodel/3.4'

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
                                           @inbound, 0, :latest_offset)


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
pool = producers.cycle



loop do
  begin
  	    outbound_messages = []
  	    outbound_errors = []
  	    messages = []
	    messages = consumer.fetch
	    messages.each do |m|
  	    #puts "Validate: processing message no.: #{m.offset}, #{m.key}\n\n"

        # Payload from sifxml.ingest contains as its first line a header line with the original topic
        header = m.value.lines[0]	
        payload = m.value.lines[1..-1].join

		# each ingest message is a group of objects of the same class, e.g. 
		# <StudentPersonals> <StudentPersonal>...</StudentPersonal> <StudentPersonal>...</StudentPersonal> </StudentPersonals>
		# If message is not well-formed, pass it to sifxml.errors as a unit
		# If message is well-formed, break it up into its constituent objects, and parse each separately
		# This allows us to bypass the SIF constraint that all objects must be of the same type

		doc = Nokogiri::XML(payload) do |config|
        		config.nonet.noblanks
		end
		if(doc.errors.empty?) 
			doc.remove_namespaces!
			doc.xpath("/*/node()").each do |x|
				#root = x.xpath("local-name(/)")
				root = x.name()
				parent = Nokogiri::XML::Node.new root+"s", Nokogiri::XML::Document.new()
				#parent.default_namespace = @namespace
				#x.parent = parent
				#parent << x
				doc2 = Nokogiri::XML::Builder.new do |xml|
					xml.method_missing(root+"s") {
						@fs_parent = parent
					}
				end
				x.add_namespace_definition(nil, @namespace)
				doc2.parent().children[0].add_child(x)
				doc2.parent().children[0].default_namespace = @namespace
				doc3 = Nokogiri::XML(doc2.parent().canonicalize(nil, nil, 1))
				xsd_errors = @xsd.validate(doc3.document)
				if(xsd_errors.empty?) 
	      				item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
	      				msg = header + x.to_s
#puts "\n\nsending to: #{@outbound1}\n\nmessage:\n\n#{msg}\n\nkey:#{item_key}\n\n"
					outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", msg, item_key ) 
				else
					puts "Invalid!"
					msg = header + "Message #{m.offset} validity error:\n" + 	
									xsd_errors.map{|e| e.message}.join("\n") + "\n" + 
									parent.document.to_s
					puts "\n\nsending to: #{@outbound2}\n\nmessage:\n\n#{msg}\n\nkey: 'invalid'\n\n"					
					outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", 
						header + "Message #{m.offset} validity error:\n" + 	
						xsd_errors.map{|e| e.message}.join("\n") + "\n" + 
						parent.document.to_s, "invalid" )
				end
			end
		else
			puts "Not Well-Formed!"
			msg = header + "Message #{m.offset} well-formedness error:\n" + doc.errors.join("\n") + "\n" + m.value	
			# puts "\n\nsending to: #{@outbound2}\n\nmessage:\n\n#{msg}\n\nkey: 'invalid'\n\n"
			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", msg, "invalid" )
		end
		end

		# debugging if needed
		# outbound_messages.each do | msg |
		# 	puts "\n\nSending to: #{msg.topic}\n"
		# 	puts "\n\nKey: #{msg.key}\n"
		# 	puts "\n\nContent: #{msg.value}"
		# end

		outbound_messages.each_slice(20) do | batch |
			pool.next.send_messages( batch )
	   	end
	

		# puts "cons-prod-ingest:: Resuming message consumption from: #{consumer.next_offset}"
  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@servicename} service shutting down...\n\n"
    consumer.close
    exit 130 
  } 

  sleep 1
end


