# cons-sms-storage.rb


# service reads validated xml messages from the sif validation service.
# Parses mesages to estabish primary id, and then simply saves whole message to key value storage
# with id as key and message as value.

# Uses the Moneta abstraction driver which allows for unified simple put/get interface over any number of 
# key value stores. Default implementation for NIAS will use LMDB, but users are free to use any of the list supported 
# by hte moneta driver, which should alow for cross-platform support.
# 

# Note most embedded databases and k/v stores are binary distriutions so need to be installed on the target platform independently.

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'
require 'moneta'

@inbound = 'sifxml.validated'

@store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages', mapsize: 1_000_000_000)

@idgen = Hashids.new( 'nsip random temp uid' )

@service_name = 'cons-sms-storage'

# create consumer
consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092,
                                           @inbound, 0, :latest_offset)


loop do

  begin
  		messages = []
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|

	    	# create 'empty' index tuple, otherids and links will be unused here but keeps all parsing code consistent
		idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => [], :equivalentids => [], :label => nil}   

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

			puts "\nStorage Index = #{idx.to_json}\n\n"

			# write the message to storage with its own refid as the key
			# puts "\n\nkey value pair will be:\n\nKEY: #{idx[:id]}\n\nVALUE:\n\n#{nodes.to_s}"

			@store["#{idx[:id]}"] = nodes.to_xml
  		
  		end


      # puts "#{@service_name}: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@service_name} service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end








