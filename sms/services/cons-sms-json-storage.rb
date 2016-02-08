# cons-sms-storage.rb


# service reads validated JSON messages from JSON and CSV feeds.
# Parses mesages to estabish primary id, and then simply saves whole message to key value storage
# with topic::id as key and message as value.

# Uses the Moneta abstraction driver which allows for unified simple put/get interface over any number of 
# key value stores. Default implementation for NIAS will use LMDB, but users are free to use any of the list supported 
# by the moneta driver, which should alow for cross-platform support.
# 

# Note most embedded databases and key/value stores are binary distriutions so need to be installed on the target platform independently.

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'
require 'moneta'

@inbound = 'json.storage'

@store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages', mapsize: 1_000_000_000)

@idgen = Hashids.new( 'nsip random temp uid' )

@service_name = 'cons-sms-json-storage'

# create consumer
consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, @inbound, 0, :latest_offset)

# given the topic name, extract likely unique id for the record 
def topic_to_id(topic, json) 
	case topic
	when "naplan.csv"
		id = json["LocalId"]
	when "naplan.csv_staff"
		id = json["LocalStaffId"]
	else
		# in desperation, return concatenation of all keys then values
		id = nil
	end
	if(id.nil?)
		if(Array === json)
			id = json.join('::')
		elsif(json.key?('id'))
			id = json["id"]
		else
			id = json.keys.join('::') + '--' + json.values.join('::')
		end
	end
	return id
end


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


            # read json message
            json = JSON.parse(payload)
            idx[:type] = header.chomp.gsub(/TOPIC: /,"")
            idx[:id] = topic_to_id(idx[:type], json)

            # write the message to storage with its own refid as the key
            # puts "\n\nkey value pair will be:\n\nKEY: #{idx[:id]}\n\nVALUE:\n\n#{nodes.to_s}"

            @store["#{idx[:type]}::#{idx[:id]}"] = json
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







