# cons-oneroster-sms-storage.rb


# Service reads validated JSON messages from the One Roster validation service.
# Parses mesages to estabish primary id, and then simply saves whole message to key value storage
# with id as key and message as value.

# Uses the Moneta abstraction driver which allows for unified simple put/get interface over any number of 
# key value stores. Default implementation for NIAS will use LMDB, but users are free to use any of the list supported 
# by the moneta driver, which should alow for cross-platform support.

# Note most embedded databases and key/value stores are binary distributions, so need to be installed on the target platform independently.

require 'json'
require 'poseidon'
require 'hashids'
require 'moneta'
require_relative '../../kafkaconsumers'

@inbound = 'oneroster.validated'

@store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')

@idgen = Hashids.new( 'nsip random temp uid' )

@service_name = 'cons-oneroster-sms-storage'

# create consumer
#consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, @inbound, 0, :latest_offset)
consumer = KafkaConsumers.new(@service_name, @inbound)
Signal.trap("INT") { consumer.interrupt }



=begin
loop do

    begin
=end
        messages = []
        messages = consumer.fetch
        outbound_messages = []
        consumer.each do |m|

            # create 'empty' index tuple, otherids and links will be unused here but keeps all parsing code consistent
            idx = { :type => nil, :id => @idgen.encode( rand(1...999) ) }

            # read JSON message
            idx_hash = JSON.parse( m.value )

            idx[:id] = idx[:type] == 'oneroster_demographics' ? idx_hash["userSourcedId"] :  idx_hash["sourcedId"]

            #puts "\nStorage Index = #{idx.to_json}\n\n"

            # write the message to storage with its own refid as the key
            # puts "\n\nkey value pair will be:\n\nKEY: #{idx[:id]}\n\nVALUE:\n\n#{nodes.to_s}"

            @store["#{idx[:id]}"] = m.value
        end

=begin
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






=end

