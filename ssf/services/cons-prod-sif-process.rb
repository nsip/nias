# cons-prod-sif-process.rb

require 'poseidon'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'

# Process any received SIF objects in sifxml.validated, and post them to sifxml.processed
# This script passes the objects through. Other microservices may alter the content, depending on their topic
# This script must specify all topics which are not passed through

@inbound = 'sifxml.validated'
@outbound = 'sifxml.processed'

@servicename = 'cons-prod-sif-process'

# create consumer
#consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


producers = KafkaProducers.new(@servicename, 10)
#pool = producers.get_producers.cycle

# if the topic of the message is one of these, a different microservice will process the SIF/XML

@skip_topics = %w(naplan.sifxml naplan.sifxmlout)

=begin
loop do
    begin
=end
        outbound_messages = []
        outbound_errors = []
        messages = []
        #messages = consumer.fetch
        consumer.each do |m|
            #puts "Validate: processing message no.: #{m.offset}, #{m.key} from #{@inbound}\n\n"

            # Payload from sifxml.ingest contains as its first line a header line with the original topic
            header = m.value.lines[0]	
            payload = m.value.lines[1..-1].join
            topic = header.chomp.gsub(/TOPIC: /,"")
            next if @skip_topics.include?(topic)
            item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", m.value, item_key ) 
            #pool.next.send_messages(outbound_messages)
	    producers.send_through_queue(outbound_messages)
            outbound_messages = []
=begin
        #end

        outbound_messages.each_slice(20) do | batch |
            pool.next.send_messages( batch )
        end
=end
end
=begin
        
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

=end
