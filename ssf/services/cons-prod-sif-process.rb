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
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


producers = KafkaProducers.new(@servicename, 10)

# if the topic of the message is one of these, a different microservice will process the SIF/XML

@skip_topics = %w(naplan.sifxml naplan.sifxmlout)

        outbound_messages = []
        messages = []
        #messages = consumer.fetch
        consumer.each do |m|
            #puts "Validate: processing message no.: #{m.offset}, #{m.key} from #{@inbound}\n\n"

            # Payload from sifxml.ingest contains as its first line a header line with the original topic
            header = m.value.lines[0]	
            payload = m.value.lines[1..-1].join
            topic = header[/TOPIC: (\S+)/, 1]
            next if @skip_topics.include?(topic)
            item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", m.value, item_key ) 
	    producers.send_through_queue(outbound_messages)
            outbound_messages = []
end
