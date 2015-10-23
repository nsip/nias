# cons-prod-sif-ingest.rb

require 'poseidon'
require 'nokogiri' # xml support

# consumer of ingest SIF/XML messages. Each object in the stream is validated. Two streams of SIF created:
# * error stream, with malformed objects and associated error messages
# * validated stream of parsed SIF objects
# Messages are received from the single stream sifxml.ingest. The key of the received message is "topic"."stream". 
# Messages are output to "topic"."stream".validated and "topic"."stream".errors


@inbound = 'sifxml.ingest'
@outbound1 = 'validated'
@outbound2 = 'errors'

@xsd = Nokogiri::XML::Schema(File.open("./ssf/services/SIF_Message1.3_3.x.xsd"))


# create consumer
consumer = Poseidon::PartitionConsumer.new("cons-prod-ingest", "localhost", 9092,
                                           @inbound, 0, :latest_offset)


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], "cons-prod-ingest", {:partitioner => Proc.new { |key, partition_count| 0 } })
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
      	    puts "processing message no.: #{m.offset}, #{m.key}\n\n"

		# each ingest message is a group of objects of the same class, e.g. 
		# <StudentPersonals> <StudentPersonal>...</StudentPersonal> <StudentPersonal>...</StudentPersonal> </StudentPersonals>
		# Parse each message as a unit; pass them on as individual objects

		doc = Nokogiri::XML(m.value) do |config|
        		config.nonet.noblanks
		end
		if(doc.errors.empty?) 
			xsd_errors = @xsd.validate(doc)
			if(xsd_errors.empty?) 
	      			item_key = "ts_entry:#{ sprintf('%09d', m.offset) }"
				doc.xpath("/*/node()").each { |x| outbound_messages << Poseidon::MessageToSend.new( "#{m.key}.#{@outbound1}", x.to_s, item_key ) }
			else
				outbound_messages << Poseidon::MessageToSend.new( "#{m.key}.#{@outbound2}", "Message #{m.offset} validity error:\n" + xsd_errors.map{|e| e.message}.join("\n") + "\n" + m.value, "invalid" )
			end
		else
			outbound_messages << Poseidon::MessageToSend.new( "#{m.key}.#{@outbound2}", "Message #{m.offset} well-formedness error:\n" + doc.errors.join("\n") + "\n" + m.value, "invalid" )
		end
		end

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
    puts "\ncons-prod-ingest service shutting down...\n\n"
    consumer.close
    exit 130 
  } 

  sleep 1
end


