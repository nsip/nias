# cons-prod-sif-ingest.rb

require 'poseidon'
require 'nokogiri' # xml support

# consumer of ingest SIF/XML messages. Each object in the stream is validated. Two streams of SIF created:
# * error stream, with malformed objects and associated error messages
# * validated stream of parsed SIF objects
# Messages are received from the single stream sifxml.ingest. The key of the received message is "topic"."stream". 
# Messages are output to "topic"."stream".validated and "topic"."stream".errors


@inbound = 'sifxml.ingest'
@outbound1 = 'sifxml.validated'
@outbound2 = 'sifxml.errors'


@xsd = Nokogiri::XML::Schema(File.open("#{__dir__}/xsd/sif1.3/SIF_Message1.3_3.x.xsd"))
@namespace = 'http://www.sifassociation.org/au/datamodel/1.3'

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
      	    puts "Validate: processing message no.: #{m.offset}, #{m.key}\n\n"

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
			doc.xpath("/*/node()").each do |x|
				root = x.xpath("local-name(/)")
				parent = Nokogiri::XML::Node.new root+"s", doc
				parent.default_namespace = @namespace
				x.parent = parent
				xsd_errors = @xsd.validate(parent.document)
				if(xsd_errors.empty?) 
puts "Validated!"
	      				item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
					outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", header + x.to_s, item_key ) 
				else
puts "Invalid!"
					outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", header + "Message #{m.offset} validity error:\n" + xsd_errors.map{|e| e.message}.join("\n") + "\n" + parent.document.to_s, "invalid" )
				end
			end
		else
puts "Not Well-Formed!"
			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", header + "Message #{m.offset} well-formedness error:\n" + doc.errors.join("\n") + "\n" + m.value, "invalid" )
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
    puts "\ncons-prod-sif-ingest service shutting down...\n\n"
    consumer.close
    exit 130 
  } 

  sleep 1
end


