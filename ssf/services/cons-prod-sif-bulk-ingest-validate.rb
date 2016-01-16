# cons-prod-sif-bulk-ingest.rb

require 'poseidon'
require 'nokogiri' # xml support

# consumer of bulk ingest SIF/XML messages. 
# Input stream consists of XML payload broken up into 1 MB chunks. Payload
# is reassembled and then validated.
# Two streams of SIF created:
# * error stream, with malformed objects and associated error messages
# * validated stream of parsed SIF objects
# Messages are received from the single stream sifxml.bulkingest. The key of the received message is "topic"."stream". 
# Messages are output to "topic"."stream".validated and "topic"."stream".errors


@inbound = 'sifxml.bulkingest'
@outbound1 = 'sifxml.validated'
@outbound2 = 'sifxml.errors'

@servicename = 'cons-prod-sif-bulk-ingest-validate'

@xsd = Nokogiri::XML::Schema(File.open("#{__dir__}/xsd/sif3.4/SIF_Message3.4.xsd"))
@namespace = 'http://www.sifassociation.org/au/datamodel/3.4'

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
                                           @inbound, 0, :latest_offset )


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
pool = producers.cycle

payload = ""
header = ""
concatcount = 0

loop do
  begin
  	    outbound_messages = []
  	    outbound_errors = []
  	    messages = []
	    messages = consumer.fetch
	    messages.each do |m|

	    if(payload.empty?) then
	        # Payload from sifxml.bulkingest contains as its first line a header line with the original topic
	        header = m.value.lines[0]	
puts header
	        payload = m.value.lines[1..-1].join
		start = Time.now
puts "Started message at #{start}"
	    else
	    	payload << m.value
	    end
	    concatcount = concatcount + 1
	    if payload.match( /===snip===/ ) then
	    	payload = payload.gsub(/\n===snip===\n/, "")
		puts "Concatenating #{concatcount} messages. Payload size: #{payload.size / 1000}..."
	    	next
	    end
#puts "Concatenating #{concatcount} messages..."
#next
		puts "Concatenation done at #{Time.now}"
		puts "Payload size: #{payload.size}"
  	    puts "Validate: processing message no.: #{m.offset}, #{m.key}\n\n"

		# each ingest message is a group of objects of the same class, e.g. 
		# <StudentPersonals> <StudentPersonal>...</StudentPersonal> <StudentPersonal>...</StudentPersonal> </StudentPersonals>
		# If message is not well-formed, pass it to sifxml.errors as a unit
		# If message is well-formed, break it up into its constituent objects, and parse each separately
		# This allows us to bypass the SIF constraint that all objects must be of the same type

		start = Time.now
		doc = Nokogiri::XML(payload) do |config|
        		config.nonet.noblanks
		end
		puts "Parsing took #{Time.now - start}"

		if(doc.errors.empty?) 
			puts "Payload well-formed. Payload size: #{payload.size}. Validating...";
#			xsd_errors = @xsd.validate(parent.document)
			doc.root.add_namespace nil, @namespace
			start = Time.now
			xsd_errors = @xsd.validate(doc)
			puts "XSD validation took #{Time.now - start}"
			if(xsd_errors.empty?) 
				puts "Validated! "
				doc.xpath("/*/node()").each_with_index do |x, i|
					if (i%10000 == 0 and i > 0) then 
						puts "#{i} records queued..." 
					end
					#root = x.xpath("local-name(/)")
					#parent = Nokogiri::XML::Node.new root+"s", doc
					#parent.default_namespace = @namespace
					#x.parent = parent
	      				item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
	      				msg = header + x.to_s
					outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", msg, item_key ) 
				end
			else
				puts "Invalid!"
				msg = header + "Message #{m.offset} validity error:\n" 
				msg << xsd_errors.map{|e| e.message + "\n...\n" + payload.lines[e.line - 3 .. e.line + 1].join("") + "...\n"}.join("\n") + "\n" 
					# puts "\n\nsending to: #{@outbound2}\n\nmessage:\n\n#{msg}\n\nkey: 'invalid'\n\n"
				puts msg
				outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", msg , "invalid" )
			end
		else
			puts "Not Well-Formed!"
			msg = header + "Message #{m.offset} well-formedness error:\n" + 
					doc.errors.map{|e| e.message + "\n...\n" + 
					payload.lines[e.line - 3 .. e.line + 1].join("") +
					"...\n"}.join("\n") + "\n" 
				#doc.errors.join("\n") + "\n" 	
			# puts "\n\nsending to: #{@outbound2}\n\nmessage:\n\n#{msg}\n\nkey: 'invalid'\n\n"
			puts msg
			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", msg, "invalid" )
		end
		payload = "" # clear payload for next iteration
		concatcount = 0
		
		puts "Finished processing payload."
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
	

	#	puts "cons-prod-ingest:: Resuming message consumption from: #{consumer.next_offset}"
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

  #sleep 1
end


