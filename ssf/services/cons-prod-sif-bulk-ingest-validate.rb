# cons-prod-sif-bulk-ingest.rb

require 'poseidon'
require 'hashids'
require 'nokogiri' # xml support
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'
require_relative '../../niaserror'

# Consumer of bulk ingest SIF/XML messages. 
# The XSD to be used for parsing SIF/XML is passed in as the first command line parameter of the script.

# Input stream sifxml/bulkingest consists of XML payload broken up into 1 MB chunks. Payload
# is reassembled and then validated, following the SIF-AU 3.4 schema.
# Two streams of SIF created:
# * error stream sifxml/errors, with malformed objects and associated error messages
# * validated stream sifxml/validated of parsed SIF objects
# The key of the received message is "topic"."stream", reflecting the original topic and stream of the message. 

@inbound = 'sifxml.bulkingest'
@outbound1 = 'sifxml.validated'
@outbound2 = 'sifxml.errors'

@servicename = 'cons-prod-sif-bulk-ingest-validate'

#@xsd = Nokogiri::XML::Schema(File.open("#{__dir__}/xsd/sif3.4/SIF_Message3.4.xsd"))
@xsd = Nokogiri::XML::Schema(File.open(ARGF.argv[0]))
@namespace = 'http://www.sifassociation.org/au/datamodel/3.4'
@hashid = Hashids.new( 'nias file upload' ) # hash ID for current file

# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


def header(node_ordinal, node_count, destination_topic, doc_id)
	return "TOPIC: #{destination_topic} #{node_ordinal}:#{node_count}:#{doc_id}\n"
end


producers = KafkaProducers.new(@servicename, 10)

payload = ""
destination_topic = ""
concatcount = 0

        outbound_messages = []
        messages = []
        consumer.each do |m|
            cont = m.value.match( /===snip[^=\n]*===/ ) #this is not the last in the suite of split up messages
            m.value.gsub!(/\n===snip[^=\n]*===\n/, "")
            if(payload.empty?) then
                # Payload from sifxml.bulkingest contains as its first line a header line with the original topic
                destination_topic = m.value.lines[0][/TOPIC: (\S+)/, 1]
                payload = m.value.lines[1..-1].join
                start = Time.now
            else
                payload << m.value
            end
            concatcount = concatcount + 1
            if (cont) then
                next
            end
            #puts "Concatenation done at #{Time.now}"
            #puts "Payload size: #{payload.size}"
            #puts "Validate: processing message no.: #{m.offset}, #{m.key}\n\n"

            #File.open('log.txt', 'w') {|f| f.puts payload }

            # each ingest message is a group of objects of the same class, e.g. 
            # <StudentPersonals> <StudentPersonal>...</StudentPersonal> <StudentPersonal>...</StudentPersonal> </StudentPersonals>
	    # HOWEVER: <BulkIngest> element has been added ad hoc, and allows an arbitrary sequence of all objects
            # If message is not well-formed, pass it to sifxml.errors as a unit
            # If message is well-formed, break it up into its constituent objects, and parse each separately
            # This allows us to bypass the SIF constraint that all objects must be of the same type

            start = Time.now
	    doc_id = payload[/<!-- CSV docid (\S+)/, 1]
	    doc_id = @hashid.encode(rand(10000000)) unless doc_id
	    payload_id = payload[/<!-- CSV docid (\S+)/, 1]
	    payload_id = 0 unless payload_id
            doc = Nokogiri::XML(payload) do |config|
                config.nonet.noblanks
            end
            puts "Parsing took #{Time.now - start}"

	    lines = []
            if(doc.errors.empty?) 
                puts "Payload well-formed. Payload size: #{payload.size}. Validating...";
                #			xsd_errors = @xsd.validate(parent.document)

                doc.root.add_namespace nil, @namespace
                start = Time.now
                xsd_errors = @xsd.validate(doc)
                puts "XSD validation took #{Time.now - start}"
                if(xsd_errors.empty?) 
                    puts "Validated! "
		    nodes = doc.xpath("/*/node()")
	    	    recordcount = payload[/<!-- CSV linetotal (\S+)/, 1]
	    	    recordcount = nodes.length unless recordcount
                    nodes.each_with_index do |x, i|
                        if (i%10000 == 0 and i > 0) then 
                            puts "#{i} records queued..." 
                        end
                        item_key = "rcvd:#{ sprintf('%09d', m.offset) }"
                        x["xmlns"] = @namespace
                        msg = header(i, recordcount, destination_topic, doc_id) + x.to_s
                        outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", msg, item_key ) 
			if outbound_messages.length > 100 
            			producers.send_through_queue ( outbound_messages )
        			outbound_messages = []
			end
                    end
                    outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", 
				NiasError.new(0, 0, 0, "XSD Validation Error", nil).to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, 0) }" )
                    outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", 
				NiasError.new(0, 0, 0, "XML Well-Formedness Error", nil).to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, 1) }" )
                else
		    lines = payload.lines
                    msg = "Message #{m.offset} validity error:\n" 
                    #puts msg
	            xsd_errors.each_with_index do |e, i|
			output = "#{msg}Line #{e.line}: #{e.message} \n...\n#{lines[e.line - 3 .. e.line + 1].join("")}...\n"
			#puts output
                    	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", 
				NiasError.new(i, xsd_errors.length, payload_id, "XSD Validation Error", output).to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, i) }" )
			if outbound_messages.length > 100 
            			producers.send_through_queue( outbound_messages )
        			outbound_messages = []
			end
		    end
                end
            else
                puts "Not Well-Formed!"
		lines = payload.lines
                msg = "Message #{m.offset} well-formedness error:\n" 
		#puts msg
                doc.errors.each_with_index do |e, i| 
			output = "#{msg}Line #{e.line}: #{e.message} \n...\n#{lines[e.line - 3 .. e.line + 1].join("")}...\n"
                    	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", 
				NiasError.new(i, doc.errors.length, payload_id, "XML Well-Formedness Error", output).to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, i)}" )
			if outbound_messages.length > 100 
            			producers.send_through_queue( outbound_messages )
        			outbound_messages = []
			end
		end
            end
            payload = "" # clear payload for next iteration
            concatcount = 0
                        puts "Finished processing payload."

        # debugging if needed
        # outbound_messages.each do | msg |
        # 	puts "\n\nSending to: #{msg.topic}\n"
        # 	puts "\n\nKey: #{msg.key}\n"
        # 	puts "\n\nContent: #{msg.value}"
        # end

        producers.send_through_queue( outbound_messages )
	outbound_messages = []
end

