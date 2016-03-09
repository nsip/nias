# cons-prod-file-report.rb

# Report on contents of a single payload (as marked by doc ID in message header) to naplan.srm_errors
# We assume that the payloads  are loaded in sequence, and do not overlap.

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'
require_relative '../../niaserror'

@inbound = 'sifxml.processed'
@outbound = 'naplan.srm_errors'

@servicename = 'cons-prod-file-report'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


producers = KafkaProducers.new(@servicename, 10)

@naplan_topics = %w(naplan.sifxml naplan.sifxml_staff naplan.sifxmlout naplan.sifxmlout_staff)

        outbound_messages = []
	# hash of record counts per payload
	payloads = {}

	recordid = 0

        consumer.each do |m|
	    recordid = recordid + 1
	    header = m.value.lines[0]
            topic = header[/TOPIC: (\S+)/, 1]
            payload = m.value.lines[1..-1].join
            # we are only interested in XML in NAPLAN topics
            next unless @naplan_topics.grep(topic) 
	    
	    node_id, count, doc_id = header.match(/^TOPIC: \S+ ([0-9]+):([0-9]+):(\S+)\n/).captures
	    payloads[doc_id] = {
			:total => Integer(count), :records_seen => 0, :schools => Set.new, :students => Set.new, 
			:yr3 => Set.new, :yr5 => Set.new, :yr7 => Set.new, :yr9 => Set.new,
		} unless payloads[doc_id]

            # read xml message
            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end      		
            type = nodes.root.name
            next unless type == 'StudentPersonal'

	    	payloads[doc_id][:records_seen] = payloads[doc_id][:records_seen] + 1
	
		aslSchoolId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId").to_s
		localid = aslSchoolId  + "::" + CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId").to_s
		testlevel = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel/xmlns:Code").to_s

		payloads[doc_id][:schools] << aslSchoolId
		payloads[doc_id][:students] << localid
		case testlevel
			when "3"
				payloads[doc_id][:yr3] << localid
			when "5"
				payloads[doc_id][:yr5] << localid
			when "7"
				payloads[doc_id][:yr7] << localid
			when "9"
				payloads[doc_id][:yr9] << localid
		end


		if(payloads[doc_id][:records_seen] == payloads[doc_id][:total])
			msg = <<MSG
Report for payload #{doc_id}

Schools: #{ payloads[doc_id][:schools].size }
Students: #{ payloads[doc_id][:students].size }
Year 3: #{ payloads[doc_id][:yr3].size }
Year 5: #{ payloads[doc_id][:yr5].size }
Year 7: #{ payloads[doc_id][:yr7].size }
Year 9: #{ payloads[doc_id][:yr9].size }

MSG
puts msg
        	    	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", 
				NiasError.new(0, 0, 0, "Report", msg).to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, 0)}" )
        		producers.send_through_queue( outbound_messages )
			outbound_messages = []
		end
	end
