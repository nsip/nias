# cons-prod-sif-parser.rb

# Simple consumer that reads OneRoster messages from an input stream.
# OneRoster messages are assumed to have been converted from CSV into serialised JSON.
# 
# Parses message to find refid & type of message, and to build index of
# all other references contained in the xml message.
# 
# Extracts GUID id (RefID), other ids, equivalent ids, type, label and [links] from each message 
#
# e.g. <refid> [OtherIdType => OtherId] <StudentSchoolEnrolment> [<StudentPersonalRefId><SchoolInfoRefId>] 
# 
# this  [ 'tuple' id - [equvalent-ids] - {otherids} - type - [links] - label ]
# 
# is then passed on to the sms indexing service
# 
# This is done so that indexer only deals with abstract tuples of this type, which can therefore come
# from ANY parsed input; doesn't have to be SIF messages, can be IMS, CSV etc. etc.
# 

require 'json'
require 'poseidon'
require 'hashids'

@inbound = 'oneroster.validated'
@outbound = 'sms.indexer'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new("cons-prod-oneroster-parser", "localhost", 9092,
                                           @inbound, 0, :latest_offset)


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], "cons-prod-oneroster-parser", {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
@pool = producers.cycle

loop do

  begin
  		messages = []
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|

	    	# create 'empty' index tuple
			idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => [], :equivalentids => [], :label => nil }      	


      		# read JSON message
		idx_hash = JSON.parse( m.value )

               # type of converted CSV One Roster record depends on presence of particular field
                idx[:type] = 'oneroster_orgs' if idx_hash.has_key?("metadata.boarding")
                idx[:type] = 'oneroster_users' if idx_hash.has_key?("username")
                idx[:type] = 'oneroster_courses' if idx_hash.has_key?("courseCode")
                idx[:type] = 'oneroster_classes' if idx_hash.has_key?("classCode")
                idx[:type] = 'oneroster_enrollments' if idx_hash.has_key?("primary")
                idx[:type] = 'oneroster_academicSessions' if idx_hash.has_key?("startDate")
                idx[:type] = 'oneroster_demographics' if idx_hash.has_key?("sex")

		case idx[:type]
		when 'oneroster_orgs'
			idx[:label] = idx_hash["name"]
		when 'oneroster_users'
			idx[:label] = idx_hash["givenName"] + " " + idx_hash["familyName"]
		when 'oneroster_courses'
			idx[:label] = idx_hash["courseCode"].empty? ? idx_hash["title"] : idx_hash["courseCode"]
		when 'oneroster_classes'
			idx[:label] = idx_hash["classCode"].empty? ? idx_hash["title"] : idx_hash["classCode"]
		when 'oneroster_academicSessions'
			idx[:label] = idx_hash["title"]
		else
			idx[:label] = idx[:id]
		end

                        idx[:id] = idx[:type] == 'oneroster_demographics' ? idx_hash["userSourcedId"] :  idx_hash["sourcedId"]

			if(idx_hash.has_key?("parentSourcedId")) 
				idx[:links] << idx_hash["parentSourcedId"]
			end
			if(idx_hash.has_key?("orgSourcedId")) 
				idx[:links] << idx_hash["orgSourcedId"]
			end
			if(idx_hash.has_key?("courseSourcedId")) 
				idx[:links] << idx_hash["courseSourcedId"]
			end
			if(idx_hash.has_key?("schoolSourcedId")) 
				idx[:links] << idx_hash["schoolSourcedId"]
			end
			if(idx_hash.has_key?("termSourcedId")) 
				idx[:links] << idx_hash["termSourcedId"]
			end
			if(idx_hash.has_key?("classSourcedId")) 
				idx[:links] << idx_hash["classSourcedId"]
			end
			if(idx_hash.has_key?("userSourcedId")) 
				idx[:links] << idx_hash["userSourcedId"]
			end
			if(idx_hash.has_key?("orgSourcedIds")) 
				idx_hash["orgSourcedIds"].split(',').each { |x|
					idx[:links] << x
				}
			end
			if(idx_hash.has_key?("agents") and not idx_hash["agents"].nil? )
				idx_hash["agents"].split(',').each { |x|
					idx[:links] << x
				}
			end


			# other identifiers
			if(idx_hash.has_key?("identifier") and idx[:type] == 'oneroster_orgs')
				idx[:otherids][:oneroster_identifier] = idx_hash["identifier"]
			end
			if(idx_hash.has_key?("userId") and idx[:type] == 'oneroster_users') 
				idx[:otherids][:oneroster_userId] = idx_hash["userId"]
			end
			if(idx_hash.has_key?("identifier") and idx[:type] == 'oneroster_users') 
				idx[:otherids][:oneroster_identifier] = idx_hash["identifier"]
			end
			if(idx_hash.has_key?("courseCode") and idx[:type] == 'oneroster_courses') 
				idx[:otherids][:oneroster_courseCode] = idx_hash["courseCode"]
			end
			if(idx_hash.has_key?("classCode") and idx[:type] == 'oneroster_classes') 
				idx[:otherids][:oneroster_classCode] = idx_hash["classCode"]
			end



			#puts "\nParser Index = #{idx.to_json}\n\n"

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
  		
  		end

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
			@pool.next.send_messages( batch )
	   	end


      # puts "cons-prod-oneroster-parser: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@servicename} service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end





