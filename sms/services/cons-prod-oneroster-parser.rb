# cons-prod-sif-parser.rb

# simple consumer that reads one roster messages from an input stream
# one roster messages are assumed to have been converted from CSV
# 
# parses message to find refid & type of message and to build index of
# all other references contained in the xml message
# 
# Extracts GUID id (RefID), other ids, type and [links] from each message 
#
# e.g. <refid> [OtherIdType => OtherId] <StudentSchoolEnrolment> [<StudentPersonalRefId><SchoolInfoRefId>] 
# 
# this  [ 'tuple' id - {otherids} - type - [links] ]
# 
# is then passed on to the sms indexing service
# 
# this is done so that indexer only deals with abstract tuples of this type, which can therefore come
# from ANY parsed input; doesn't have to be SIF messages, cna be IMS, CSV etc. etc.
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
			idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => []}      	


      		# read JSON message
		idx_hash = JSON.parse( m.value )

               # type of converted CSV One Roster record depends on presence of particular field
                idx[:type] = 'orgs' if idx_hash.haskey?("metadata.boarding")
                idx[:type] = 'users' if idx_hash.haskey?("username")
                idx[:type] = 'courses' if idx_hash.haskey?("courseCode")
                idx[:type] = 'classes' if idx_hash.haskey?("classCode")
                idx[:type] = 'enrollments' if idx_hash.haskey?("primary")
                idx[:type] = 'academicSessions' if idx_hash.haskey?("startDate")
                idx[:type] = 'demographics' if idx_hash.haskey?("sex")

                        idx[:id] = idx[:type] == 'demographics' ? idx_hash["userSourcedId"] :  idx_hash["sourcedId"]

			if(idx_hash.haskey?("parentSourcedId")) {
				idx[:links] << idx_hash["parentSourcedId"]
			}
			if(idx_hash.haskey?("orgSourcedId")) {
				idx[:links] << idx_hash["orgSourcedId"]
			}
			if(idx_hash.haskey?("courseSourcedId")) {
				idx[:links] << idx_hash["courseSourcedId"]
			}
			if(idx_hash.haskey?("schoolSourcedId")) {
				idx[:links] << idx_hash["schoolSourcedId"]
			}
			if(idx_hash.haskey?("termSourcedId")) {
				idx[:links] << idx_hash["termSourcedId"]
			}
			if(idx_hash.haskey?("classSourcedId")) {
				idx[:links] << idx_hash["classSourcedId"]
			}
			if(idx_hash.haskey?("userSourcedId")) {
				idx[:links] << idx_hash["userSourcedId"]
			}
			if(idx_hash.haskey?("orgSourcedIds")) {
				idx_hash["orgSourcedIds"].split(',').each { |x|
					idx[:links] << x
				}
			}
			if(idx_hash.haskey?("agents")) {
				idx_hash["agents"].split(',').each { |x|
					idx[:links] << x
				}
			}


			# other identifiers
			if(idx_hash.haskey?("identifier") and idx[:type] == 'orgs') {
				idx[:otherids][:acaraids] = idx_hash["identifier"]
			}
			if(idx_hash.haskey?("userId") and idx[:type] == 'users') {
				idx[:otherids][:userId] = idx_hash["userId"]
			}
			if(idx_hash.haskey?("identifier") and idx[:type] == 'users') {
				idx[:otherids][:localid] = idx_hash["identifier"]
			}
			if(idx_hash.haskey?("courseCode") and idx[:type] == 'courses') {
				idx[:otherids][:localid] = idx_hash["courseCode"]
			}
			if(idx_hash.haskey?("classCode") and idx[:type] == 'classes') {
				idx[:otherids][:localid] = idx_hash["classCode"]
			}



			puts "\nParser Index = #{idx.to_json}\n\n"

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
    puts "\ncons-prod-oneroster-parser service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end





