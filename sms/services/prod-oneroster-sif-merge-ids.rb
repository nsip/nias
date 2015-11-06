# prod-oneroster-sif-merge-ids.rb

# Script which reviews the local IDs in SIF records and OneRosters in Redis that constitute a match,
# and then constructs tuples requesting that the corresponding GUIDs  be  merged as equivalent
# 
# the result is always a tuple structure of the form:
#
# ( id, type, [equivalent-ids], {other-ids}, [links] ) 
#
# id: The SIF GUID
# equivalent-id: The OneRoster GUID
#
require 'json'
require 'poseidon'
require 'hashids'
require 'redis'

@outbound = 'sms.indexer'

@idgen = Hashids.new( 'nsip random temp uid' )

@redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)

@servicename = 'prod-oneroster-sif-merge-ids'

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
        p = Poseidon::Producer.new(["localhost:9092"], "prod-oneroster-sif-merge-ids", {:partitioner => Proc.new { |key, partition_count| 0 } })
        producers << p
end
@pool = producers.cycle


cursor = -1
outbound_messages = []

loop do

  begin
               # create 'empty' index tuple
                        idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => [], :equivalentids => []}    

		if(cursor != 0) then
			cursor = 0 if cursor == -1
			idscan = @redis.sscan 'other:ids', cursor
	
			puts "Read #{idscan[1].length} ids"
			cursor = idscan[0]
			idscan[1].each do |x| # for each other-id 
				# if they have a SIF local id value and a OneRoster id value that are the same, then we have identified
				# a match between their corresponding guids
				#puts ">> " + @redis.hgetall(x).to_s
				match = @redis.hmget x, 'oneroster_identifier', 'oneroster_userId', 'oneroster_courseCode', 'oneroster_classCode', 'localid'
				#puts x + ': ' + match.join(',')
				unless(match[0].nil? or match[4].nil?) 
					idx[:id] = match[0]				
					idx[:equivalentids] = [match[4]]
					puts "\nParser Index = #{idx.to_json}\n\n"
                        		outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
				end
				unless(match[1].nil? or match[4].nil?) 
					idx[:id] = match[1]				
					idx[:equivalentids] = [match[4]]				
					puts "\nParser Index = #{idx.to_json}\n\n"
                        		outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
				end
				unless(match[2].nil? or match[4].nil?) 
					idx[:id] = match[2]				
					idx[:equivalentids] = [match[4]]				
					puts "\nParser Index = #{idx.to_json}\n\n"
                        		outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
				end
				unless(match[3].nil? or match[4].nil?) 
					idx[:id] = match[3]				
					idx[:equivalentids] = [match[4]]				
					puts "\nParser Index = #{idx.to_json}\n\n"
                        		outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
				end
			end
		end

		# send results to indexer to create sms data graph
                outbound_messages.each_slice(20) do | batch |
puts batch
                        @pool.next.send_messages( batch )
                end




      # puts "cons-sms-indexer:: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@outbound} does not exist yet, will retry in 30 seconds"
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










































