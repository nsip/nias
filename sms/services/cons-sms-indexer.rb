# cons-sms-indexer.rb

# Simple pull service at the heart of nias
# 
# takes in the parsed tuples for data models (i.e. results of parsing sif xml messages after validation)
# 
# the result is always a tuple structure of the form:

# type: The description of the object/message type e.g. StudentPersonal
# 
# id: The guid or RefId of the object - if none supplied by inbound data will have been created during ingest
# 
# links: [] An array of other object references that this object knows about i.e. a StudentSchoolEnrolment will
# contain its own refid, the refid of a schoolinfo and the refid of a studentpersonal
#
# The SMS builds a bi-directional graph of all references, all nodes are traversable so intermedate objects
# can be invivisble to the user unless specifically needed.
# 
# Links can be followed in either direction meaning aggregate queries can be made for high-level objects such as district/lea
# 
# The SMS also takes in all alternate (human-readable) IDs for the same object, and creates a hash from that ID to the GUID/RefID.
# The hash uses the id type as key and the id as value.
# So if "XYZZY" is the LocalId for the object with RefId "1", and it is also the StateProvinceId for the object with RefId "2",
# then the Redis hash "XYZZY" with have XYZZY[localid] = 1, XYZZY[stateprovinceid] = 2
# 

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'
require 'redis'

@inbound = 'sms.indexer'

@idgen = Hashids.new( 'nsip random temp uid' )

@redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)

@servicename = 'cons-sms-indexer'

# create consumer
@consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
                                           @inbound, 0, :latest_offset)




loop do

  begin
  		messages = []
	    messages = @consumer.fetch
	    
	    messages.each do |m|

      		idx_hash = JSON.parse( m.value )

      		# puts "\n\nIndexer Message : - #{idx_hash.inspect}\n\n"

        	# no responses needed from redis so pipeline for speed
    		  @redis.pipelined do

      				@redis.sadd 'known:collections', idx_hash['type']

      				@redis.sadd idx_hash['type'], idx_hash['id']
      				
      				@redis.sadd idx_hash['id'], idx_hash['links'] unless idx_hash['links'].empty?

      				idx_hash['otherids'].each do |key, value|
      					@redis.hset "oid:#{value}", key, idx_hash['id']
      				end

      				# then add id to sets for links
      				idx_hash['links'].each do | link |
                  			refs = []
                  			refs = idx_hash['links'].reject { |n| n == link } # can ignore self-links
                  			refs << idx_hash['id']

                  			@redis.sadd link, refs unless refs.empty?

      				end

    			end
  		
  		end

      # puts "cons-sms-indexer:: Resuming message consumption from: #{consumer.next_offset}"

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










































