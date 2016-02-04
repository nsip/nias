# cons-sms-indexer.rb

# Simple pull service at the heart of nias
# 
# takes in the parsed tuples for data models (i.e. results of parsing sif xml messages after validation)
# 
# the result is always a tuple structure of the form:
#
# ( id, type, [equivalent-ids], {other-ids}, [links] , label) 
#
# id: The guid or RefId of the object - if none supplied by inbound data will have been created during ingest
#
# type: The description of the object/message type e.g. StudentPersonal. 
# if type is empty, then the tuple is assumed to be an update rather than a create
#
# equivalent-ids : refids that are asserted to be equivalent to the current refid.
# If an equivalent-id has been or is being asserted to be equivalent to the current refid,
# then all links to and from the current refid are duplicated onto the equivalent-id. So the graph is duplicated for all
# equivalent nodes.
#
# other-ids: all alternate (human-readable) IDs for the same object, as a hash from that ID to the GUID/RefID.
# The hash uses the id type as key and the alternate id as value.
# So if "XYZZY" is the LocalId for the object with RefId "1", and it is also the StateProvinceId for the object with RefId "2",
# then the Redis hash "XYZZY" with have XYZZY[localid] = 1, XYZZY[stateprovinceid] = 2 
# The mapping of other-id -> type is meant to have a unique value (ref id). For that reason, if two different schemas are used,
# containing the same other-id, these need to be differentiated in name: e.g. localId in SIF vs identifier in OneRoster
#
# label: a human-readable label for the object. Used e.g. in graphs
# 
# links: [] An array of other object references that this object knows about i.e. a StudentSchoolEnrolment will
# contain its own refid, the refid of a schoolinfo and the refid of a studentpersonal
#
# The SMS builds a bi-directional graph of all references, all nodes are traversable so intermedate objects
# can be invivisble to the user unless specifically needed.
# 
# Links can be followed in either direction meaning aggregate queries can be made for high-level objects such as district/lea
# 
# The following redis data structures result:
#
# * known:collections: set, contains all types
# * other:ids: set, contains all other-ids
# * (type): set, contains all ids of that type
# * (id): set, contains all links to and from that id
# * (other_id): hash: key is identifier type, value is id
# * equivalent:ids:(equivalent-id): set, contains all equivalent ids
# * labels: hash: key is GUID, value is human-readable label

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

      		#puts "\n\nIndexer Message : - #{idx_hash.inspect}\n\n"

		# get the nodes equivalent to the current node
		prev_equivalents = @redis.smembers "equivalent:ids:#{idx_hash['id']}"
		equivalents = prev_equivalents + idx_hash['equivalentids']
		equivalents = equivalents.uniq

		# are there any new equivalences because of this tuple? If so, duplicate the existing links among all equivalences
		new_equivalents = idx_hash['equivalentids'].reject{|x| prev_equivalents.include? x}
		unless (new_equivalents.empty?)
			new_equivalents << idx_hash['id']
			new_equivalents.each do |x|
				new_equivalents.each do |y|
					next if x == y
					@redis.sunionstore x, x, y
				end
			end
		end
		

        	# no responses needed from redis so pipeline for speed
    		  @redis.pipelined do

				@redis.hset 'labels', idx_hash['id'], idx_hash['label'] unless (idx_hash['label'].nil?)

      				@redis.sadd 'known:collections', idx_hash['type'] unless (idx_hash['type'].nil? or idx_hash['type'].empty?)

      				@redis.sadd idx_hash['type'], idx_hash['id'] unless (idx_hash['type'].nil? or idx_hash['type'].empty?)
      				
      				@redis.sadd idx_hash['id'], idx_hash['links'] unless idx_hash['links'].empty?

				equivalents.each do |id|
      					@redis.sadd id, idx_hash['links'] unless idx_hash['links'].empty?
				end

				idx_hash['otherids'].each do |key, value|
					@redis.hset "oid:#{value}", key, idx_hash['id']
					@redis.sadd 'other:ids', "oid:#{value}"
#puts "#####" + "oid:#{value}" + " " + key + " " + idx_hash['id']
				end

				# extract equivalent ids
				idx_hash['equivalentids'].each do |equiv|
                  			refs = []
                  			refs = idx_hash['equivalentids'].reject { |n| n == equiv } # can ignore self-links
                  			refs << idx_hash['id']
#puts %Q(redis.sadd "equivalent:ids:#{equiv}", #{refs} )
					@redis.sadd "equivalent:ids:#{equiv}", refs unless refs.empty?
#puts "guid2 set equivalent:ids:#{equiv} : " + (@redis.smembers "equivalent:ids:#{guid2}").join(" ")

				end

      				# then add id to sets for links
      				idx_hash['links'].each do | link |
                  			refs = []
                  			refs = idx_hash['links'].reject { |n| n == link } # can ignore self-links
                  			refs << idx_hash['id']
                  			refs = refs + equivalents

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


