# sms_query.rb

# the query method for the sms data graph, will be wrapped in a sinatra web endpoint for 
# web users

require 'redis'
require 'hashids'
require_relative '../niasconfig'


class SMSQuery

    def initialize
	config = NiasConfig.new
        @redis = config.redis
        @idgen = Hashids.new( 'nsip sms queries' )

    end


    # main query method, takes only 2 params
    # 
    # item - refid/uid of an object in the sms
    # 
    # collection - any of the known collections within the sms
    # 
    # the result is any objects that fulfill the expressed relationship
    # 
    # so an item of an LEAId and the collection SchoolInfos will return 
    # all schools within an LEA, an item of SchoolInfoId and the collection 
    # StudentPersonals will return all students in a school etc. etc.
    # 
    # returns an array of object references, which can be returned to 
    # the caller, or used to extract the relevant messages from the 
    # key value store and return the full payload
    # 
    # 
    def find( q_item, q_collection )

        return [] if q_item.nil?

        puts "\n\nquerying...\n\n"
        result = []

        q_direct_start = Time.now

        terms = []
        terms << q_item
        terms << q_collection

        # puts "direct sinter - #{terms.inspect}"

        result = @redis.sinter terms 

        q_direct_finish = Time.now

        puts "direct query took: #{q_direct_finish - q_direct_start}\n\n"
        puts "result is #{result.count} items\n\n"
        # puts "result is #{result.inspect} and empty is: #{result.empty?}\n\n"

        # if no direct results then try indirect
        # initially, we exclude SchoolInfo, which is tied to everything, and will return too many objects
        if result.empty? then

            tmp = @idgen.encode( rand(1...999) )
            q_indirect_start = Time.now

            q = []
            q = @redis.sdiff q_item, "SchoolInfo"
            # puts "\n\nindirect sinter - #{q.inspect}\n\n"
            unless q.empty? then
                @redis.pipelined do
                    @redis.sunionstore tmp, q.to_a
                    result = @redis.sinter tmp, q_collection
                    @redis.expire tmp, 5 
                end
                result = result.value.count == 0 ? [] : result.value
            end

            # puts q
            # puts result

            if result.empty? then
                puts "Query failed on excluding schools\n\n"
                # include Schools back in
                q = @redis.smembers q_item
                return result if q.empty? # return empty results if item not in db

                tmp = @idgen.encode( rand(1...999) )
                @redis.pipelined do
                    @redis.sunionstore tmp, q.to_a
                    result = @redis.sinter tmp, q_collection
                    @redis.expire tmp, 5 
                end
                result = result.value.count == 0 ? [] : result.value
            end

            q_indirect_finish = Time.now
            #puts "\n\nindirect query took #{q_indirect_finish - q_indirect_start}\n\n"
            #puts "\n\nresult is #{result.value.count} items\n\n"
        end
        return result

    end

    # 
    # convenience menthod to just get members of a collection
    # 
    def collection_only( name )

        result = []
        result = @redis.smembers name

        return result

    end

    # provide the list of collections creates in the sms
    # 
    # returns an array of collection names e.g. StudentPersonal, SchoolInfo etc.
    # 
    def known_collections

        return @redis.smembers('known:collections').to_a

    end

    # resolve a local identifier to a GUID
    def local_id_resolver( id )
        otheridhash = @redis.hgetall "oid:#{id}"
        ret = nil
        if otheridhash.has_key?('localid')
            ret = otheridhash['localid']
        elsif otheridhash.has_key?('oneroster_identifier')
            ret = otheridhash['localid']
        elsif otheridhash.has_key?('oneroster_userId')
            ret = otheridhash['userId']
        elsif otheridhash.has_key?('oneroster_courseCode')
            ret = otheridhash['localid']
        elsif otheridhash.has_key?('oneroster_classCode')
            ret = otheridhash['localid']
        end
        return ret
    end

    # get label for a GUID
    def get_label (id) 
        ret = @redis.hget "labels", id
        ret = id if ret.nil? or ret.empty?
        return ret
    end

end


# test script

# smsq = SMSQuery.new

# puts "\nKnown collections: #{smsq.known_collections}\n\n"

# result = smsq.find( 'a58986d8-069c-4df6-829a-ed8ea09d06db', 'StudentPersonal' )

# puts result.inspect

# puts "\n Query result is #{result}\n\n"





