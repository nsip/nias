# oneroster_sif_merge.rb


# Class which reviews the local IDs in SIF records and OneRoster records in Redis that constitute a match,
# and then constructs tuples requesting that the corresponding GUIDs be  merged as equivalent
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
require_relative '../kafkaproducers'
require_relative '../niasconfig'


class OneRosterSifMerge


    def initialize
	config = NiasConfig.new
        @outbound = 'sms.indexer'
        @idgen = Hashids.new( 'nsip random temp uid' )
        @redis = config.redis
        @servicename = 'prod-oneroster-sif-merge-ids'

        # set up producer pool - busier the broker the better for speed
        producers = KafkaProducers.new(@servicename, 10)
        #@pool = producers.get_producers.cycle
    end

    def merge_ids

        begin

            outbound_messages = []

            otherids = @redis.smembers 'other:ids'

            otherids.each do | x |
                # create 'empty' index tuple
                idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => [], :equivalentids => [], :label => nil}    

                # if they have a SIF local id value and a OneRoster id value that are the same, then we have identified
                # a match between their corresponding guids

                match = @redis.hmget x, 'oneroster_identifier', 'oneroster_userId', 'oneroster_courseCode', 'oneroster_classCode', 'localid'

                # we attempt a match on the SIF local Id with each of the oneroster_identifier, oneroster_userId, oneroster_courseCode, and oneroster_classCode
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


            # send results to indexer to create sms data graph
            #outbound_messages.each_slice(20) do | batch |
                #@pool.next.send_messages( batch )
                producers.send_through_queue( outbound_messages )
            #end

        rescue Poseidon::Errors::UnknownTopicOrPartition
            puts "Topic #{@outbound} does not exist yet."
        end
    end

end


# test script - assumes OR and SIF data in db

# orsm = OneRosterSifMerge.new

# orsm.merge_ids







































