# equiv_ids_server.rb
require 'sinatra' 
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/content_for'
require 'hashids' # temp non-colliding client & producer id generator
require 'json'
require 'poseidon'
require_relative '../kafkaproducers'


# tiny web service to assert the equivalence of a set of GUIDs on Redis
class EquivalenceServer < Sinatra::Base

    helpers Sinatra::ContentFor

    # Given the request /equiv?ids=y1,y2...
    # generates equivalences  to the sms.indexer topic, of the form
    # 
    # { id:y1, type:nil, equivalent-ids:[y2...], links:nil, other-ids:nil }
    #
    # which is then passed on to the SMS indexing service
    #
    # The ids are presumed to be appended to what is already in Redis
    #
    # Equivalence is bidirectional, so the order of y1 and y2 does not matter.
    # 

    configure do
        set :outbound, 'sms.indexer'

        @idgen = Hashids.new( 'nsip random temp uid' )
    end
    @servicename = "equiv_ids_server"

    producer_id = @idgen.encode(Random.new.rand(999))
    # set up producer pool - busier the broker the better for speed
    producers = KafkaProducers.new(@servicename, 10)
    #pool = producers.get_producers.cycle

    post "/equiv" do
        halt 400 if params['ids'].nil?
        @ids = params['ids'].split(',')
        halt 400 if @ids.length < 2
                outbound_messages = []
        
        # create 'empty' index tuple
        idx = { :type => nil, :id => @ids[0], :otherids => {}, :links => [], :equivalentids => []}      	

        idx[:equivalentids] = @ids[1..-1]

        #puts "\nParser Index = #{idx.to_json}\n\n"

        outbound_messages << Poseidon::MessageToSend.new( "#{settings.outbound}", idx.to_json, "indexed" )
        
        # send results to indexer to create SMS data graph
        #outbound_messages.each_slice(20) do | batch |
            #pool.next.send_messages( batch )
            producers.send_through_queue( outbound_messages )
        #end

    end


end





