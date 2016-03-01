# hookup_ids_server.rb
require 'sinatra' 
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/content_for'
require 'hashids' # temp non-colliding client & producer id generator
require 'json'
require 'poseidon'
require_relative '../kafkaproducers'


# tiny web service to link one GUID to a sequence of other GUIDs on Redis
class HookupServer < Sinatra::Base

    helpers Sinatra::ContentFor

    # Given the request /hookup?sourceid=x,x,x,x&targetid=y,y,y,y
    # generates biridirectonal links to the sms.indexer topic, of the form
    # 
    # this  [ 'tuple' id - [links] ]
    #
    # which is then passed on to the sms indexing service
    #
    # The ids are presumed to be appended to what is already there
    # 

    outbound = 'sms.indexer'

    @idgen = Hashids.new( 'nsip random temp uid' )
    @servicename = "hookup_ids_server"

    # set up producer pool - busier the broker the better for speed
    producers = KafkaProducers.new(@servicename, 10)
    #pool = producers.get_producers.cycle

    post "/hookup" do
        @sourceid = params['sourceid']
        @targetid = params['targetid']
        halt 400 if @sourceid.nil?
        halt 400 if @targetid.nil?
        outbound_messages = []
                @sourceid.split(',').each do |m|

            # create 'empty' index tuple
            idx = { :type => nil, :id => m, :otherids => {}, :links => [], :equivalentids => []}      	

            idx[:links] = @targetid.split(',')

            #puts "\nParser Index = #{idx.to_json}\n\n"

            outbound_messages << Poseidon::MessageToSend.new( "#{outbound}", idx.to_json, "indexed" )
                    end

        @targetid.split(',').each do |m|

            # create 'empty' index tuple
            idx = { :type => nil, :id => m, :otherids => {}, :links => [], :equivalentids => []}      	

            idx[:links] = @sourceid.split(',')

            #puts "\nParser Index = #{idx.to_json}\n\n"

            outbound_messages << Poseidon::MessageToSend.new( "#{outbound}", idx.to_json, "indexed" )
                    end
        # send results to indexer to create sms data graph
        #outbound_messages.each_slice(20) do | batch |
            #pool.next.send_messages( batch )
            producers.send_through_queue( outbound_messages )
        #end

    end


end





