# equiv_ids_server.rb
require 'sinatra' 
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/content_for'
require 'hashids' # temp non-colliding client & producer id generator
require 'json'
require 'poseidon'

# tiny web service to assert the equivalence of a set of GUIDs on Redis
class EquivalenceServer < Sinatra::Base

	helpers Sinatra::ContentFor

# Given the request ?ids=y1,y2...
# generates equivalences  to the sms.indexer topic, of the form
# 
# this  [ 'tuple' id:y1 - type:nil - equivalent-ids:[y2...], links:nil, other-ids:nil ]
#
# which is then passed on to the sms indexing service
#
# the ids are presumed to be appended to what is already there
#
# Equivalence is bidirectional, so the order of y1 and y2 does not matter
# 

configure do
                set :outbound, 'sms.indexer'

@idgen = Hashids.new( 'nsip random temp uid' )
end

producer_id = @idgen.encode(Random.new.rand(999))
# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], "equiv_ids_server", {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
pool = producers.cycle


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
  		

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
			pool.next.send_messages( batch )
	   	end

	end


  
end





