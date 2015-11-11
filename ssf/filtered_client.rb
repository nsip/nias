# filtered_client.rb

# $LOAD_PATH << '.'

require 'sinatra'
require 'sinatra/reloader' if development?
require 'poseidon' # kafka interface
require 'zk' # zookeeper interface
require 'hashids' # temp non-colliding client & producer id generator
require 'nokogiri' # xml support
require 'cgi'
#require 'rack-matrix_params' # matrix params support

#use Rack::MatrixParams

class FilteredClient < Sinatra::Base


# Client to SSF: given request for localhost:1234/topic/stream;contextId=filter,
# display contents of Kafka stream topic.stream.filter

configure do
	# create an interface to the zookeeper node
	set :zk, ZK.new
	set :hashid, Hashids.new( "gregor samza" )
	
	# uncomment line below to allow session to keep temporary client-id, if calling from browser with
	# cookies enabled will mean automatic correct setting of message offset in session
	# enable :sessions

	# All received XML messages are also sent from /:topic:/stream to a global sifxml.ingest topic, for validation by microservice
	set :xmltopic, 'sifxml.ingest'
end


helpers do
		def valid_route?( url )
			return settings.zk.children("/brokers/topics").include?( url )
		end

		def resolve_offset( offset_param = "latest" )
			if offset_param == nil
				offset = :earliest_offset
			elsif offset_param == "latest"
				offset = :latest_offset
			elsif offset_param == "earliest"
				offset = :earliest_offset
			else
				offset = offset_param.to_i
			end
			return offset
		end
end



# if matrix context specified, pick up the filtered content
# read messages from a stream
# 
# pass parameter 'offset' to manage position
# 
# call route with no params to get current offset to begin reading from
# 
# call with offset=earliest to get oldest available message for topic/stream
# call with offset=latest to get most recent available message for topic/stream
# 
get "/filtered/:topic/:stream/:profile" do

	# check validity of route
	tpc = params['topic']
	#context = params['stream']['contextId']
	strm = params['stream'].gsub(/;.*$/,'')
	context = params['stream']
	filter = params['profile']
	topic_name = "#{tpc}.#{strm}.#{filter}"
	if  !valid_route?( topic_name ) then
		params.inspect
		halt 400, params.inspect + "Sorry #{topic_name} is not a supported route." 
	end
	

	# see if user already has a temporary client id, if not generate one
	# useful across calls as allows broker to correlate position in logs
	# for same user session.
	if session['client_id'] == nil 
		session['client_id'] = settings.hashid.encode(Random.new.rand(999))
	end
	client_id = session['client_id']
	puts "\nClient ID  is #{client_id}\n\n"
	


	offset = resolve_offset( params['offset'] )



	# get batch of messages from broker
	messages = []
	begin
		consumer = Poseidon::PartitionConsumer.new(client_id, "localhost", 9092,
                                           topic_name, 0, offset)
		messages = consumer.fetch
	# rescue StandardError => e
	rescue Poseidon::Errors::OffsetOutOfRange 
		# most common cause is records have been deleted by log cleaning since 
		# last visit, so reset to current high water mark and let consumer
		# figure it out
		puts "[warning] - bad offset supplied, resetting..."
		offset = :latest_offset
		retry
	end



	# stream messages to client
	stream do | out |
		begin
	    	messages.each do |msg|
	    		# puts msg.value
	    		record = { 
	    			:data => "<div class='record'>" + 
					CGI.escapeHTML(msg.value).gsub("\n","<br/>").gsub("ZZREDACTED","<span class='redacted'>REDACTED</span>").gsub("1582-10-15","<span class='redacted'>REDACTED</span>").gsub("00000000-0000-0000-0000-000000000000","<span class='redacted'>REDACTED</span>") + 

					'</div>',
	    			:key => msg.key,
	    			:consumer_offset => consumer.offset,
	    			:hwm => consumer.highwater_mark,
	    			:restart_from => consumer.next_offset
	    		}
	    		out << record.to_s
	    	end
	    	footer = {:advice => "Start consuming from #{consumer.next_offset}" }
	    	out << footer.to_s
	  	rescue StandardError => e
	    	out << "Error streaming messages \n\n#{e}"
	  	end
  	end




end

end










