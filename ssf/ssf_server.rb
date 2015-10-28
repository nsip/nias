# ssf_server.rb

# $LOAD_PATH << '.'

require 'sinatra'
require 'sinatra/reloader' if development?
require 'json'
require 'csv'
require 'poseidon' # kafka interface
require 'zk' # zookeeper interface
require 'hashids' # temp non-colliding client & producer id generator
require 'nokogiri' # xml support



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



get "/" do

	return 'SSF Server is up and running...'
end

get "/privacy" do
	@profile = params['profile']
	if @profile.nil? 
		# List known privacy profiles
		@d = Dir["#{__dir__}/services/privacyfilters/*.xpath"].map{|x| x[/\/([^\/]+).xpath$/, 1]}
		erb :privacyfilters
	else
		# Edit privacy profile named
		@file = File.open("#{__dir__}/services/privacyfilters/#{@profile}.xpath", "r")
		@contents = @file.read
		@file.close
		erb :create
	end
end

# receive edit of privacy profile
post "/privacy" do
	@profile = params['profile']
	if !@profile.nil?
		@logfile = File.open("#{__dir__}/services/privacyfilters/#{@profile}.xpath","w")
    		@logfile.truncate(@logfile.size)
    		@logfile.write(params[:file])
    		@logfile.close
    		redirect '/privacy'
	end
end


# send messages - messages assumed to be in the data file sent
# as part of the request, handles json without needing content type specified
# xml & csv need explicit setting of the Content-Type header 
# 
# 
post "/:topic/:stream" do
	content_type 'application/json'
	
	# check validity of route
	tpc = params['topic']
	strm = params['stream']
	topic_name = "#{tpc}.#{strm}"
	
	# 
	# uncomment this block if you want to prevent dynamic creation
	# of new topics
	# 
	# if  !valid_route?( topic_name ) then
	# 	halt 400, "Sorry #{topic_name} is not a supported route." 
	# end



	if session['producer_id'] == nil 
		session['producer_id'] = settings.hashid.encode(Random.new.rand(999))
	end
	producer_id = session['producer_id']
	puts "\nProducer ID  is #{producer_id}\n\n"




	# extract messages
	puts "\nData sent is #{request.media_type}\n\n"
	# get the payload
	request.body.rewind
	# read messages based on content type
	raw_messages = []
	case request.media_type
	when 'application/json' then raw_messages = JSON.parse( request.body.read )
	when 'application/xml' then 
	#	doc = Nokogiri::XML( request.body.read )
	#	raw_messages = doc.xpath( "//message")
		# we will leave parsing the XML to the microservice cons-prod-ingest.rb
		raw_messages << request.body.read 
	when 'text/csv' then raw_messages = CSV.parse( request.body.read , {:headers=>true})
	else
		halt 415, "Sorry content type #{request.media_type} is not supported, must be one of: application/json - application/xml - text/csv"
	end
	
	messages = []
	raw_messages.each do | msg |
		case request.media_type
		when 'application/json' then msg = msg.to_json
		#when 'application/xml' then msg = msg.to_s
		when 'application/xml' then 
			msg = msg.to_s
			puts "topic is: #{settings.xmltopic} : topic name is #{topic_name}\n\n"
			messages << Poseidon::MessageToSend.new( "#{settings.xmltopic}", msg, "#{topic_name}" )
		when 'text/csv' then msg = msg.to_hash.to_json
		end
		messages << Poseidon::MessageToSend.new( "#{topic_name}", msg, "#{strm}" )
		# write to default for audit if required
		# messages << Poseidon::MessageToSend.new( "#{tpc}.#{:default}", msg, "#{strm}" )
	end



	# set up producer pool - busier the broker the better for speed
	producers = []
	(1..10).each do | i |
		p = Poseidon::Producer.new(["localhost:9092"], producer_id, {:partitioner => Proc.new { |key, partition_count| 0 } })
		producers << p
	end
	pool = producers.cycle




	# send the messages
	sending = Time.now
	puts "sending messages ( #{messages.count} )...."
	puts "started at: #{sending.to_s}"


	messages.each_slice(20) do | batch |
			pool.next.send_messages( batch )
	end


	finish = Time.now
	puts "\nFinished: #{finish.to_s}\n\n"
	puts "\ntime taken to send: #{(finish - sending).to_s} seconds\n\n"
	
	return 202
end


# read messages from a stream
# 
# pass parameter 'offset' to manage position
# 
# call route with no params to get current offset to begin reading from
# 
# call with offset=earliest to get oldest available message for topic/stream
# call with offset=latest to get most recent available message for topic/stream
# 
get "/:topic/:stream" do

	# check validity of route
	tpc = params['topic']
	str = params['stream']
	topic_name = "#{tpc}.#{str}"
	if  !valid_route?( topic_name ) then
		halt 400, "Sorry #{topic_name} is not a supported route." 
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
	    			:data => msg.value,
	    			:key => msg.key,
	    			:consumer_offset => consumer.offset,
	    			:hwm => consumer.highwater_mark,
	    			:restart_from => consumer.next_offset
	    		}
	    		out << record.to_json
	    		out << "\n\n" #for readability when demonstrating only
	    	end
	    	footer = {:advice => "Start consuming from #{consumer.next_offset}" }
	    	out << footer.to_json
	  	rescue StandardError => e
	    	out << "Error streaming messages \n\n#{e}"
	  	end
  	end




end












