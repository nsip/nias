# ssf_server.rb


require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/content_for'
require 'sinatra/base'
require 'json'
require 'csv'
require 'poseidon' # kafka interface
require 'zk' # zookeeper interface
require 'hashids' # temp non-colliding client & producer id generator
require 'nokogiri' # xml support
require 'csvlint' # csv support
require 'json-schema'
require 'thin'
#require 'em-websocket' # for server-side push of errors
#require 'sinatra-websocket' # for server-side push of errors
#require 'kafka-consumer'
require 'poseidon_cluster'
require 'redis'

require_relative '../kafkaproducers'
require_relative '../kafkaconsumers'
require_relative './ssf_server_helpers'
require_relative '../niasconfig'
require_relative '../niaserror'

=begin
Class to handle ingesting of messages into NIAS. Deals with ingest and bulk ingest of topic/stream, and requests for topic/stream and particular privacy profiles of topic/stream. Parses JSON and CSV messages into JSON objects.
=end

#EventMachine.run do
class SSFServer < Sinatra::Base

    helpers Sinatra::ContentFor

    configure do
        # create an interface to the zookeeper node
        set :zk, ZK.new
        set :hashid, Hashids.new( "gregor samza" )
                # uncomment line below to allow session to keep temporary client-id, if calling from browser with
        # cookies enabled will mean automatic correct setting of message offset in session
        # enable :sessions

        # All received XML messages to normal endpoint are also sent from /:topic:/stream to a global sifxml.ingest topic, for validation by microservice
        # The source topic/stream is injected into the header line TOPIC: topic/stream before the XML payload
        set :xmltopic, 'sifxml.ingest'
        # All received XML messages to bulk ingest endpoint are sent to
        # global sifxml.bulkingest topic, broken down into 1 MB messages. They
        # are reasssembled into the original payload and then broken down into 
        # individual objects
        set :xmlbulktopic, 'sifxml.bulkingest'
	# CSV errors
	set :csverrors, 'csv.errors'
	# storage, CSV/JSON messages. XML messages are sent to storage from sifxml.validated
	set :jsonstorage, 'json.storage'

	# sinatra-websocket settings
	set :server, 'thin'
	#set :sockets, []

    end

    @validation_error = false
    @servicename = 'ssf_server'
    #@@errorconsumer = '???'
    # Kafka queue, which will be pushed out through websocket

    helpers do
        # is this a valid route for a Kafka topic
        def valid_route?( url )
            return settings.zk.children("/brokers/topics").include?( url )
        end

        # return all available Kafka topics
        def get_topics_list
            topics = []
            topics = settings.zk.children("/brokers/topics")

            # transcribe dotted queue names to url style presentation for web users 
            url_topics = topics.map { |t_name| t_name.gsub('.', '/')  }
            return url_topics.sort!
        end

    end



    get "/ssf" do
    # Kafka queue, which will be pushed out through websocket
        @topics = get_topics_list
        erb :ssf
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

        if request.content_length.to_i > 1000000 then
            halt 400, "SSF does not accept messages over 1 MB in size. Try the bulk uploader: #{tpc}/#{strm}/bulk (limit: 500 MB)" 
        end

        # uncomment this block if you want to prevent dynamic creation
        # of new topics
        # 
        # if  !valid_route?( topic_name ) then
        # 	halt 400, "Sorry #{topic_name} is not a supported route." 
        # end

        messages = []
	fetched_messages = fetch_raw_messages(topic_name, request.media_type, request.body.read )
	@validation_error = fetched_messages["validation_error"]
	
        case request.media_type
	    when 'application/xml' then
                topic = "#{settings.xmltopic}"
                key = "#{topic_name}"
            when 'text/csv' then 
		if(@validation_error) then
                	topic = "#{settings.csverrors}" 
		else
        		topic = "#{topic_name}"
		end
        	key = "#{strm}"
	    else		
        	topic = "#{topic_name}"
        	key = "#{strm}"
	end

        fetched_messages["messages"].each do | msg |
            case request.media_type
            when 'application/json' then msg = msg.to_json
            when 'application/xml' then 
                msg =  "TOPIC: #{topic_name}\n" + msg.to_s
            when 'text/csv' then 
		if(@validation_error) then
			msg = msg.to_s
		else
			msg = msg.to_hash.to_json
		end
            end

            #puts "\n\ntopic is: #{topic} : key is #{key}\n\n#{msg}\n\n"
            messages << Poseidon::MessageToSend.new( "#{topic}", msg, "#{key}" )

            unless request.media_type == 'application/xml' or @validation_error
                messages << Poseidon::MessageToSend.new( "#{settings.jsonstorage}", "TOPIC: #{topic_name}\n" + msg, "#{key}" )
            end

            # write to default for audit if required
            # messages << Poseidon::MessageToSend.new( "#{topic}.default", msg, "#{strm}" )
         end
	 if(request.media_type == 'text/csv' and !@validation_error) 
		messages << Poseidon::MessageToSend.new( "#{settings.csverrors}", NiasError.new(0, 0, 0, "CSV Well-Formedness Validator", nil).to_s, "#{key}" )
	 end

        post_messages(messages, :none, false)		
        return 202
    end

    # bulk uploader: relaxes message limit from 1 MB to 500 MB, but splits up files into 1 MB segments, for reassembly 
    # 
    # 
    post "/:topic/:stream/bulk" do
        content_type 'application/json'

        # check validity of route
        tpc = params['topic']
        strm = params['stream']
        topic_name = "#{tpc}.#{strm}"

        if request.content_length.to_i > 2000000000 then
            halt 400, "SSF does not accept messages over 500 MB in size." 
        end

        # uncomment this block if you want to prevent dynamic creation
        # of new topics
        # 
        # if  !valid_route?( topic_name ) then
        # 	halt 400, "Sorry #{topic_name} is not a supported route." 
        # end

        messages = []

        fetched_messages = fetch_raw_messages(topic_name, request.media_type, request.body.read)
	@validation_error = fetched_messages["validation_error"]
	fetched_messages["messages"].each do | msg |

            topic = "#{topic_name}"
            key = "#{strm}"
		if(@validation_error) then
                	topic = "#{settings.csverrors}" 
		end

            case request.media_type
            when 'application/json' then msg = msg.to_json
            when 'application/xml' then 
                msg =  "TOPIC: #{topic_name}\n" + msg.to_s
                topic = "#{settings.xmlbulktopic}"
                key = "#{topic_name}"
            when 'text/csv' then 
		if(@validation_error) then
			msg = msg.to_s
		else
			msg = msg.to_hash.to_json
		end
            end

            #puts "\n\ntopic is: #{topic} : key is #{key}\n\n#{msg}\n\n"

            # Kafka has default message size of 1 MB. We chop message up into 950 KB chunks, with all but last terminating in "\n===snip n===", where n is the ordinal number of the chunk
	    # Won't be needed for CSV, each message is one line of CSV > JSON
            msgsplit = to_2d_array(msg, 972800)
            msgtail = msgsplit.pop
            msgsplit.map!.with_index  { |x, idx| x + "\n===snip #{idx}===\n" }
                        msgsplit.each do |msg1|
                messages << Poseidon::MessageToSend.new( "#{topic}", msg1, "#{key}" )
            end
            messages << Poseidon::MessageToSend.new( "#{topic}", msgtail , "#{key}" )

            # write to default for audit if required
            # messages << Poseidon::MessageToSend.new( "#{topic}.default", msg, "#{strm}" )			
        end
	    if(request.media_type == 'text/csv' and !@validation_error) 
		messages << Poseidon::MessageToSend.new( "#{settings.csverrors}", NiasError.new(0, 0, 0, "CSV Well-Formedness Validator", nil).to_s, "valid" )
	    end

        post_messages(messages, :none, true)		
        return 202
    end

    # 
    # add privacy profile extension to q name if supported and pass on
    # 
    get "/:topic/:stream/:profile" do

        topic = params['topic']
        stream = "#{params['stream']}.#{params['profile']}"
        path = "/#{topic}/#{stream}"
        status, headers, body = call env.merge("PATH_INFO" => "#{path}")
        [status, headers, body]		

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
	consumer = KafkaConsumers.new(client_id, [topic_name], offset)
	Signal.trap("INT") { consumer.interrupt }


        # stream messages to client
        stream do | out |
            begin
                consumer.each do |msg|
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

    get "/csverrors" do
	content_type 'text/event-stream'
        if session['client_id'] == nil 
            session['client_id'] = settings.hashid.encode(Random.new.rand(999))
        end
        client_id = session['client_id']
        puts "\nClient ID  is #{client_id}\n\n"

        # get batch of messages from broker
        messages = []
	Signal.trap("INT") { 
		puts "Consumer stopping on INT"
		@csverror_consumer.stop if @csverror_consumer 
	}
=begin 
	Signal.trap("EXIT") { 
		puts "Consumer stopping on EXIT"
		@csverror_consumer.stop if @csverror_consumer 
	}
=end
	Signal.trap("HUP") { 
		puts "Consumer stopping on HUP"
		@csverror_consumer.stop if @csverror_consumer 
	}
	Signal.trap("QUIT") { 
		puts "Consumer stopping on QUIT"
		@csverror_consumer.stop if @csverror_consumer 
	}
	@csverror_consumer = KafkaConsumers.new(client_id, ["csv.errors", "naplan.srm_errors", "sifxml.errors", "naplan.filereport"], :latest_offset) unless @csverror_consumer
        stream do | out |
            begin
                @csverror_consumer.each do |msg|
			out << "data: #{msg.value.gsub(/\n/, "\ndata: ")}\n\n"
		end
	    rescue StandardError => e
		out << "data: Error streaming messages\ndata: #{e}\n\n"
	    end
        end
    end

    # read messages from a stream
    post "/fileupload" do
puts "??"
puts params.inspect
puts "??"
	mimetype = params[:mimetype]
	topic_menu = params['topic_menu'] || nil
	topic = params[:topic]
	stream = params[:stream]
	payload = params[:file]
	payloadname = payload.nil? ? nil : payload[:filename] || nil
	flush = params[:flush] || nil

        if(flush)
                begin
                puts "Flushing REDIS!"
                config = NiasConfig.new
                @redis = config.redis
                @redis.flushdb
                rescue
                        return 500, 'Error executing SMS Flush.'
                end
        end

	validation = validate_fileupload(mimetype, topic_menu, topic, stream, payloadname)
	unless validation == "OK"
            	params.inspect
		halt 400, params.inspect + validation 
	end
	tempfile = params[:file][:tempfile]
	filename = params[:file][:filename]
	body = tempfile.read
	body.gsub!(%r!^.*\nContent-Type:[^\n]+\n\n!, '')
	body.gsub!(%r!^.*?\n\n------WebKitFormBoundary.*$!, '\n')

	if(topic.nil? or stream.nil?)
		topic_out = topic_menu
	else
		topic_out = "#{topic}/#{stream}"
	end
	strm = topic_out.sub(%r!^.*/!, '')
	topic = topic_out.sub(%r!/.*$!, '')
	topic_name = topic_out.gsub(%r!/!, '.')
	
        if request.content_length.to_i > 500000000 then
            halt 400, "SSF does not accept messages over 500 MB in size." 
        end

        # uncomment this block if you want to prevent dynamic creation
        # of new topics
        # 
        # if  !valid_route?( topic_name ) then
        # 	halt 400, "Sorry #{topic_name} is not a supported route." 
        # end

        messages = []
	fetched_messages = fetch_raw_messages(topic_name, mimetype, body )
	@validation_error = fetched_messages["validation_error"]
        fetched_messages["messages"].each do | msg |

            topic = "#{topic_name}"
            key = "#{strm}"

            case mimetype
            when 'application/json' then msg = msg.to_json
            when 'application/xml' then 
                msg =  "TOPIC: #{topic_name}\n" + msg.to_s
                topic = "#{settings.xmlbulktopic}"
                key = "#{topic_name}"
            when 'text/csv' then 
                if(@validation_error) then
                        msg = msg.to_s
                        topic = "#{settings.csverrors}"
                else
			msg = msg.to_hash.to_json
		end
            end

            #puts "\n\ntopic is: #{topic} : key is #{key}\n\n#{msg}\n\n"

            # Kafka has default message size of 1 MB. We chop message up into 950 KB chunks, with all but last terminating in "\n===snip n===", where n is the ordinal number of the chunk
	    # Won't be needed for CSV, each message is one line of CSV > JSON
            msgsplit = to_2d_array(msg, 972800)
            msgtail = msgsplit.pop
            msgsplit.map!.with_index  { |x, idx| x + "\n===snip #{idx}===\n" }
                        msgsplit.each do |msg1|
                messages << Poseidon::MessageToSend.new( "#{topic}", msg1, "#{key}" )
            end
            messages << Poseidon::MessageToSend.new( "#{topic}", msgtail , "#{key}" )

            # write to default for audit if required
            # messages << Poseidon::MessageToSend.new( "#{topic}.default", msg, "#{strm}" )			
        end
	    if(!@validation_error) 
		messages << Poseidon::MessageToSend.new( "#{settings.csverrors}", NiasError.new(0, 0, 0, "CSV Well-Formedness Validator", nil).to_s, "#{topic_name}" )
	    end

        post_messages(messages, :none, true)		
        return 202, "File read successfully"
   end	

	# tried to pass control to POST topic_menu; have to invoke the methods separately here


end # end of sinatra app class











