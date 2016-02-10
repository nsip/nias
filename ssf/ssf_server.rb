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
require_relative '../sms/services/cvsheaders-naplan'

=begin
Class to handle ingesting of messages into NIAS. Deals with ingest and bulk ingest of topic/stream, and requests for topic/stream and particular privacy profiles of topic/stream. Parses JSON and CSV messages into JSON objects.
=end

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
    end

    @validation_error = false

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

        # workout whether to set offset to earliest or latest
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

	def validate_fileupload(mimetype, topic_menu, topic, stream, payload)
		return "MIME type #{mimetype} not recognised" if mimetype != 'application/xml' and mimetype != 'application/json' and mimetype != 'text/csv'
		return "No topic provided" if topic_menu.nil? and (topic.nil? or stream.nil?)
		return "No file provided" if payload.nil?
		return "Topic #{topic} malformed" if !topic.nil? and topic.match(%r!/!)
		return "Stream #{stream} malformed" if !stream.nil? and stream.match(%r!/!)
		return "OK"
	end

	@validation_error = false
        # fetch messages from the body of the HTTP request, and parse the messages if they are in CSV or JSON
        def fetch_raw_messages(topic_name, mimetype, body)
            # set producer ID for the session
            if session['producer_id'] == nil 
                session['producer_id'] = settings.hashid.encode(Random.new.rand(999))
            end
	    	@validation_error = false
            puts "\nProducer ID  is #{session['producer_id']}\n\n"
                        # extract messages
            puts "\nData sent is #{mimetype}\n\n"
            # get the payload
            #request.body.rewind
            # read messages based on content type
            raw_messages = []
            case mimetype
            when 'application/json' then raw_messages = JSON.parse( body ) # request.body.read )
            when 'application/xml' then 
                # we will leave parsing the XML to the microservice cons-prod-sif-ingest.rb
                                raw_messages << body # request.body.read 
            when 'text/csv' then 
		# There are reportedly performance issues for CSV validation above 700KB. But 20 MB validates without complaint
		csv = body # request.body.read
=begin
		cvs_schema = nil
		case topic_name
                when 'naplan.csv_staff'
			csv_schema = CSVHeaders.get_naplan_staff_csv_csvw
		when 'naplan.csv'
			csv_schema = CSVHeaders.get_naplan_student_csv_csvw
		else
			csv_schema = nil
		end
		csv_schema = Csvlint::Schema.from_csvw_metadata("http://example.com", JSON.parse(csv_schema)) unless csv_schema.nil?
=end

		validator = Csvlint::Validator.new( StringIO.new( csv ) , {}, nil)
		validator.validate
		if(validator.valid? and validator.errors.empty?) then
			raw_messages = CSV.parse( csv , {:headers=>true})
			csvlines = csv.lines()
			raw_messages.each_with_index do |e, i| 
				e[:__linenumber] = i+1 
				e[:__linecontent] = csvlines[i+1].chomp
			end
		else
			@validation_error = true
			raw_messages = validator.errors.map {|e| "Row: #{e.row} Col: #{e.column}, Category #{e.category}: Type #{e.type}, Content #{e.content}, Constraints: #{e.constraints}" }
			raw_messages.each {|e| puts e}
		end
            else
                halt 415, "Sorry content type #{mimetype} is not supported, must be one of: application/json - application/xml - text/csv"
            end
	    return raw_messages
        end

        # post messages to the appropriate stream. bulk identifies whether these are intended 
	# for the simple endpoint (limit 1 MB) or the bulk endpoint
        def post_messages( messages , compression_codec, bulk )
            # set up producer pool - busier the broker the better for speed
            # but *no* multiple producers if doing bulk ingest: splitting the message among producers risks its being reassembled out of sequence
            producers = []
            producercount = bulk ? 1 : 10
            (1..producercount).each do | i |
                p = Poseidon::Producer.new(["localhost:9092"], session['producer_id'], {:compression_codec => compression_codec , :partitioner => Proc.new { |key, partition_count| 0 } })
                producers << p
            end
            pool = producers.cycle
                        # send the messages
            sending = Time.now
            puts "sending messages ( #{messages.count} )...."
            puts "started at: #{sending.to_s}"

            messages.each_slice(20) do | batch |
                pool.next.send_messages( batch )
                #p.send_messages( batch )
            end
                        finish = Time.now
            puts "\nFinished: #{finish.to_s}\n\n"
            puts "\ntime taken to send: #{(finish - sending).to_s} seconds\n\n"
        end

        # https://www.ruby-forum.com/topic/1057851
        # Split up str into an array of strings with string length = value
        def to_2d_array(str, value)
            str.unpack("a#{value}"*((str.size/value)+((str.size%value>0)?1:0)))
        end

    end



    get "/ssf" do

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

        fetched_messages.each do | msg |
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

        fetch_raw_messages(topic_name, request.media_type, request.body.read).each do | msg |

            topic = "#{topic_name}"
            key = "#{strm}"

            case request.media_type
            when 'application/json' then msg = msg.to_json
            when 'application/xml' then 
                msg =  "TOPIC: #{topic_name}\n" + msg.to_s
                topic = "#{settings.xmlbulktopic}"
                key = "#{topic_name}"
            when 'text/csv' then msg = msg.to_hash.to_json
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
#puts body

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
        fetch_raw_messages(topic_name, mimetype, body).each do | msg |

            topic = "#{topic_name}"
            key = "#{strm}"

            case mimetype
            when 'application/json' then msg = msg.to_json
            when 'application/xml' then 
                msg =  "TOPIC: #{topic_name}\n" + msg.to_s
                topic = "#{settings.xmlbulktopic}"
                key = "#{topic_name}"
            when 'text/csv' then msg = msg.to_hash.to_json
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

        post_messages(messages, :none, true)		
        return 202, "File read successfully"
	

	# tried to pass control to POST topic_menu; have to invoke the methods separately here



    end












end # end of sinatra app class











