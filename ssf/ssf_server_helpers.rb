# ssf_server_helpers.rb


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
require 'em-websocket' # for server-side push of errors
require 'sinatra-websocket' # for server-side push of errors

#require_relative '../naplan/services/cvsheaders-naplan'
require_relative '../kafkaproducers'
require_relative '../kafkaconsumers'
require_relative '../niaserror'


=begin
Helper methods for ssf_server.rb
=end

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
        # fetch messages from the body of the HTTP request, and parse the messages if they are in CSV or JSON.
	# Also inject a doc-id for the CSV records, to identify them as all belonging to the same payload.
        def fetch_raw_messages(topic_name, mimetype, body)
            # set producer ID for the session
	    sessionid = Hashids.new( "gregor samza" ).encode(Random.new.rand(100000))
            if session['producer_id'] == nil 
                session['producer_id'] = sessionid
            end
	    @validation_error = false
            puts "\nProducer ID  is #{session['producer_id']}\n\n"
                        # extract messages
            puts "\nData sent is #{mimetype}\n\n"
            # get the payload
            #request.body.rewind
            # read messages based on content type
            raw_messages = []
puts mimetype
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
				e[:__docid] = sessionid
				e[:__linetotal] = raw_messages.length
			end
		else
			@validation_error = true
			raw_messages = []
			validator.errors.each_with_index do |e, i|
				content = e.content
				content.chomp! unless content.nil?
				raw_messages << NiasError.new(i, validator.errors.length, 0, "CSV Well-Formedness Validator", 
					"Row: #{e.row} Col: #{e.column}, Category: #{e.category}: Type: #{e.type}, Content: #{content}, Constraints: #{e.constraints}" ).to_s
			end
			raw_messages.each {|e| puts e}
		end
            else
                halt 415, "Sorry content type #{mimetype} is not supported, must be one of: application/json - application/xml - text/csv"
            end
	    return {"validation_error" => @validation_error, "messages" => raw_messages}
        end

        # post messages to the appropriate stream. bulk identifies whether these are intended 
	# for the simple endpoint (limit 1 MB) or the bulk endpoint
        def post_messages( messages , compression_codec, bulk )
            # set up producer pool - busier the broker the better for speed
            # but *no* multiple producers if doing bulk ingest: splitting the message among producers risks its being reassembled out of sequence
            producers = []
            producercount = bulk ? 1 : 10
	    producers = KafkaProducers.new(@servicename, producercount)

                        # send the messages
            sending = Time.now
            puts "sending messages ( #{messages.count} )...."
            puts "started at: #{sending.to_s}"

		producers.send_through_queue(messages)
                        finish = Time.now
            puts "\nFinished: #{finish.to_s}\n\n"
            puts "\ntime taken to send: #{(finish - sending).to_s} seconds\n\n"
        end

        # https://www.ruby-forum.com/topic/1057851
        # Split up str into an array of strings with string length = value
        def to_2d_array(str, value)
            str.unpack("a#{value}"*((str.size/value)+((str.size%value>0)?1:0)))
        end




