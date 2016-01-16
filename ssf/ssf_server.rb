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

	end


	helpers do
			def valid_route?( url )
				return settings.zk.children("/brokers/topics").include?( url )
			end

			def get_topics_list
				topics = []
				topics = settings.zk.children("/brokers/topics")

				# transcribe dotted queue names to url style presentation for web users 
				url_topics = topics.map { |t_name| t_name.gsub('.', '/')  }
				return url_topics.sort!
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
			
			def fetch_raw_messages()
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
					# we will leave parsing the XML to the microservice cons-prod-sif-ingest.rb
					
					raw_messages << request.body.read 
				when 'text/csv' then raw_messages = CSV.parse( request.body.read , {:headers=>true})
				else
					halt 415, "Sorry content type #{request.media_type} is not supported, must be one of: application/json - application/xml - text/csv"
				end
			end
			
			def post_messages( messages , compression_codec )
				# set up producer pool - busier the broker the better for speed
				producers = []
				(1..10).each do | i |
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
				end
		
				finish = Time.now
				puts "\nFinished: #{finish.to_s}\n\n"
				puts "\ntime taken to send: #{(finish - sending).to_s} seconds\n\n"
			end

  			# https://www.ruby-forum.com/topic/1057851
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
		 	halt 400, "SSF does not accept messages over 1 MB in size. Try the bulk uploader: #{tpc}/#{strm}/bulk (limit: 50 MB)" 
		end

		# uncomment this block if you want to prevent dynamic creation
		# of new topics
		# 
		# if  !valid_route?( topic_name ) then
		# 	halt 400, "Sorry #{topic_name} is not a supported route." 
		# end

		messages = []
		fetch_raw_messages().each do | msg |

			topic = "#{topic_name}"
			key = "#{strm}"

			case request.media_type
			when 'application/json' then msg = msg.to_json
			when 'application/xml' then 
				msg =  "TOPIC: #{topic_name}\n" + msg.to_s
				topic = "#{settings.xmltopic}"
				key = "#{topic_name}"
			when 'text/csv' then msg = msg.to_hash.to_json
			end

			#puts "\n\ntopic is: #{topic} : key is #{key}\n\n#{msg}\n\n"
			messages << Poseidon::MessageToSend.new( "#{topic}", msg, "#{key}" )
			
			# write to default for audit if required
			# messages << Poseidon::MessageToSend.new( "#{topic}.default", msg, "#{strm}" )
			
		end

		post_messages(messages, :none)		
		return 202
	end
	
	# bulk uploader: relaxes message limit from 1 MB to 50 MB, but splits up files into 1 MB segments, for reassembly 
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

		fetch_raw_messages().each do | msg |

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

			# Kafka has default message size of 1 MB. We chop message up into 950 KB chunks, with all but last terminating in "\n===snip==="
			msgsplit = to_2d_array(msg, 972800)
			msgtail = msgsplit.pop
			
			msgsplit.each do |msg1|
				messages << Poseidon::MessageToSend.new( "#{topic}", msg1 + "\n===snip===\n", "#{key}" )
			end
			messages << Poseidon::MessageToSend.new( "#{topic}", msgtail, "#{key}" )
			
			# write to default for audit if required
			# messages << Poseidon::MessageToSend.new( "#{topic}.default", msg, "#{strm}" )
			
		end

		post_messages(messages, :none)		
		return 202
	end

	# 
	# add privacy profile extension to q name if supported and pass on
	# 
	get "/:topic/:stream/:profile" do

		topic = params['topic']
		stream = "#{params['stream']}.#{params['profile']}"
		path = "/#{topic}/#{stream}"
		# puts "\n\nNew Path is #{path}\n\n"
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

end # end of sinatra app class











