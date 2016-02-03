# naplan_server.rb


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


# simple example web service to show harnessing parallel 
# kafka processes, in this case the method 
# recieves xml wrapped as an item in a json structure - see the test data file
# 10k_naplan_data.json.
# In reality csv file would have been processed elsewhere into simple
# single xml messages for the services to consume, this just allows us to skip
# that step in order to test parallelisation.
# 
# key point is to use no key in message or a assign a partiion key algorithm
# so that multiple paritions can be user per topic.
# 
# a test topic and parallel consumer is set up by the launch_cluster script 
# test by sending 10k_naplan_data.json to :/naplan/json/cluster
# 
class Naplan_SSFServer < Sinatra::Base

	helpers Sinatra::ContentFor

	configure do

		set :hashid, Hashids.new( "naplan gregor samza" )
		
	end

	# send messages - messages assumed to be in the data file sent
	# as part of the request, handles json without needing content type specified
	# xml & csv need explicit setting of the Content-Type header 
	# 
	# 
	post "/naplan/:topic/:stream" do
		content_type 'application/json'
		
		# check validity of route
		tpc = params['topic']
		strm = params['stream']
		topic_name = "kafka.#{tpc}.#{strm}"
		

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
		
		messages = []
		raw_messages.each do | msg |

			topic = "#{topic_name}"
			key = "#{strm}"

			case request.media_type
			when 'application/json' then msg = msg.to_json
			when 'application/xml' then msg =  msg.to_s
			when 'text/csv' then msg = msg.to_hash.to_json
			end
			
			messages << Poseidon::MessageToSend.new( "#{topic}", msg )
						
		end

		# custom partioner to allow allocation to multiple partitions for 
		# parallel processing if keys are provided
		# ...taking the easy way for demo by having no keys which will result
		# in round-robin allocation to partitions
		# 
		# bulk_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
		
		# set up producer pool - busier the broker the better for speed
		producers = []
		(1..10).each do | i |
			# p = Poseidon::Producer.new(["localhost:9092"], producer_id, {:partitioner => bulk_partitioner})
			p = Poseidon::Producer.new(["localhost:9092"], producer_id )
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


end # end of sinatra app class











