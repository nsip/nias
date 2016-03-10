require_relative './niasconfig'
require 'kafka-consumer'

class KafkaConsumers


	def initialize(id, topics, offset = :latest_offset)
		config = NiasConfig.new
		@consumer =  Kafka::Consumer.new(id, topics, zookeeper: "#{config.zookeeper}", initial_offset: offset)
	end

	def interrupt
		return @consumer.interrupt
	end

	def stop
		return @consumer.stop
	end

	def each(&block)
		return @consumer.each(&block)
	end

	# fetch messages until either secs seconds elapse or n messages are read, whichever comes first
	def fetch(n, secs)
		now = Time.now
		msgs = []
puts "n #{n} --- secs #{secs}"
		@consumer.each do |m|
			msgs << m
			#puts m[0].value
puts " >> n #{msgs.length}: offset #{m.offset} --- secs #{Time.now - now}"
			@consumer.stop if Time.now > now + secs
			@consumer.stop if msgs.length == n
		end
		return msgs
	end

end

