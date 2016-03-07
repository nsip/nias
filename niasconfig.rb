# Configurations for the NIAS suite
require 'redis'

class NiasConfig 

	@@HOST = 'localhost'
	@@SINATRA_PORT = '9292'
	@@REDIS = 'localhost'
	@@ZOOKEEPER = 'localhost:2181'
	@@KAFKA_PORT = '9092'
	@@KAFKA_HOST = 'localhost'

	def get_host 
		return @@HOST
	end

	def get_sinatra_port
		return @@SINATRA_PORT
	end

	def redis
		return Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)
	end

	def zookeeper
		return @@ZOOKEEPER
	end

	def kafka
		return "#{@@KAFKA_HOST}:#{@@KAFKA_PORT}"
	end

	def kafka_port
		return @@KAFKA_PORT
	end

	def kafka_host
		return @@KAFKA_HOST
	end

end
