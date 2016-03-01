require 'poseidon'
require 'thread'

class KafkaProducers

	def initialize(id, size)
        	producers = []
        	bulk_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
        	(1..size).each do | i |
            		p = Poseidon::Producer.new(["localhost:9092"], id, {:partitioner => bulk_partitioner})
            		#p = Poseidon::Producer.new(["localhost:9092"], id)
            		producers << p
        	end
		@id = id
		@size = size
		@producers = producers
		@queue = Queue.new
		@producers.each do |p| 
			@queue << p
		end
		@spare_queue = Queue.new
	end

	def get_producers
		return @producers
	end

	# need queue of producers, since kafkaconsumers spawns a new thread
	def send_through_queue(outbound_messages)
		outbound_messages.each_slice(20) do | batch |
			if(@size == 1)
				@producers[0].send_messages(batch)
			else
				if @queue.empty?
					#refill @queue from @spare_queue
					until @spare_queue.empty?
						p = @spare_queue.pop
						@queue.push p
					end
				end
				p = @queue.pop
				p.send_messages(batch)
				@spare_queue << p
			end
		end
	end

end

