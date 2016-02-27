require 'poseidon'

class KafkaProducers

	def initialize(id, size)
        	producers = []
        	bulk_partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
        	(1..size).each do | i |
            		p = Poseidon::Producer.new(["localhost:9092"], id, {:partitioner => bulk_partitioner})
            		producers << p
        	end
		@id = id
		@size = size
		@producers = producers
	end

	def get_producers
		return @producers
	end

end

