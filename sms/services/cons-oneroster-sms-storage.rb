# cons-oneroster-sms-storage.rb


# service reads validated JSON messages from the One Roster validation service.
# Parses mesages to estabish primary id, and then simply saves whole message to key value storage
# with id as key and message as value.

# Uses the Moneta abstraction driver which allows for unified simple put/get interface over any number of 
# key value stores. Default implementation for NIAS will use LMDB, but users are free to use any of the list supported 
# by hte moneta driver, which should alow for cross-platform support.
# 

# Note most embedded databases and k/v stores are binary distriutions so need to be installed on the target platform independently.

require 'json'
require 'poseidon'
require 'hashids'
require 'moneta'

@inbound = 'oneroster.validated'

@store = Moneta.new( :LMDB, dir: '/tmp/moneta', db: 'oneroster-messages')

@idgen = Hashids.new( 'nsip random temp uid' )

@service_name = 'cons-oneroster-sms-storage'

# create consumer
consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092,
                                           @inbound, 0, :latest_offset)


loop do

  begin
  		messages = []
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|

	    	# create 'empty' index tuple, otherids and links will be unused here but keeps all parsing code consistent
			idx = { :type => nil, :id => @idgen.encode( rand(1...999) ) }

      		# read JSON message
                idx_hash = JSON.parse( m.value )

		# type of converted CSV One Roster record depends on presence of particular field
		idx[:type] = 'orgs' if idx_hash.haskey?("metadata.boarding")
		idx[:type] = 'users' if idx_hash.haskey?("username")
		idx[:type] = 'courses' if idx_hash.haskey?("courseCode")
		idx[:type] = 'classes' if idx_hash.haskey?("classCode")
		idx[:type] = 'enrollments' if idx_hash.haskey?("primary")
		idx[:type] = 'academicSessions' if idx_hash.haskey?("startDate")
		idx[:type] = 'demographics' if idx_hash.haskey?("sex")

			idx[:id] = idx[:type] == 'demographics' ? idx_hash["userSourcedId"] :  idx_hash["sourcedId"]

			puts "\nStorage Index = #{idx.to_json}\n\n"

			# write the message to storage with its own refid as the key
			# puts "\n\nkey value pair will be:\n\nKEY: #{idx[:id]}\n\nVALUE:\n\n#{nodes.to_s}"

			@store["#{idx[:id]}"] = m.value
  		
  		end


      # puts "#{@service_name}: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@service_name} service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end








