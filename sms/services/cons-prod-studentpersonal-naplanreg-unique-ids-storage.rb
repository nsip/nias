# cons-prod-studentpersonal-naplanreg-unique-ids-storage.rb

# Create indexes of School+LocalId and of School+FirstName+GivenName+DOB for all student records in Redis.
# Raise error if any duplicate keys found.
# Redis indexes are of form SchoolLocalId:### and SchoolNameDOB::###

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'
require 'redis'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'

# extract School+ LocalId
# id is GUID, nodes is Nokogiri-parsed XML
def extract_SchoolLocalId(nodes)
    ret = nil
    schoolId = nodes.at_xpath("//xmlns:MostRecent/xmlns:SchoolACARAId")
    localId = nodes.at_xpath("//xmlns:LocalId")
    schoolId1 = schoolId.child || nil if schoolId
    localId1 = localId.child || nil if localId
    ret = "#{schoolId1}::#{localId1}" if schoolId1 and localId1
    return ret
end

def extract_SchoolNameDOB(nodes)
    ret = nil
    schoolId = nodes.at_xpath("//xmlns:MostRecent/xmlns:SchoolACARAId")
    givenname = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
    familyname = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
    dob = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
    schoolId1 = schoolId.child || nil if schoolId
    givenname1 = givenname.child || nil if givenname
    familyname1 = familyname.child || nil if familyname
    dob1 = dob.child || nil if dob
    ret = "#{schoolId1}::#{givenname1}::#{familyname1}::#{dob1}" if schoolId1 and givenname1 and familyname1 and dob1
    return ret
end

@inbound = 'sifxml.processed'
@outbound1 = 'sifxml.errors'
@outbound2 = 'csv.errors'
@servicename = 'cons-prod-studentpersonal-naplanreg-unique-ids-storage.rb'

@idgen = Hashids.new( 'nsip random temp uid' )
@redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)

# create consumer
#consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }

producers = KafkaProducers.new(@servicename, 10)
#@pool = producers.get_producers.cycle

=begin
loop do

    begin
=end
        messages = []
        #messages = consumer.fetch
        outbound_messages = []
        consumer.each do |m|

            header = m.value.lines[0]
            payload = m.value.lines[1..-1].join
	    errors = []
            fromcsv = payload["<!-- CSV line"]
            csvline = payload[/<!-- CSV line (\d+) /, 1]
            csvcontent = payload[/<!-- CSV content (.+) -->/, 1]

            # read xml message
            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end      		

            type = nodes.root.name
	    next unless type == "StudentPersonal"

            # get any pure refids
	    refId = ''
            root_types = nodes.xpath("//@RefId")  
            root_types.each { | node | refId = node.child }

	    school_local_id = extract_SchoolLocalId(nodes)
	    school_name_dob = extract_SchoolNameDOB(nodes)

	    @redis.sadd "SchoolLocalId::#{school_local_id}", refId
	    if(Integer(@redis.scard("SchoolLocalId::#{school_local_id}")) > 1)
		errors << "Error: There is a duplicate entry with the ACARA School Id + Local Id #{school_local_id}"
            end
	    @redis.sadd "SchoolNameDOB::#{school_name_dob}", refId
	    if(Integer(@redis.scard("SchoolNameDOB::#{school_name_dob}")) > 1)
		errors << "Warning: There is a duplicate entry with the ACARA School Id, Given Name, Family Name and Date of Birth #{school_name_dob}"
	    end
            errors.each_with_index do |e, i|
                outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", e + "\n" + payload, "rcvd:#{ sprintf('%09d:%d', m.offset, i)}" )
                outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", "CSV line #{csvline}: " + e + "\n" + csvcontent, "rcvd:#{ sprintf('%09d:%d', m.offset, i)}" ) if fromcsv
            end
        #end

        #outbound_messages.each_slice(20) do | batch |
            #@pool.next.send_messages( batch )
           producers.send_through_queue( outbound_messages )
        #end
	outbound_messages = []
end
=begin

        # puts "cons-prod-sif-parser: Resuming message consumption from: #{consumer.next_offset}"

    rescue Poseidon::Errors::UnknownTopicOrPartition
        puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
        sleep 30
    end
    # puts "Resuming message consumption from: #{consumer.next_offset}"

    # trap to allow console interrupt
    trap("INT") { 
        puts "\n#{@servicename} service shutting down...\n\n"
        exit 130 
    } 

    sleep 1
end
=end
