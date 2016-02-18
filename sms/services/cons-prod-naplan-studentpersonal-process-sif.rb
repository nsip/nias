# cons-prod-naplan-studentpersonal-process-sif.rb
# Consumer that reads in StudentPersonal XML records conforming to the NAPLAN Registration specification from sifxml.validated
# and inserts Platform Student Identifier if one has not already been supplied
# Only deals with topics naplan.sifxml or naplan.sifxmlout, which have been skipped by ssf/services/cons-prod-sif-process.rb

# if called with arg "psi", generate and inject PSI identifier if it is not present in the source object

require 'nokogiri'
require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require_relative 'cvsheaders-naplan'
require_relative '../../Luhn'

@inbound = 'sifxml.validated'
@outbound = 'sifxml.processed'

@idgen = Hashids.new( 'nsip random temp uid' )

@servicename = 'cons-prod-naplan-studentpersonal-process-sif'

@psi = ARGF.argv.include?('psi')

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
@inbound, 0, :latest_offset)


# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
    p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
    producers << p
end
@pool = producers.cycle

@accepted_topics = %w(naplan.sifxml naplan.sifxmlout)

@checksum_to_letter = {
0 => 'K',
1 => 'M',
2 => 'R',
3 => 'A',
4 => 'S',
5 => 'P',
6 => 'D',
7 => 'H',
8 => 'E',
9 => 'G'
}

def checksum_letter(number)
	ret = ''
	checksum = Luhn.checksum(number)
	return @checksum_to_letter[checksum]
end

def new_psi
         psi_number = sprintf("%010d",rand(1...99999999))
         return "R1" + psi_number + checksum_letter(psi_number)
end


loop do
    begin 
        outbound_messages = []
        outbound_errors = []
        messages = []
        messages = consumer.fetch
        messages.each do |m|
            #puts "Validate: processing message no.: #{m.offset}, #{m.key}\n\n"
    
            # Payload from sifxml.validated contains as its first line a header line with the original topic
            header = m.value.lines[0]
            payload = m.value.lines[1..-1].join
            topic = header.chomp.gsub(/TOPIC: /,"")
            next unless @accepted_topics.include?(topic)
            item_key = "rcvd:#{ sprintf('%09d', m.offset) }"

            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end

	    type = nodes.root.name
	    next unless type == "StudentPersonal"
	    psi_nodes = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
	    if psi_nodes.to_s.empty? and @psi
		psi_id = new_psi()
		psi = Nokogiri::XML::Node.new "OtherId", nodes
		psi['Type'] = 'NAPPlatformStudentId'
		psi.content = psi_id

		other_ids = nodes.at_xpath("//xmlns:OtherIdList")
		other_ids = Nokogiri::XML::Node.new("OtherIdList", nodes) unless other_ids
		other_ids << psi
	    end

            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", header + nodes.root.to_s, item_key )
        end
        
        outbound_messages.each_slice(20) do | batch |
            @pool.next.send_messages( batch )
        end
            
        # puts "cons-prod-ingest:: Resuming message consumption from: #{consumer.next_offset}"
    rescue Poseidon::Errors::UnknownTopicOrPartition
        puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
        sleep 30
    end     
        # puts "Resuming message consumption from: #{consumer.next_offset}"
            
    # trap to allow console interrupt
    trap("INT") { 
        puts "\n#{@servicename} service shutting down...\n\n"
        consumer.close
        exit 130 
    }       
            
    sleep 1     
end         

