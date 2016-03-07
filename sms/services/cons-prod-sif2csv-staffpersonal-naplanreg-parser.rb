# cons-prod-sif2scv-studentpersonal-naplanreg-parser.rb

# Consumer that reads in StaffPersonal XML records conforming to the NAPLAN Registration specification from naplan/sifxml_staff stream,
# and generates SIF/XML equivalent records in naplan/csvstaff_out stream.

# Deals with multiple class codes in CSV, assumes they are represented in CSV as "classcode1,classcode2,classcode3"

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'

@inbound = 'naplan.sifxml_staff.none'
@outbound = 'naplan.csvstaff_out'

@servicename = 'cons-prod-sif2csv-staffpersonal-naplanreg-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


#puts "#{@servicename} fetching offset #{ consumer.offset(0) } "

producers = KafkaProducers.new(@servicename, 10)
#@pool = producers.get_producers.cycle

=begin
loop do

    begin
=end
        messages = []
        outbound_messages = []
        #messages = consumer.fetch

        #puts "#{@servicename} fetching offset #{ consumer.offset(n) } "
        #puts messages[0].value.lines[0..10].join("\n") + "\n\n" unless messages.empty?
        consumer.each do |m|

            # create csv object
            csv = { }
            payload = m.value

            # read xml message
            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end      		

	    next if nodes.nil?
            type = nodes.root.name
            next unless type == 'StaffPersonal'

            csv['LocalStaffId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId")
            csv['FamilyName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
            csv['GivenName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
            csv['HomeGroup'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:HomeGroup")
            csv['ClassCode'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:NAPLANClassList/xmlns:ClassCode")
            csv['ASLSchoolId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
            csv['LocalSchoolId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
            csv['LocalCampusId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
            csv['EmailAddress'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:EmailList/xmlns:Email")
            csv['StaffSchoolRole'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:Title")
            csv['AdditionalInfo'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:SIF_ExtendedElements/xmlns:SIF_ExtendedElement[@Name = 'AdditionalInfo']")

            # puts "\nParser Index = #{idx.to_json}\n\n"
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", CSVHeaders.csv_object2array(csv, CSVHeaders.get_csvheaders_staff()).to_csv.chomp.gsub(/\s+/, " ") + "\n", "rcvd:#{ sprintf('%09d', m.offset)}" )
        #end
        # send results to indexer to create sms data graph
        #outbound_messages.each_slice(20) do | batch |
            #puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
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
