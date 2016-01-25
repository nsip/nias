# cons-prod-csv2sif-staffpersonal-naplanreg-parser.rb
# Consumer that reads in StaffPersonal CSV records conforming to the NAPLAN Registration specification from naplan/csv_staff stream,
# and generates SIF/XML equivalent records in naplan/sifxmlout_staff stream.

# Deals with multiple class codes in CSV, assumes they are represented in CSV as "classcode1,classcode2,classcode3"


require 'nokogiri'
require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require_relative 'cvsheaders-naplan'

@inbound = 'naplan.csv_staff'
@outbound = 'naplan.sifxmlout_staff'

@idgen = Hashids.new( 'nsip random temp uid' )

@servicename = 'cons-prod-csv2sif-staffpersonal-naplanreg-parser'

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

loop do

    begin
        messages = []
        outbound_messages = []
        messages = consumer.fetch
                messages.each do |m|
            row = JSON.parse(m.value) 
            # Carriage return unacceptable
            row.each_key do |key|
                row[key].gsub!("[ ]*\n[ ]*", " ")
            end
            classcodes = row['ClassCode'].split(/,/)
            classcodes_xml = ''
            classcodes.each { |x| classcodes_xml << "      <ClassCode>#{x}</ClassCode>\n" }

            xml = <<XML
<StaffPersonal RefId="#{SecureRandom.uuid}">
  <LocalId>#{row['LocalId']}</LocalId>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>#{row['FamilyName']}</FamilyName>
      <GivenName>#{row['GivenName']}</GivenName>
    </Name>
    <EmailList>
      <Email Type="01">#{row['EmailAddress']}</Email>
    </EmailList>
  </PersonInfo>
  <MostRecent>
    <SchoolLocalId>#{row['SchoolLocalId']}</SchoolLocalId>
    <SchoolACARAId>#{row['ASLSchoolId']}</SchoolACARAId>
    <LocalCampusId>#{row['LocalCampusId']}</LocalCampusId>
    <NAPLANClassList>
#{classcodes_xml}
    </NAPLANClassList>
    <HomeGroup>#{row['Homegroup']}</HomeGroup>
  </MostRecent>

</StaffPersonal>
XML


            nodes = Nokogiri::XML( xml ) do |config|
                config.nonet.noblanks
            end
            nodes.xpath('//StaffPersonal//child::*[not(node())]').each do |node|
                node.remove
            end
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", nodes.root.to_s, "indexed" )
        end

        # send results to indexer to create sms data graph
        outbound_messages.each_slice(20) do | batch |
            @pool.next.send_messages( batch )
        end


        # puts "cons-prod-oneroster-parser: Resuming message consumption from: #{consumer.next_offset}"

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





