# cons-prod-sif2scv-studentpersonal-naplanreg-parser.rb

# consumer that reads in studentpersonal records from naplan/sifxml stream, 
# and generates csv equivalent records in naplan/csvstudents stream

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'

@inbound = 'naplan.sifxml.none'
@outbound = 'naplan.csvstudents'

@servicename = 'cons-prod-sif2scv-studentpersonal-naplanreg-parser'

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
        #consumer.fetch do |n, messages|

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

            type = nodes.root.name
            next unless type == 'StudentPersonal'

            csv['LocalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId")
            csv['SectorId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'SectorStudentId']")
            csv['DiocesanId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'DiocesanStudentId']")
            csv['OtherId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'OtherStudentId']")
            csv['TAAId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'TAAStudentId']")
            csv['JurisdictionId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:StateProvinceId")
            csv['NationalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NationalStudentId']")
            csv['PlatformId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
            csv['PreviousLocalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousLocalSchoolStudentId']")
            csv['PreviousSectorId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousSectorStudentId']")
            csv['PreviousDiocesanId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousDiocesanStudentId']")
            csv['PreviousOtherId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousOtherStudentId']")
            csv['PreviousTAAId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousTAAStudentId']")
            csv['PreviousJurisdictionId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousStateProvinceId']")
            csv['PreviousNationalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNationalStudentId']")
            csv['PreviousPlatformId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNAPPlatformStudentId']")
            csv['FamilyName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
            csv['GivenName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
            csv['PreferredName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:PreferredGivenName")
            csv['MiddleName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:MiddleName")
            csv['BirthDate'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
            csv['Sex'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:Sex")
            csv['CountryOfBirth'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:CountryOfBirth")
            csv['EducationSupport'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:EducationSupport")
            csv['FFPOS'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FFPOS")
            csv['VisaCode'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:VisaSubClass")
            csv['IndigenousStatus'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:IndigenousStatus")
            csv['LBOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LBOTE")
            csv['StudentLOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LanguageList/xmlns:Language[xmlns:LanguageType = 4]/xmlns:Code")
            csv['YearLevel'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:YearLevel/xmlns:Code")
            csv['TestLevel'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel/xmlns:Code")
            csv['FTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FTE")
            csv['HomeGroup'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Homegroup")
            csv['ClassCode'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ClassCode")
            csv['ASLSchoolId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
            csv['SchoolLocalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
            csv['LocalCampusId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
            csv['MainSchoolFlag'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:MembershipType") 
            csv['OtherSchoolId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:OtherEnrollmentSchoolACARAId")
            csv['ReportingSchoolId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ReportingSchoolId")
            csv['HomeSchooledStudent'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:HomeSchooledStudent")
            csv['Sensitive'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:Sensitive")
            csv['OfflineDelivery'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OfflineDelivery")
            csv['Parent1SchoolEducation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1SchoolEducationLevel")
            csv['Parent1NonSchoolEducation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1NonSchoolEducation")
            csv['Parent1Occupation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1EmploymentType")
            csv['Parent1LOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1Language")
            csv['Parent2SchoolEducation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2SchoolEducationLevel")
            csv['Parent2NonSchoolEducation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2NonSchoolEducation")
            csv['Parent2Occupation'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2EmploymentType")
            csv['Parent2LOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2Language")
            csv['AddressLine1'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line1")
            csv['AddressLine2'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line2")
            csv['Locality'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:City")
            csv['Postcode'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:PostalCode")
            csv['StateTerritory'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:StateProvince")

            # puts "\nParser Index = #{idx.to_json}\n\n"
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", CSVHeaders.csv_object2array(csv, CSVHeaders.get_csvheaders_students()).to_csv.chomp.gsub(/\s+/, " ") + "\n", "rcvd:#{ sprintf('%09d', m.offset)}" )
        #end
        # send results to indexer to create sms data graph
        #outbound_messages.each_slice(20) do | batch |
            #puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
            #@pool.next.send_messages( batch )
            producers.send_through_queue( outbound_messages )
        #end
	    outbound_messages = []
        end


        # puts "cons-prod-sif-parser: Resuming message consumption from: #{consumer.next_offset}"
=begin
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
