# cons-prod-sif2csv-SRM-validate.rb

# Consumer that replicates the validation rules of the NAPLAN Student Registration Management System
# Assumes that records are already validated against default SIF/XML schema. Pushes errors to sifxml.errors, and to
# csv.errors if the file was originally CSV

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'

@inbound = 'sifxml.validated'
@outbound1 = 'sifxml.errors'
@outbound2 = 'csv.errors'

@servicename = 'cons-prod-sif2csv-staffpersonal-naplanreg-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
    p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
    producers << p
end
@pool = producers.cycle

@naplan_topics = %w(naplan.sifxml naplan.sifxml_staff naplan.sifxmlout naplan.sifxmlout_staff)

def validate_staff(nodes)
	ret = []
       	emailaddress = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:EmailList/xmlns:Email")
       	additionalinfo = CSVHeaders.lookup_xpath(nodes, "//xmlns:SIF_ExtendedElements/xmlns:SIF_ExtendedElement[@Name = 'AdditionalInfo']")
	ret << "Error: Email Address #{emailaddress.to_s} is malformed" if emailaddress and not emailaddress.to_s.match(/^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-zA-Z][a-zA-Z]$/)
	if(additionalinfo)
		unless additionalinfo.to_s.match(/^[ynYN]$/)
			ret << "Error: Addition Info #{additionalinfo.to_s} is not Y or N"
		end
	end
	return ret
end

def validate_student(nodes)
	ret = []
        testlevel = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel/xmlns:Code")
	ret << "Error: '#{testlevel}' is not a valid value for TestLevel: expect 3, 5, 7, 9" if testlevel and not testlevel.to_s.match(/^[3579]$/)

        birthdate = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
        yearlevel_string = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:YearLevel/xmlns:Code")
	begin
		yearlevel = Integer(yearlevel_string.to_s)
	rescue ArgumentError
		yearlevel_string = nil
	end
	if(yearlevel_string and birthdate)
		if(yearlevel > 1 and yearlevel < 12)
			age_in_years = (Date.today - Date.parse(birthdate.to_s)).to_i / (365.24)
			expected_age = yearlevel + 5
			if(age_in_years < expected_age - 1 or age_in_years > expected_age + 2)
				ret << "Error: Date of Birth '#{birthdate} is inconsistent with Year Level #{yearlevel}" 
			end
		end
	end

       	postcode = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:PostalCode")
	ret << "Error: '#{postcode}' is not a valid value for Postcode: expect four digits" if postcode and not postcode.to_s.match(/^\d\d\d\d$/)
        
	stateprovince = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:StateProvince")
	ret << "Error: '#{stateprovince}' is not a valid value for State/Province: expect standard abbreviation" if postcode and not postcode.to_s.match(/^(NSW|VIC|ACT|TAS|SA|WA|QLD|NT)$/)

=begin
            csv['LocalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId")
            csv['SectorId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'SectorStudentId']")
            csv['DiocesanId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'DiocesanStudentId']")
            csv['OtherId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'OtherStudentId']")
            csv['TAAId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'TAAStudentId']")
            csv['StateProvinceId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:StateProvinceId")
            csv['NationalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NationalStudentId']")
            csv['PlatformId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
            csv['PreviousLocalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousLocalSchoolStudentId']")
            csv['PreviousSectorId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousSectorStudentId']")
            csv['PreviousDiocesanId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousDiocesanStudentId']")
            csv['PreviousOtherId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousOtherStudentId']")
            csv['PreviousTAAId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousTAAStudentId']")
            csv['PreviousStateProvinceId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousStateProvinceId']")
            csv['PreviousNationalId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNationalStudentId']")
            csv['PreviousPlatformId'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNAPPlatformStudentId']")
            csv['FamilyName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
            csv['GivenName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
            csv['PreferredName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:PreferredGivenName")
            csv['MiddleName'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:MiddleName")
            csv['Sex'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:Sex")
            csv['CountryOfBirth'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:CountryOfBirth")
            csv['EducationSupport'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:EducationSupport")
            csv['FFPOS'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FFPOS")
            csv['VisaCode'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:VisaSubClass")
            csv['IndigenousStatus'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:IndigenousStatus")
            csv['LBOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LBOTE")
            csv['StudentLOTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LanguageList/xmlns:Language[xmlns:LanguageType = 4]/xmlns:Code")
            csv['FTE'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FTE")
            csv['Homegroup'] = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Homegroup")
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
=end

	return ret
end


loop do

    begin
        messages = []
        outbound_messages = []
        messages = consumer.fetch

        messages.each do |m|

	    header = m.value.lines[0]
            topic = header.chomp.gsub(/TOPIC: /,"")
            payload = m.value.lines[1..-1].join
            # we are only interested in XML in NAPLAN topics
            next unless @naplan_topics.grep(topic) 
            fromcsv = payload["<!-- CSV line"]
	    csvline = payload[/<!-- CSV line (\d+) /, 1]
            csvcontent = payload[/<!-- CSV content (.+) -->/, 1]

            # read xml message
            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end      		

            type = nodes.root.name
            next unless type == 'StaffPersonal' or type == 'StudentPersonal'
            errors = validate_staff(nodes) if type == 'StaffPersonal'
            errors = validate_student(nodes) if type == 'StudentPersonal'

	    errors.each do |e|
            	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", e + "\n" + payload, "invalid" )
            	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", "CSV line #{csvline}: " + e + "\n" + csvcontent, "invalid" ) if fromcsv
	    end
        end
        # send results to error streams
        outbound_messages.each_slice(20) do | batch |
            #puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
            @pool.next.send_messages( batch )
        end
        #end


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

