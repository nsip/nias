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

@inbound = 'naplan.sifxml.none'
@outbound = 'naplan.csvstudents'

@servicename = 'cons-prod-sif2scv-studentpersonal-naplanreg-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)
#consumer = Poseidon::ConsumerGroup.new(@servicename, ["localhost:9092"], ["localhost:2181"], @inbound)

#puts "#{@servicename} fetching offset #{ consumer.offset(0) } "

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
@pool = producers.cycle

def lookup_xpath(nodes, xpath)
	@ret = nodes.at_xpath(xpath)
	return "" if @ret.nil?
	#return "" if @ret.empty?
	return @ret.child
end

def csv_object2array(csv)
	@ret = Array.new(@csvheaders_students.length)
	@csvheaders_students.each_with_index do |key, i|
		@ret[i] = csv[key]
	end
	return @ret
end

loop do

  begin
  	    messages = []
	    outbound_messages = []
	    messages = consumer.fetch
	    #consumer.fetch do |n, messages|

#puts "#{@servicename} fetching offset #{ consumer.offset(n) } "
#puts messages[0].value.lines[0..10].join("\n") + "\n\n" unless messages.empty?
	    messages.each do |m|

	    	# create csv object
		csv = { }
            	payload = m.value

      		# read xml message
      		nodes = Nokogiri::XML( payload ) do |config|
        		config.nonet.noblanks
			end      		

		        type = nodes.root.name
			next unless type == 'StudentPersonal'

			csv['LocalId'] = lookup_xpath(nodes, "//xmlns:LocalId")
			csv['SectorId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'SectorStudentId']")
			csv['DiocesanId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'DiocesanStudentId']")
			csv['OtherId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'OtherStudentId']")
			csv['TAAId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'TAAStudentId']")
			csv['StateProvinceId'] = lookup_xpath(nodes, "//xmlns:StateProvinceId")
			csv['NationalId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NationalStudentId']")
			csv['PlatformId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
			csv['PreviousLocalId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousLocalSchoolStudentId']")
			csv['PreviousSectorId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousSectorStudentId']")
			csv['PreviousDiocesanId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousDiocesanStudentId']")
			csv['PreviousOtherId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousOtherStudentId']")
			csv['PreviousTAAId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousTAAStudentId']")
			csv['PreviousStateProvinceId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousStateProvinceId']")
			csv['PreviousNationalId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNationalStudentId']")
			csv['PreviousPlatformId'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNAPPlatformStudentId']")
			csv['FamilyName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
			csv['GivenName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
			csv['PreferredName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:PreferredGivenName")
			csv['MiddleName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:MiddleName")
			csv['BirthDate'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
			csv['Sex'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:Sex")
			csv['CountryOfBirth'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:CountryOfBirth")
			csv['EducationSupport'] = lookup_xpath(nodes, "//xmlns:EducationSupport")
			csv['FFPOS'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FFPOS")
			csv['VisaCode'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:VisaSubClass")
			csv['IndigenousStatus'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:IndigenousStatus")
			csv['LBOTE'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LBOTE")
			csv['StudentLOTE'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LanguageList/xmlns:Language[xmlns:LanguageType = 4]/xmlns:Code")
			csv['YearLevel'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:YearLevel")
			csv['TestLevel'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel")
			csv['FTE'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FTE")
			csv['Homegroup'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Homegroup")
			csv['ClassCode'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ClassCode")
			csv['ASLSchoolId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
			csv['SchoolLocalId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
			csv['LocalCampusId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
			csv['MainSchoolFlag'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:MembershipType") 
			csv['OtherSchoolId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:OtherEnrollmentSchoolACARAId")
			csv['ReportingSchoolId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ReportingSchoolId")
			csv['HomeSchooledStudent'] = lookup_xpath(nodes, "//xmlns:HomeSchooledStudent")
			csv['Sensitive'] = lookup_xpath(nodes, "//xmlns:Sensitive")
			csv['OfflineDelivery'] = lookup_xpath(nodes, "//xmlns:OfflineDelivery")
			csv['Parent1SchoolEducation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1SchoolEducationLevel")
			csv['Parent1NonSchoolEducation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1NonSchoolEducation")
			csv['Parent1Occupation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1EmploymentType")
			csv['Parent1LOTE'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1Language")
			csv['Parent2SchoolEducation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2SchoolEducationLevel")
			csv['Parent2NonSchoolEducation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2NonSchoolEducation")
			csv['Parent2Occupation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2EmploymentType")
			csv['Parent2LOTE'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2Language")
			csv['AddressLine1'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line1")
			csv['AddressLine2'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line2")
			csv['Locality'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:City")
			csv['Postcode'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:PostalCode")
			csv['StateTerritory'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:StateProvince")

			# puts "\nParser Index = #{idx.to_json}\n\n"
			

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", csv_object2array(csv).to_csv.chomp.gsub(/\s+/, " ") + "\n", "indexed" )
  		
  		end
  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
#puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
			@pool.next.send_messages( batch )
	   	end
		#end


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

