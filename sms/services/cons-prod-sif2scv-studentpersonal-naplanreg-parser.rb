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
	@ret = Array.new(@csvheaders.length)
	@csvheaders.each_with_index do |key, i|
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
	    
	    messages.each do |m|

	    	# create csv object
		csv = { }
		#header = m.value.lines[0]
            	#payload = m.value.lines[1..-1].join
            	payload = m.value

      		# read xml message
      		nodes = Nokogiri::XML( payload ) do |config|
        		config.nonet.noblanks
			end      		

		        type = nodes.root.name
			next unless type == 'StudentPersonal'

			csv['Local School Student ID'] = lookup_xpath(nodes, "//xmlns:LocalId")
			csv['Sector Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'SectorStudentId']")
			csv['Diocesan Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'DiocesanStudentId']")
			csv['Other Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'OtherStudentId']")
			csv['TAA Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'TAAStudentId']")
			csv['Jurisdiction Student ID'] = lookup_xpath(nodes, "//xmlns:StateProvinceId")
			csv['National Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NationalStudentId']")
			csv['Platform Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
			csv['Previous Local School Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousLocalSchoolStudentId']")
			csv['Previous Sector Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousSectorStudentId']")
			csv['Previous Diocesan Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousDiocesanStudentId']")
			csv['Previous Other Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousOtherStudentId']")
			csv['Previous TAA Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousTAAStudentId']")
			csv['Previous Jurisdiction Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousStateProvinceId']")
			csv['Previous National Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNationalStudentId']")
			csv['Previous Platform Student ID'] = lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNAPPlatformStudentId']")
			csv['Family Name'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
			csv['Given Name'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
			csv['Preferred Given Name'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:PreferredGivenName")
			csv['Middle Name'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:MiddleName")
			csv['Date Of Birth'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
			csv['Sex'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:Sex")
			csv['Student Country of Birth'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:CountryOfBirth")
			csv['Education Support'] = lookup_xpath(nodes, "//xmlns:EducationSupport")
			csv['Full Fee Paying Student'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FFPOS")
			csv['Visa Code'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:VisaSubClass")
			csv['Indigenous Status'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:IndigenousStatus")
			csv['LBOTE Status'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LBOTE")
			csv['Student Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LanguageList/xmlns:Language[xmlns:LanguageType = 4]/xmlns:Code")
			csv['Year Level'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:YearLevel")
			csv['Test Level'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel")
			csv['FTE'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FTE")
			csv['Home Group'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Homegroup")
			csv['Class Code'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ClassCode")
			csv['ASL School ID'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
			csv['Local School ID'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
			csv['Local Campus ID'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
			csv['Main School Flag'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:MembershipType") == '01' ? 'Y' : 'N'
			csv['Other School ID'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:OtherEnrollmentSchoolACARAId")
			csv['Reporting School ID'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ReportingSchoolId")
			csv['Home Schooled Student'] = lookup_xpath(nodes, "//xmlns:HomeSchooledStudent")
			csv['Sensitive'] = lookup_xpath(nodes, "//xmlns:Sensitive")
			csv['Offline Delivery'] = lookup_xpath(nodes, "//xmlns:OfflineDelivery")
			csv['Parent 1 School Education'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1SchoolEducationLevel")
			csv['Parent 1 Non-School Education'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1NonSchoolEducation")
			csv['Parent 1 Occupation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1EmploymentType")
			csv['Parent 1 Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1Language")
			csv['Parent 2 School Education'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2SchoolEducationLevel")
			csv['Parent 2 Non-School Education'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2NonSchoolEducation")
			csv['Parent 2 Occupation'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2EmploymentType")
			csv['Parent 2 Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2Language")
			csv['Address Line 1'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address/xmlns:Street/xmlns:Line1")
			csv['Address Line 2'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address/xmlns:Street/xmlns:Line2")
			csv['Locality'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address/xmlns:City")
			csv['Postcode'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address/xmlns:PostalCode")
			csv['State or Territory'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address/xmlns:StateProvince")

			# puts "\nParser Index = #{idx.to_json}\n\n"

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", csv_object2array(csv).to_csv, "indexed" )
  		
  		end

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
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

