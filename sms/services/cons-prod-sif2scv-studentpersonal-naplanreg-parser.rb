# cons-prod-sif2scv-studentpersonal-naplanreg-parser.rb

# consumer that reads in studentpersonal records from naplan/sifxml stream, 
# and generates csv equivalent records in naplan/csvstudents stream
 

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'

@inbound = 'naplan.sifxml'
@outbound = 'naplan.csvstudents'

@servicename = 'cons-prod-sif2scv-studentpersonal-naplanreg-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

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
	    messages = consumer.fetch
	    outbound_messages = []
	    
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

			csv['Local School Student ID'] = lookup_xpath(nodes, "//LocalId")
			csv['Sector Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'SectorStudentId']")
			csv['Diocesan Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'DiocesanStudentId']")
			csv['Other Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'OtherStudentId']")
			csv['TAA Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'TAAStudentId']")
			csv['Jurisdiction Student ID'] = lookup_xpath(nodes, "//StateProvinceId")
			csv['National Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'NationalStudentId']")
			csv['Platform Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'NAPPlatformStudentId']")
			csv['Previous Local Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousLocalSchoolStudentId']")
			csv['Previous Sector Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousSectorStudentId']")
			csv['Previous Diocesan Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousDiocesanStudentId']")
			csv['Previous Other Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousOtherStudentId']")
			csv['Previous TAA Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousTAAStudentId']")
			csv['Previous Jurisdiction Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousJurisdictionStudentId']")
			csv['Previous National Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousNationalStudentId']")
			csv['Previous Platform Student ID'] = lookup_xpath(nodes, "//OtherIdList/OtherId[@Type = 'PreviousNAPPlatformStudentId']")
			csv['Family Name'] = lookup_xpath(nodes, "//PersonInfo/Name/FamilyName")
			csv['Given Name'] = lookup_xpath(nodes, "//PersonInfo/Name/GivenName")
			csv['Preferred Given Name'] = lookup_xpath(nodes, "//PersonInfo/Name/PreferredGivenName")
			csv['Middle Name'] = lookup_xpath(nodes, "//PersonInfo/Name/MiddleName")
			csv['Date Of Birth'] = lookup_xpath(nodes, "//PersonInfo/Demographics/BirthDate")
			csv['Sex'] = lookup_xpath(nodes, "//PersonInfo/Demographics/Sex")
			csv['Student Country of Birth'] = lookup_xpath(nodes, "//PersonInfo/Demographics/CountryOfBirth")
			csv['Education Support'] = lookup_xpath(nodes, "//EducationSupport")
			csv['Full Fee Paying Student'] = lookup_xpath(nodes, "//MostRecent/FFPOS")
			csv['Visa Code'] = lookup_xpath(nodes, "//PersonInfo/Demographics/VisaSubClass")
			csv['Indigenous Status'] = lookup_xpath(nodes, "//PersonInfo/Demographics/IndigenousStatus")
			csv['LBOTE Status'] = lookup_xpath(nodes, "//PersonInfo/Demographics/LBOTE")
			csv['Student Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//PersonInfo/Demographics/LanguageList/Language[LanguageType = 4]/Code")
			csv['Year Level'] = lookup_xpath(nodes, "//MostRecent/YearLevel")
			csv['Test Level'] = lookup_xpath(nodes, "//MostRecent/TestLevel")
			csv['FTE'] = lookup_xpath(nodes, "//MostRecent/FTE")
			csv['Home Group'] = lookup_xpath(nodes, "//MostRecent/HomeGroup")
			csv['Class Code'] = lookup_xpath(nodes, "//MostRecent/ClassCode")
			csv['ASL School ID'] = lookup_xpath(nodes, "//MostRecent/SchoolACARAId")
			csv['Local School ID'] = lookup_xpath(nodes, "//MostRecent/SchoolLocalId")
			csv['Local Campus ID'] = lookup_xpath(nodes, "//MostRecent/SchoolCampusId")
			csv['Main School Flag'] = lookup_xpath(nodes, "//MostRecent/MembershipType")
			csv['Other School ID'] = lookup_xpath(nodes, "//MostRecent/OtherEnrollmentSchoolStateProvinceId")
			csv['Reporting School ID'] = lookup_xpath(nodes, "//MostRecent/ReportingSchool")
			csv['Home Schooled Student'] = lookup_xpath(nodes, "//HomeSchooledStudent")
			csv['Sensitive'] = lookup_xpath(nodes, "//Sensitive")
			csv['Offline Delivery'] = lookup_xpath(nodes, "//OfflineDelivery")
			csv['Parent 1 School Education'] = lookup_xpath(nodes, "//MostRecent/Parent1SchoolEducation")
			csv['Parent 1 Non-School Education'] = lookup_xpath(nodes, "//MostRecent/Parent1NonSchoolEducation")
			csv['Parent 1 Occupation'] = lookup_xpath(nodes, "//MostRecent/Parent1Occupation")
			csv['Parent 1 Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//MostRecent/Parent1Language")
			csv['Parent 2 School Education'] = lookup_xpath(nodes, "//MostRecent/Parent2SchoolEducation")
			csv['Parent 2 Non-School Education'] = lookup_xpath(nodes, "//MostRecent/Parent2NonSchoolEducation")
			csv['Parent 2 Occupation'] = lookup_xpath(nodes, "//MostRecent/Parent2Occupation")
			csv['Parent 2 Main Language Other than English Spoken at Home'] = lookup_xpath(nodes, "//MostRecent/Parent2Language")
			csv['Address Line 1'] = lookup_xpath(nodes, "//PersonInfo/AddressList/Address/Street/Line1")
			csv['Address Line 2'] = lookup_xpath(nodes, "//PersonInfo/AddressList/Address/Street/Line2")
			csv['Locality'] = lookup_xpath(nodes, "//PersonInfo/AddressList/Address/City")
			csv['Postcode'] = lookup_xpath(nodes, "//PersonInfo/AddressList/Address/PostalCode")

			# puts "\nParser Index = #{idx.to_json}\n\n"

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", csv_object2array(csv).to_csv, "indexed" )
  		
  		end

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
			@pool.next.send_messages( batch )
	   	end


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

