# cons-prod-csv2sif-studentpersonal-naplanreg-parser.rb

# consumer that reads in studentpersonal records from naplan/csv stream,
# and generates csv equivalent records in naplan/sifxmlout stream


require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require 'cvsheaders-naplan.rb'

@inbound = 'naplan.csv'
@outbound = 'naplan.sifxmlout'

@idgen = Hashids.new( 'nsip random temp uid' )

@servicename = 'cons-prod-csv2sif-studentpersonal-naplanreg-parser'


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
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|
		#m = @csvheaders.join(',') +  "\r" + m
		CSV.parse(m, {:headers = @csvheaders}) do |row|

			xml = <<XML
<StudentPersonal RefId="#{SecureRanom.uuid}">
  <LocalId>#{row['Local School Student ID']}</LocalId>
  <StateProvinceId>#{row['Jurisdiction Student ID']}</StateProvinceId>
  <OtherIdList>
    <OtherId Type="SectorStudentId">#{row['Sector Student ID']}</OtherId>
    <OtherId Type="DiocesanStudentId">#{row['Diocesan Student ID']}</OtherId>
    <OtherId Type="OtherStudentId">#{row['Other Student ID']}</OtherId>
    <OtherId Type="TAAStudentId">#{row['TAA Student ID']}</OtherId>
    <OtherId Type="NationalStudentId">#{row['National Student ID']}</OtherId>
    <OtherId Type="NAPPlatformStudentId">#{row['Platform Student ID']}</OtherId>
    <OtherId Type="PreviousLocalSchoolStudentId">#{row['Previous Local School Student ID']}</OtherId>
    <OtherId Type="PreviousSectorStudentId">#{row['Previous Sector Student ID']}</OtherId>
    <OtherId Type="PreviousDiocesanStudentId">#{row['Previous Diocesan Student ID']}</OtherId>
    <OtherId Type="PreviousOtherStudentId">#{row['Previous Other Student ID']}</OtherId>
    <OtherId Type="PreviousTAAStudentId">#{row['Previous TAA Student ID']}</OtherId>
    <OtherId Type="PreviousJurisdictionStudentId">#{row['Previous Jurisdiction Student ID']}</OtherId>
    <OtherId Type="PreviousNationalStudentId">#{row['Previous National Student ID']}</OtherId>
    <OtherId Type="PreviousNAPPlatformStudentId">#{row['Previous Platform Student ID']}</OtherId>
  </OtherIdList>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>#{row['Family Name']}</FamilyName>
      <GivenName>#{row['Given Name']}</GivenName>
      <MiddleName>#{row['Middle Name']}</MiddleName>
      <PreferredGivenName>#{row['Middle Name']}</PreferredGivenName>
    </Name>
    <Demographics>
      <IndigenousStatus>#{row['Indigenous Status']}</IndigenousStatus>
      <Sex>#{row['Sex']}</Sex>
      <BirthDate>#{row['Date Of Birth']}</BirthDate>
      <CountryOfBirth>#{row['Student Country of Birth']}</CountryOfBirth>
      <LanguageList>
        <Language>
          <Code>#{row['Student Main Language Other than English Spoken at Home']}</Code>
          <LanguageType>2</LanguageType>
        </Language>
      </LanguageList>
      <VisaSubClass>#{row['Visa Code']}</VisaSubClass>
      <LBOTE>#{row['LBOTE Status']}</LBOTE>
    </Demographics>
    <AddressList>
      <Address Type="0123" Role="012A">
        <Street>
          <Line1>#{row['Address Line 1']}</Line1>
          <Line2>#{row['Address Line 2']}</Line2>
        </Street>
        <City>#{row['Locality']}</City>
        <Country>1101</Country>
        <PostalCode>#{row['Postcode']}</PostalCode>
      </Address>
    </AddressList>
  </PersonInfo>
  <MostRecent>
    <SchoolLocalId>#{row['Local School ID']}</SchoolLocalId>
    <YearLevel>
      <Code>#{row['Year Level']}</Code>
    </YearLevel>
    <FTE>#{row['FTE']}</FTE>
    <Parent1Language>#{row['Parent 1 Main Language Other than English Spoken at Home']}</Parent1Language>
    <Parent2Language>#{row['Parent 2 Main Language Other than English Spoken at Home']}</Parent2Language>
    <Parent1EmploymentType>#{row['Parent 1 Occupation']}</Parent1EmploymentType>
    <Parent2EmploymentType>#{row['Parent 2 Occupation']}</Parent2EmploymentType>
    <Parent1SchoolEducationLevel>#{row['Parent 1 School Education']}</Parent1SchoolEducationLevel>
    <Parent2SchoolEducationLevel>#{row['Parent 2 School Education']}</Parent2SchoolEducationLevel>
    <Parent1NonSchoolEducation>#{row['Parent 1 Non-School Education']}</Parent1NonSchoolEducation>
    <Parent2NonSchoolEducation>#{row['Parent 2 Non-School Education']}</Parent2NonSchoolEducation>
    <LocalCampusId>#{row['Local Campus ID']}</LocalCampusId>
    <SchoolACARAId>#{row['ASL School ID']}</SchoolACARAId>
    <TestLevel>#{row['Test Level']}</TestLevel>
    <Homegroup>#{row['Home Group']}</Homegroup>
    <ClassCode>#{row['Class Code']}</ClassCode>
    <MembershipType>#{row['Main School Flag']}</MembershipType>
    <FFPOS>#{row['Full Fee Paying Student']}</FFPOS>
    <ReportingSchoolId>#{row['Reporting School ID']}</ReportingSchoolId>
    <OtherEnrollmentSchoolACARAId>#{row['Reporting School ID']}</OtherEnrollmentSchoolACARAId>
  </MostRecent>
  <EducationSupport>#{row['Education Support']}</EducationSupport>
  <HomeSchooledStudent>#{row['Home Schooled Student']}</HomeSchooledStudent>
  <Sensitive>#{row['Sensitive']}</Sensitive>
  <OfflineDelivery>#{row['Offline Delivery']}</OfflineDelivery>
</StudentPersonal>

XML

			nodes = Nokogiri::XML( payload ) do |config|
                        	config.nonet.noblanks
                        end
			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", nodes.to_s, "indexed" )
  		
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
    puts "\ncons-prod-oneroster-parser service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end





