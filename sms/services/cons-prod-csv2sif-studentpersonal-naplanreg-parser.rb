# cons-prod-csv2sif-studentpersonal-naplanreg-parser.rb
# Consumer that reads in StudentPersonal CSV records conforming to the NAPLAN Registration specification from naplan/csv stream,
# and generates SIF/XML equivalent records in naplan/sifxmlout stream


require 'nokogiri'
require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require_relative 'cvsheaders-naplan'

def Postcode2State( postcodestr ) 
    postcode = postcodestr.to_i
    if(postcode < 200) 
        ret = ""
    elsif(postcode < 300) 
        ret = "ACT" 
    elsif(postcode < 800) 
        ret = "" 
    elsif(postcode < 1000) 
        ret = "NT" 
    elsif(postcode < 2600) 
        ret = "NSW" 
    elsif(postcode < 2620) 
        ret = "ACT" 
    elsif(postcode < 2900) 
        ret = "NSW" 
    elsif(postcode < 2921) 
        ret = "ACT" 
    elsif(postcode < 3000) 
        ret = "NSW" 
    elsif(postcode < 4000) 
        ret = "VIC" 
    elsif(postcode < 5000) 
        ret = "QLD" 
    elsif(postcode < 6000) 
        ret = "SA" 
    elsif(postcode < 7000) 
        ret = "WA" 
    elsif(postcode < 8000) 
        ret = "TAS" 
    elsif(postcode < 9000) 
        ret = "VIC" 
    elsif(postcode < 10000) 
        ret = "QLD" 
    else 
        ret = ""
    end
    return ret
end

@inbound = 'naplan.csv'
#@outbound = 'naplan.sifxmlout'
@outbound = 'sifxml.ingest'

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
        outbound_messages = []
        messages = consumer.fetch
                messages.each do |m|
            row = JSON.parse(m.value) 

# inject the source CSV line as a comment into the generated XML; errors found in the templated SIF/XML
# will be reported back in the csv.errors stream

            xml = <<XML
<StudentPersonals  xmlns="http://www.sifassociation.org/au/datamodel/3.4">
<StudentPersonal RefId="#{SecureRandom.uuid}">
<!-- CSV line #{row['__linenumber']} -->
  <LocalId>#{row['LocalId']}</LocalId>
  <StateProvinceId>#{row['StateProvinceId']}</StateProvinceId>
  <OtherIdList>
    <OtherId Type="SectorStudentId">#{row['SectorId']}</OtherId>
    <OtherId Type="DiocesanStudentId">#{row['DiocesanId']}</OtherId>
    <OtherId Type="OtherStudentId">#{row['OtherId']}</OtherId>
    <OtherId Type="TAAStudentId">#{row['TAAId']}</OtherId>
    <OtherId Type="NationalStudentId">#{row['NationalId']}</OtherId>
    <OtherId Type="NAPPlatformStudentId">#{row['PlatformId']}</OtherId>
    <OtherId Type="PreviousLocalSchoolStudentId">#{row['PreviousLocalId']}</OtherId>
    <OtherId Type="PreviousSectorStudentId">#{row['PreviousSectorId']}</OtherId>
    <OtherId Type="PreviousDiocesanStudentId">#{row['PreviousDiocesanId']}</OtherId>
    <OtherId Type="PreviousOtherStudentId">#{row['PreviousOtherId']}</OtherId>
    <OtherId Type="PreviousTAAStudentId">#{row['PreviousTAAId']}</OtherId>
    <OtherId Type="PreviousStateProvinceId">#{row['PreviousStateProvinceId']}</OtherId>
    <OtherId Type="PreviousNationalStudentId">#{row['PreviousNationalId']}</OtherId>
    <OtherId Type="PreviousNAPPlatformStudentId">#{row['PreviousPlatformId']}</OtherId>
  </OtherIdList>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>#{row['FamilyName']}</FamilyName>
      <GivenName>#{row['GivenName']}</GivenName>
      <MiddleName>#{row['MiddleName']}</MiddleName>
      <PreferredGivenName>#{row['PreferredName']}</PreferredGivenName>
    </Name>
    <Demographics>
      <IndigenousStatus>#{row['IndigenousStatus']}</IndigenousStatus>
      <Sex>#{row['Sex']}</Sex>
      <BirthDate>#{row['BirthDate']}</BirthDate>
      <CountryOfBirth>#{row['CountryOfBirth']}</CountryOfBirth>
      <LanguageList>
        <Language>
          <Code>#{row['StudentLOTE']}</Code>
          <LanguageType>4</LanguageType>
        </Language>
      </LanguageList>
      <VisaSubClass>#{row['VisaCode']}</VisaSubClass>
      <LBOTE>#{row['LBOTE']}</LBOTE>
    </Demographics>
    <AddressList>
      <Address Type="0765" Role="012B">
        <Street>
          <Line1>#{row['AddressLine1']}</Line1>
          <Line2>#{row['AddressLine2']}</Line2>
        </Street>
        <City>#{row['Locality']}</City>
        <StateProvince>#{row['StateTerritory']}</StateProvince>
        <Country>1101</Country>
        <PostalCode>#{row['Postcode']}</PostalCode>
      </Address>
    </AddressList>
  </PersonInfo>
  <MostRecent>
    <SchoolLocalId>#{row['SchoolLocalId']}</SchoolLocalId>
    <YearLevel>
      <Code>#{row['YearLevel']}</Code>
    </YearLevel>
    <FTE>#{row['FTE']}</FTE>
    <Parent1Language>#{row['Parent1LOTE']}</Parent1Language>
    <Parent2Language>#{row['Parent2LOTE']}</Parent2Language>
    <Parent1EmploymentType>#{row['Parent1Occupation']}</Parent1EmploymentType>
    <Parent2EmploymentType>#{row['Parent2Occupation']}</Parent2EmploymentType>
    <Parent1SchoolEducationLevel>#{row['Parent1SchoolEducation']}</Parent1SchoolEducationLevel>
    <Parent2SchoolEducationLevel>#{row['Parent2SchoolEducation']}</Parent2SchoolEducationLevel>
    <Parent1NonSchoolEducation>#{row['Parent1NonSchoolEducation']}</Parent1NonSchoolEducation>
    <Parent2NonSchoolEducation>#{row['Parent2NonSchoolEducation']}</Parent2NonSchoolEducation>
    <LocalCampusId>#{row['LocalCampusId']}</LocalCampusId>
    <SchoolACARAId>#{row['ASLSchoolId']}</SchoolACARAId>
    <TestLevel><Code>#{row['TestLevel']}</Code></TestLevel>
    <Homegroup>#{row['Homegroup']}</Homegroup>
    <ClassCode>#{row['ClassCode']}</ClassCode>
    <MembershipType>#{row['MainSchoolFlag']}</MembershipType>
    <FFPOS>#{row['FFPOS']}</FFPOS>
    <ReportingSchoolId>#{row['ReportingSchoolId']}</ReportingSchoolId>
    <OtherEnrollmentSchoolACARAId>#{row['OtherSchoolId']}</OtherEnrollmentSchoolACARAId>
  </MostRecent>
  <EducationSupport>#{row['EducationSupport']}</EducationSupport>
  <HomeSchooledStudent>#{row['HomeSchooledStudent']}</HomeSchooledStudent>
  <Sensitive>#{row['Sensitive']}</Sensitive>
  <OfflineDelivery>#{row['OfflineDelivery']}</OfflineDelivery>
</StudentPersonal>
</StudentPersonals>

XML


            nodes = Nokogiri::XML( xml ) do |config|
                config.nonet.noblanks
            end
	    # remove empty nodes from anywhere in the document
            nodes.xpath('//*//child::*[not(node())]').each do |node|
                node.remove
            end
            outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", "TOPIC: naplan.sifxmlout\n" + nodes.root.to_s, "indexed" )
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





