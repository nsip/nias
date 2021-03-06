# cons-prod-csv2sif-studentpersonal-naplanreg-parser.rb
# Consumer that reads in StudentPersonal CSV records conforming to the NAPLAN Registration specification from naplan/csv stream,
# and generates SIF/XML equivalent records in naplan/sifxmlout stream


require 'nokogiri'
require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require 'json-schema'
require_relative 'cvsheaders-naplan'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'
require_relative '../../niaserror'

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
@errbound = 'csv.errors'

@idgen = Hashids.new( 'nsip random temp uid' )

@servicename = 'cons-prod-csv2sif-studentpersonal-naplanreg-parser'

# http://stackoverflow.com/questions/3450641/removing-all-empty-elements-from-a-hash-yaml
class Hash
        def compact
                delete_if { |k, v| v.nil? or (v.respond_to?('empty?') and v.strip.empty?) }
        end
end


# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }



producers = KafkaProducers.new(@servicename, 10)

# default values
@default_csv = {'OfflineDelivery' => 'N', 'Sensitive' => 'Y', 'HomeSchooledStudent' => 'N', 'EducationSupport' => 'N', 'FFPOS' => 'N', 'MainSchoolFlag' => '01' , "AddressLine2" => ""}

# JSON schema
@jsonschemafile = File.read("#{__dir__}/naplan.student.json")
@jsonschema = JSON.parse(@jsonschemafile)

        messages = []
        outbound_messages = []
	recordid = 0
        consumer.each do |m|
	    recordid = recordid + 1
            row = JSON.parse(m.value) 
            row.merge!(@default_csv) { |key, v1, v2| v1 }

        doctype_error_per_docid = {}


            # validate that we have received the right kind of record here, from the headers
            if(row['LocalStaffId'] and not row['LocalId'])
                # only send this error out once per docid
                unless doctype_error_per_docid[row["__docid"]]
puts row["__docid"]
                	outbound_messages << Poseidon::MessageToSend.new( "#{@errbound}", 
				NiasError.new(0,1, row["__docid"], "CSV Document Type Error",
					"You appear to have submitted a StaffPersonal record instead of a StudentPersonal record\n#{row['__linecontent']}").to_s, 
				"rcvd:#{ sprintf('%09d:%d', m.offset, 0)}" )
            		producers.send_through_queue( outbound_messages )
			outbound_messages = []
			doctype_error_per_docid[row["__docid"]] = true
			next
		end
            else

                # mappings of CSV alternate values
		row['FFPOS'] = '1' if row['FFPOS'] == 'Y'
		row['FFPOS'] = '2' if row['FFPOS'] == 'N'
		row['FFPOS'] = '9' if row['FFPOS'] == 'U'
		row['FFPOS'] = '9' if row['FFPOS'] == 'X'
		row['MainSchoolFlag'] = '01' if row['MainSchoolFlag'] == 'Y'
		row['MainSchoolFlag'] = '02' if row['MainSchoolFlag'] == 'N'
		# delete any blank/empty values
		row = row.compact



		# validate against JSON Schema
		json_errors = JSON::Validator.fully_validate(@jsonschema, row)
		# any errors are on mandatory elements, so stop processing further
		if(!json_errors.empty?)
			json_errors.each_with_index do |e, i|
				puts e
                		outbound_messages << Poseidon::MessageToSend.new( "#{@errbound}", NiasError.new(i, json_errors.length, row["__docid"], "Mandatory Element Check",
						"#{e}\n#{row['__linecontent']}").to_s,
					"rcvd:#{ sprintf('%09d:%d', m.offset, i)}" )
			end
            		producers.send_through_queue( outbound_messages )
			outbound_messages = []
			next
		end



# inject the source CSV line number as a comment into the generated XML; errors found in the templated SIF/XML
# will be reported back in the csv.errors stream

                xml = <<XML
    <StudentPersonals  xmlns="http://www.sifassociation.org/au/datamodel/3.4">
    <StudentPersonal RefId="#{SecureRandom.uuid}">
    <!-- CSV line #{row['__linenumber']} -->
    <!-- CSV content #{row['__linecontent']} -->
    <!-- CSV docid #{row['__docid']} -->
    <!-- CSV linetotal #{row['__linetotal']} -->
      <LocalId>#{row['LocalId']}</LocalId>
      <StateProvinceId>#{row['JurisdictionId']}</StateProvinceId>
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
        <OtherId Type="PreviousStateProvinceId">#{row['PreviousJurisdictionId']}</OtherId>
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
        <Homegroup>#{row['HomeGroup']}</Homegroup>
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
                #nodes.xpath('//*//child::*[not(node())]').each do |node|
                nodes.xpath('//*//child::*[not(node()) and not(text()[normalize-space()]) ]').each do |node|
                    node.remove
                end
                nodes.xpath('//*/child::*[text() and not(text()[normalize-space()])]').each do |node|
                    node.remove
		end
                outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", "TOPIC: naplan.sifxmlout\n" + nodes.root.to_s, "rcvd:#{ sprintf('%09d', m.offset)}" )
        # send results to indexer to create sms data graph
        producers.send_through_queue( outbound_messages )
	outbound_messages = []
            end

end
