
require "net/http"
require "spec_helper"
require 'poseidon' 

csv = <<CSV
Local School Student ID,Sector Student ID,Diocesan Student ID,Other Student ID,TAA Student ID,Jurisdiction Student ID,National Student ID,Platform Student ID,Previous Local School Student ID,Previous Sector Student ID,Previous Diocesan Student ID,Previous Other Student ID,Previous TAA Student ID,Previous Jurisdiction Student ID,Previous National Student ID,Previous Platform Student ID,Family Name,Given Name,Preferred Given Name,Middle Name,Date Of Birth,Sex,Student Country of Birth,Education Support,Full Fee Paying Student,Visa Code,Indigenous Status,LBOTE Status,Student Main Language Other than English Spoken at Home,Year Level,Test Level,FTE,Home Group,Class Code,ASL School ID,Local School ID,Local Campus ID,Main School Flag,Other School ID,Reporting School ID,Home Schooled Student,Sensitive,Offline Delivery,Parent 1 School Education,Parent 1 Non-School Education,Parent 1 Occupation,Parent 1 Main Language Other than English Spoken at Home,Parent 2 School Education,Parent 2 Non-School Education,Parent 2 Occupation,Parent 2 Main Language Other than English Spoken at Home,Address Line 1,Address Line 2,Locality,Postcode,State or Territory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2009-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,knptb460,046129,01,N,knptb460,knptb460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

out = <<XML
<StudentPersonal RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">
  <LocalId>fjghh371</LocalId>
  <StateProvinceId>59286</StateProvinceId>
  <OtherIdList>
    <OtherId Type="SectorStudentId">14668</OtherId>
    <OtherId Type="DiocesanStudentId">65616</OtherId>
    <OtherId Type="OtherStudentId">75189</OtherId>
    <OtherId Type="TAAStudentId">50668</OtherId>
    <OtherId Type="NationalStudentId">35164</OtherId>
    <OtherId Type="NAPPlatformStudentId">47618</OtherId>
    <OtherId Type="PreviousLocalSchoolStudentId">66065</OtherId>
    <OtherId Type="PreviousSectorStudentId">4716</OtherId>
    <OtherId Type="PreviousDiocesanStudentId">50001</OtherId>
    <OtherId Type="PreviousOtherStudentId">65241</OtherId>
    <OtherId Type="PreviousTAAStudentId">55578</OtherId>
    <OtherId Type="PreviousStateProvinceId">44128</OtherId>
    <OtherId Type="PreviousNationalStudentId">37734</OtherId>
    <OtherId Type="PreviousNAPPlatformStudentId">73143</OtherId>
  </OtherIdList>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>Seefeldt</FamilyName>
      <GivenName>Treva</GivenName>
      <MiddleName>E</MiddleName>
      <PreferredGivenName>Treva</PreferredGivenName>
    </Name>
    <Demographics>
      <IndigenousStatus>2</IndigenousStatus>
      <Sex>2</Sex>
      <BirthDate>2009-07-26</BirthDate>
      <CountryOfBirth>1101</CountryOfBirth>
      <LanguageList>
        <Language>
          <Code>2201</Code>
          <LanguageType>4</LanguageType>
        </Language>
      </LanguageList>
      <VisaSubClass>101</VisaSubClass>
      <LBOTE>Y</LBOTE>
    </Demographics>
    <AddressList>
      <Address Type="0123" Role="012A">
        <Street>
          <Line1>30769 PineTree Rd.</Line1>
        </Street>
        <City>Pepper Pike</City>
        <StateProvince>QLD</StateProvince>
        <Country>1101</Country>
        <PostalCode>9999</PostalCode>
      </Address>
    </AddressList>
  </PersonInfo>
  <MostRecent>
    <SchoolLocalId>046129</SchoolLocalId>
    <YearLevel>
      <Code>7</Code>
    </YearLevel>
    <FTE>0.89</FTE>
    <Parent1Language>1201</Parent1Language>
    <Parent2Language>1201</Parent2Language>
    <Parent1EmploymentType>2</Parent1EmploymentType>
    <Parent2EmploymentType>4</Parent2EmploymentType>
    <Parent1SchoolEducationLevel>3</Parent1SchoolEducationLevel>
    <Parent2SchoolEducationLevel>2</Parent2SchoolEducationLevel>
    <Parent1NonSchoolEducation>8</Parent1NonSchoolEducation>
    <Parent2NonSchoolEducation>7</Parent2NonSchoolEducation>
    <LocalCampusId>01</LocalCampusId>
    <SchoolACARAId>knptb460</SchoolACARAId>
    <TestLevel>
      <Code>7</Code>
    </TestLevel>
    <Homegroup>7E</Homegroup>
    <ClassCode>7D</ClassCode>
    <MembershipType>02</MembershipType>
    <FFPOS>1</FFPOS>
    <ReportingSchoolId>knptb460</ReportingSchoolId>
    <OtherEnrollmentSchoolACARAId>knptb460</OtherEnrollmentSchoolACARAId>
  </MostRecent>
  <EducationSupport>Y</EducationSupport>
  <HomeSchooledStudent>U</HomeSchooledStudent>
  <Sensitive>Y</Sensitive>
  <OfflineDelivery>Y</OfflineDelivery>
</StudentPersonal>
XML
out.gsub!(/\n[ ]+/,"").chomp!

@service_name = 'sms_services_cons_prod_csv2sif_studentpersonal_naplanreg_parser_spec'

describe "NAPLAN convert CSV to SIF" do

def post_csv(csv) 
	request = Net::HTTP::Post.new("/naplan/csv")
	request.body = csv
	request["Content-Type"] = "text/csv"
	@http.request(request)
end

	before(:all) do
		@http = Net::HTTP.new("localhost", "9292")
		@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.sifxmlout", 0, :latest_offset)
		puts "Next offset    = #{@xmlconsumer.next_offset}"
		sleep 1
		post_csv(csv)
	end

	context "Valid CSV to naplan.csv" do
		before(:example) do
		end
		it "pushes templated XML to naplan.sifxmlout" do
			sleep 1
                       begin
                                a = @xmlconsumer.fetch
                                expect(a).to_not be_nil
                                expect(a.empty?).to be false
				a[0].value.gsub!(%r{<StudentPersonal RefId="[^"]+">}, '<StudentPersonal RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">').gsub!(/\n[ ]+/,"")
                                expect(a[0].value).to eq out
                        rescue Poseidon::Errors::OffsetOutOfRange
                            puts "[warning] - bad offset supplied, resetting..."
                            offset = :latest_offset
                            retry
                        end
		end
		after(:example) do
		end
	end

	after(:all) do
		sleep 5
	end

end
