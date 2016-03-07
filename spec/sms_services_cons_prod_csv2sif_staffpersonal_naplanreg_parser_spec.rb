# also tests SRM validation

require "net/http"
require "spec_helper"
require 'poseidon_cluster' 
require_relative '../niasconfig'

csv = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

blank_param = <<CSV
GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

invalid_email = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldtexample.com,Y,teacher
CSV

invalid_additionalinfo = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,No way!,teacher
CSV

wrong_record = <<CSV 
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,knptb460,046129,01,02,knptb460,knptb460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

default_values = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress
fjghh372,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com
CSV



out = <<XML
<StaffPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">
  <LocalId>fjghh371</LocalId>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>Seefeldt</FamilyName>
      <GivenName>Treva</GivenName>
    </Name>
    <EmailList>
      <Email Type="01">tseefeldt@example.com</Email>
    </EmailList>
  </PersonInfo>
  <Title>teacher</Title>
  <MostRecent>
    <SchoolLocalId>046129</SchoolLocalId>
    <SchoolACARAId>knptb460</SchoolACARAId>
    <LocalCampusId>01</LocalCampusId>
    <NAPLANClassList>
      <ClassCode>7D</ClassCode>
    </NAPLANClassList>
    <HomeGroup>7E</HomeGroup>
  </MostRecent>
  <SIF_ExtendedElements>
    <SIF_ExtendedElement Name="AdditionalInfo">Y</SIF_ExtendedElement>
  </SIF_ExtendedElements>
</StaffPersonal>
XML
out.gsub!(/\n[ ]*/,"").chomp!
default_xml = String.new(out)
default_xml.gsub!('teacher', 'principal')
default_xml.gsub!('"AdditionalInfo">Y<', '"AdditionalInfo">N<')
default_xml.gsub!('fjghh371', 'fjghh372')

describe "NAPLAN convert CSV to SIF" do

    def post_csv(csv) 
        request = Net::HTTP::Post.new("/naplan/csv_staff")
        request.body = csv
        request["Content-Type"] = "text/csv"
        @http.request(request)
    end

    before(:all) do
	config = NiasConfig.new
	@service_name = 'sms_services_cons_prod_csv2sif_staffpersonal_naplanreg_parser_spec'
        @http = Net::HTTP.new("#{config.get_host}", "#{config.get_sinatra_port}") 
	@xmlconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["#{config.kafka}"], ["#{config.zookeeper}"], "naplan.sifxmlout_staff.none", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
	@xmlconsumer.claimed.each { |x| @xmlconsumer.checkout { |y| puts y.next_offset }}
	@errorconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_err#{rand(1000)}", ["#{config.kafka}"], ["#{config.zookeeper}"], "csv.errors", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
	@errorconsumer.claimed.each { |x| @errorconsumer.checkout { |y| puts y.next_offset }}
	@siferrorconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_err#{rand(1000)}", ["#{config.kafka}"], ["#{config.zookeeper}"], "sifxml.errors", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
	@siferrorconsumer.claimed.each { |x| @siferrorconsumer.checkout { |y| puts y.next_offset }}
	@srmerrorconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_err#{rand(1000)}", ["#{config.kafka}"], ["#{config.zookeeper}"], "naplan.srm_errors", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
	@srmerrorconsumer.claimed.each { |x| @srmerrorconsumer.checkout { |y| puts y.next_offset }}
        sleep 1
    end

    context "Valid CSV to naplan.csv_staff" do
	before(:example) do
        	post_csv(csv)
		sleep 1
	end
        it "pushes templated XML to naplan.sifxmlout_staff.none" do
            begin
		a = groupfetch(@xmlconsumer)	
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                expect(a[0].nil?).to be false
                expect(a[0].value.nil?).to be false
		received = a[0].value
                received.gsub!(%r{<StaffPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="[^"]+">}, '<StaffPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">')
		received.gsub!(%r{<\?xml version="1.0"\?>},'')
		received.gsub!(%r{<!-- CSV [^>]+>},'')
		received.gsub!(/\n[ ]*/,"")
                expect(received).to eq out
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
			
        end
    end

    context "Missing default values in CSV to naplan.csv_staff" do
        before(:example) do
                post_csv(default_values)
                sleep 1
        end
        it "pushes templated XML with supplied default values to naplan.sifxmlout_staff.none" do
            begin
		a = groupfetch(@xmlconsumer)	
                expect(a.empty?).to be false
                expect(a[0].nil?).to be false
                expect(a[0].value.nil?).to be false
                received = a[0].value
                received.gsub!(%r{<StaffPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="[^"]+">}, '<StaffPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">')
                received.gsub!(%r{<\?xml version="1.0"\?>},'')
                received.gsub!(%r{<!-- CSV [^>]+>},'')
                received.gsub!(/\n[ ]*/,"")
                expect(received).to eq default_xml
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    context "Invalid email in CSV to naplan.csv_staff" do
        before(:example) do
                post_csv(invalid_email)
                sleep 3
        end
        it "pushes error to naplan.srm_errors" do
            begin
		a = groupfetch(@srmerrorconsumer)	
                expect(a.empty?).to be false
                expect(a[0]).to_not be_nil
                expect(a[0].value.nil?).to be false
                                errors = a.find_all{ |e| e.value["is malformed"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
   end

   context "Invalid additional info in CSV to naplan.csv_staff" do
        before(:example) do
                post_csv(invalid_additionalinfo)
                sleep 3
        end
        it "pushes error to naplan.srm_errors" do
            begin
		a = groupfetch(@srmerrorconsumer)	
                expect(a.empty?).to be false
                expect(a[0]).to_not be_nil
                expect(a[0].value.nil?).to be false
                                errors = a.find_all{ |e| e.value["is not Y or N"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

   context "Blank mandatory parameter in CSV to naplan.csv_staff" do
        before(:example) do
                post_csv(blank_param)
                sleep 3
        end
        it "pushes error to csv.errors" do
            begin
		a = groupfetch(@errorconsumer)	
                expect(a.empty?).to be false
                expect(a[0]).to_not be_nil
                expect(a[0].value.nil?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

   context "Student record in CSV to naplan.csv_staff" do
        before(:example) do
                post_csv(wrong_record)
                sleep 3
        end
        it "pushes error to csv.errors" do
            begin
		a = groupfetch(@errorconsumer)	
                expect(a.empty?).to be false
                expect(a[0]).to_not be_nil
                expect(a[0].value.nil?).to be false
                                errors = a.find_all{ |e| e.value["You appear to have submitted a"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    after(:all) do
	@xmlconsumer.close
	@errorconsumer.close
	@srmerrorconsumer.close
	@siferrorconsumer.close
	sleep 5
    end

end
