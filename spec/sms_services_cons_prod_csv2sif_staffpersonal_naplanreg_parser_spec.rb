# also tests SRM validation

require "net/http"
require "spec_helper"
require 'poseidon' 

csv = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

invalid_email = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldtexample.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

invalid_additionalinfo = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldtexample.com,No way!,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
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

@service_name = 'sms_services_cons_prod_csv2sif_staffpersonal_naplanreg_parser_spec'

describe "NAPLAN convert CSV to SIF" do

    def post_csv(csv) 
        request = Net::HTTP::Post.new("/naplan/csv_staff")
        request.body = csv
        request["Content-Type"] = "text/csv"
        @http.request(request)
    end

    before(:all) do
        @http = Net::HTTP.new("localhost", "9292")
        sleep 1
    end

    context "Valid CSV to naplan.csv" do
	before(:example) do
        	@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.sifxmlout_staff.none", 0, :latest_offset)
        	puts "Next offset    = #{@xmlconsumer.next_offset}"
        	post_csv(csv)
		sleep 5
	end
        it "pushes templated XML to naplan.sifxmlout_staff.none" do
            begin
                a = @xmlconsumer.fetch
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

    context "Invalid email in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(invalid_email)
                sleep 5
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["is malformed"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
   end

   context "Invalid additional info in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(invalid_additionalinfo)
                sleep 5
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["is not Y or N"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end


    after(:all) do
        sleep 5
    end

end
