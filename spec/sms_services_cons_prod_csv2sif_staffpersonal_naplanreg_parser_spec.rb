
require "net/http"
require "spec_helper"
require 'poseidon' 

csv = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

# AdditionalInfo = No way!
invalid_csv_additionalinfo = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,No way!,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV

# email missing @ 
invalid_csv_email = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldtexample.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,Y,teacher
CSV


out = <<XML
<StaffPersonal RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">
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
        	@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.sifxmlout_staff", 0, :latest_offset)
        	puts "Next offset    = #{@xmlconsumer.next_offset}"
		sleep 1
        	post_csv(csv)
	end
        it "pushes templated XML to naplan.sifxmlout_staff" do
            sleep 1
            begin
                a = @xmlconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                a[0].value.gsub!(%r{<StaffPersonal RefId="[^"]+">}, '<StaffPersonal RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">').gsub!(/\n[ ]*/,"")
                expect(a[0].value).to eq out
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    context "Invalid CSV to naplan.csv, malformed email address" do
	before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)               
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_email)
	end
        it "pushes errors to csv.errors" do
            sleep 1
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				email_errors = a.find_all{ |e| e.value["Type format, Content tseefeldtexample.com"] }
				expect(email_errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    context "Invalid CSV to naplan.csv, malformed additionalinfo boolean" do
	before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)               
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_additionalinfo)
	end
        it "pushes errors to csv.errors" do
            sleep 1
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
		boolean_errors = a.find_all{ |e| e.value["Type invalid_boolean, Content No way!"] }
		expect(boolean_errors.empty?).to be false
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
