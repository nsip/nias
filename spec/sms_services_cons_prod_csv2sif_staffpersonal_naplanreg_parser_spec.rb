
require "net/http"
require "spec_helper"
require 'poseidon' 

csv = <<CSV
LocalId,GivenName,FamilyName,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,EmailAddress,ReceiveAdditionalInformation,StaffSchoolRole
fjghh371,Treva,Seefeldt,7E,"7D,7E",knptb460,046129,01,tseefeldt@example.com,Y,teacher
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
  <MostRecent>
    <SchoolLocalId>046129</SchoolLocalId>
    <SchoolACARAId>knptb460</SchoolACARAId>
    <LocalCampusId>01</LocalCampusId>
    <NAPLANClassList>
      <ClassCode>7D</ClassCode>
      <ClassCode>7E</ClassCode>
    </NAPLANClassList>
    <HomeGroup>7E</HomeGroup>
  </MostRecent>
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
        @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.sifxmlout_staff", 0, :latest_offset)
        puts "Next offset    = #{@xmlconsumer.next_offset}"
        sleep 1
        post_csv(csv)
    end

    context "Valid CSV to naplan.csv" do
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

    after(:all) do
        sleep 5
    end

end
