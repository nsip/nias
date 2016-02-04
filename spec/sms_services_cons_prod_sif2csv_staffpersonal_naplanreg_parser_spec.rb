
require "net/http"
require "spec_helper"
require 'poseidon' 

out = <<CSV
fjghh371,Treva,Seefeldt,7E,"7D,7E",knptb460,046129,01,tseefeldt@example.com,,
CSV

xml = <<XML
<StaffPersonals xmlns="http://www.sifassociation.org/au/datamodel/3.4">
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
</StaffPersonals>
XML

@service_name = 'sms_services_cons_prod_sif2csv_staffpersonal_naplanreg_parser_spec'


describe "NAPLAN convert SIF to CSV" do

def post_xml(xml) 
	Net::HTTP.start("localhost", "9292") do |http|
		request = Net::HTTP::Post.new("/naplan/sifxml_staff")
		request.body = xml
		request["Content-Type"] = "application/xml"
		response = http.request(request)
	end
end
	before(:all) do
		puts @service_name 
		@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.csvstaff_out", 0, :latest_offset)
		puts "Next offset    = #{@xmlconsumer.next_offset}"
		post_xml(xml)
		sleep 10
	end

	context "Valid XML to naplan/sifxml_staff" do
		it "pushes templated CSV to naplan.csvstaff_out" do
                       begin
                                a = @xmlconsumer.fetch
                                expect(a).to_not be_nil
                                expect(a.empty?).to be false
                                expect(a[0].value).to eq out
                        rescue Poseidon::Errors::OffsetOutOfRange
                            puts "[warning] - bad offset supplied, resetting..."
                            offset = :latest_offset
                            retry
                        end
		end
	end
	
	after(:all) do
		sleep 10
	end

end
