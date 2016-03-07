
require "net/http"
require "spec_helper"
require 'poseidon_cluster' 
require_relative '../niasconfig'

out = <<CSV
fjghh371,Treva,Seefeldt,7D,7E,knptb460,046129,01,tseefeldt@example.com,N,principal
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
  <Title>principal</Title>
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
  <SIF_ExtendedElements>
    <SIF_ExtendedElement Name="AdditionalInfo">N</SIF_ExtendedElement>
  </SIF_ExtendedElements>
</StaffPersonal>
</StaffPersonals>
XML


@service_name = 'sms_services_cons_prod_sif2csv_staffpersonal_naplanreg_parser_spec'



describe "NAPLAN convert SIF to CSV" do

    def post_xml(xml) 
        Net::HTTP.start("#{$config.get_host}", "#{$config.get_sinatra_port}") do |http|
            request = Net::HTTP::Post.new("/naplan/sifxml_staff")
            request.body = xml
            request["Content-Type"] = "application/xml"
            response = http.request(request)
        end
    end

    before(:all) do
	$config = NiasConfig.new
        @xmlconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["#{$config.kafka}"], ["#{$config.zookeeper}"], "naplan.csvstaff_out", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
        @xmlconsumer.claimed.each { |x| @xmlconsumer.checkout { |y| puts y.next_offset }}
        post_xml(xml)
        sleep 3
    end

    context "Valid XML to naplan/sifxml_staff" do
        it "pushes templated CSV to naplan.csvstaff_out" do
            begin
                a = groupfetch(@xmlconsumer)
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                expect(a[0]).to_not be_nil
                expect(a[0].value.nil?).to be false
                expect(a[0].value).to eq out
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end
    after(:all) do
        @xmlconsumer.close
        sleep 5
    end

end
