
require "net/http"
require "spec_helper"
require 'poseidon' 


xml = <<XML
<StudentPersonal RefId="e9fc2b1f-07a5-4c07-ad9a-6ffa3d9b576d">
    <LocalId>45715</LocalId>
    <StateProvinceId>96413752</StateProvinceId>
    <OtherIdList/>
    <PersonInfo>
        <Name Type="LGL">
            <FamilyName>Dodich</FamilyName>
            <GivenName>Kira</GivenName>
            <MiddleName>Glynis</MiddleName>
            <PreferredGivenName>Kira</PreferredGivenName>
        </Name>
        <Demographics>
            <IndigenousStatus>2</IndigenousStatus>
            <Sex>2</Sex>
            <BirthDate>2004-02-10</BirthDate>
            <CountryOfBirth>1101</CountryOfBirth>
        </Demographics>
        <AddressList>
            <Address Type="0123" Role="012A">
                <Street>
                    <Line1>Line1</Line1>
                </Street>
                <City>City1</City>
                <StateProvince>StatePrivince</StateProvince>
                <PostalCode>PostalCode</PostalCode>
            </Address>
        </AddressList>
        <EmailList>
            <Email Type="06">Dodich.Kira.G@vic.edu.au</Email>
        </EmailList>
    </PersonInfo>
    <MostRecent>
        <YearLevel>
            <Code>6</Code>
        </YearLevel>
        <Parent1Language>1201</Parent1Language>
        <Parent2Language>1201</Parent2Language>
        <Parent1EmploymentType>8</Parent1EmploymentType>
        <Parent2EmploymentType>3</Parent2EmploymentType>
        <Parent1SchoolEducationLevel>2</Parent1SchoolEducationLevel>
        <Parent2SchoolEducationLevel>0</Parent2SchoolEducationLevel>
        <Parent1NonSchoolEducation>5</Parent1NonSchoolEducation>
        <Parent2NonSchoolEducation>6</Parent2NonSchoolEducation>
    </MostRecent>
</StudentPersonal>
XML


header = '<StudentPersonals xmlns="http://www.sifassociation.org/au/datamodel/3.4">'
footer = '</StudentPersonals>'

xml_malformed = xml.gsub(%r{</PersonInfo>}, "")
xml_invalid = xml.gsub(%r{GivenName}, "FirstName")

@service_name = 'ssf_services_cons_prod_sif_bulk_ingest_validate_spec'

recordcount = 30000
#recordcount = 10

xmlbody = xml * recordcount


describe "Bulk SIF Ingest/Produce" do

    def post_xml(xml) 
        Net::HTTP.start("localhost", "9292") do |http|
            request = Net::HTTP::Post.new("/rspec/test/bulk")
            request.body = xml
            request["Content-Type"] = "application/xml"
            http.request(request)
        end
    end
    before(:all) do
        @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.errors", 0, :latest_offset)
        @xmlvalidconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.validated", 0, :latest_offset)
    end
    context "Malformed XML" do
        it "pushes error to sifxml.errors" do
            puts "Next offset    = #{@xmlconsumer.next_offset}"
            post_xml(header + xml_malformed + xmlbody + footer)
            sleep 20
            begin
                a = @xmlconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                expect(a[0].value).to match(/well-formedness error/)
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    context "Invalid XML" do
        it "pushes error to sifxml.errors" do
            puts "Next offset    = #{@xmlconsumer.next_offset}"
            post_xml(header + xml_invalid + xmlbody + footer)
            sleep 20
            begin
                a = @xmlconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                expect(a[0].value).to match(/validity error/)
                expect(a[0].value).to match(/rspec\.test/)
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

    context "Valid XML" do
        it "pushes validated XML to sifxml.validated" do
            puts "Next offset    = #{@xmlvalidconsumer.next_offset}"
            post_xml(header + xmlbody + footer)
            sleep 20
            begin
                a = @xmlvalidconsumer.fetch(:max_bytes => 10000000)
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                expected = "TOPIC: rspec.test\n" + xml
                expected.gsub!(/ xmlns="[^"]+"/, "")
                expected.gsub!(/\n[ ]*/, "")
                a[0].value.gsub!(/ xmlns="[^"]+"/, "").gsub!(/\n[ ]*/, "")
                expect(a[0].value.chomp).to eq expected.chomp
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

end
