
require "net/http"
require "spec_helper"
require 'poseidon' 


xml = <<XML
<TeachingGroups xmlns="http://www.sifassociation.org/au/datamodel/3.4">
    <TeachingGroup RefId="f94278d5-b1b4-4936-ae95-f6d78d0887e2">
        <SchoolYear>0001</SchoolYear>
        <LocalId>982b604a-00ac-487c-ae5a-bd62aae4b3a6</LocalId>
        <ShortName>1A</ShortName>
        <LongName>1A</LongName>
        <SchoolInfoRefId>1e5aa150-ca94-45a0-a893-9ec427ee2160</SchoolInfoRefId>
        <SchoolLocalId>72868</SchoolLocalId>
        <StudentList>
            <TeachingGroupStudent>
                <StudentPersonalRefId>9570f36c-9c4d-4a0a-912d-25f26d5264b4</StudentPersonalRefId>
                <StudentLocalId>53856</StudentLocalId>
                <Name Type="LGL">
                    <FamilyName>Huett</FamilyName>
                    <GivenName>Charmaine</GivenName>
                    <MiddleName>Estrella</MiddleName>
                    <PreferredGivenName>Charmaine</PreferredGivenName>
                </Name>
            </TeachingGroupStudent>
        </StudentList>
        <TeacherList>
            <TeachingGroupTeacher>
                <StaffPersonalRefId>cef8aa68-554e-4771-82f5-d54568a7e909</StaffPersonalRefId>
                <StaffLocalId>14541</StaffLocalId>
                <Name Type="LGL">
                    <Title>Miss</Title>
                    <FamilyName>Zeimetz</FamilyName>
                    <GivenName>Vada</GivenName>
                    <MiddleName>Terri</MiddleName>
                    <PreferredFamilyName>Zeimetz</PreferredFamilyName>
                    <PreferredGivenName>Vada</PreferredGivenName>
                </Name>
                <Association/>
            </TeachingGroupTeacher>
        </TeacherList>
    </TeachingGroup>
</TeachingGroups>
XML

out = "{\"type\":\"TeachingGroup\",\"id\":\"f94278d5-b1b4-4936-ae95-f6d78d0887e2\",\"otherids\":{\"localid\":\"982b604a-00ac-487c-ae5a-bd62aae4b3a6\"},\"links\":[\"f94278d5-b1b4-4936-ae95-f6d78d0887e2\",\"1e5aa150-ca94-45a0-a893-9ec427ee2160\",\"9570f36c-9c4d-4a0a-912d-25f26d5264b4\",\"cef8aa68-554e-4771-82f5-d54568a7e909\"],\"equivalentids\":[],\"label\":\"1A\"}"

@service_name = 'sms_services_cons_prod_sif_parser_spec'

def post_xml(xml) 
	request = Net::HTTP::Post.new("/rspec/test")
	request.body = xml
	request["Content-Type"] = "application/xml"
	@http.request(request)
end


describe "SIF Ingest/Produce" do

	before(:all) do
		@http = Net::HTTP.new("localhost", "9292")
		@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sms.indexer", 0, :latest_offset)
		puts "Next offset    = #{@xmlconsumer.next_offset}"
		sleep 1
		post_xml(xml)
	end

	context "Valid XML" do
		before(:example) do
		end
		it "pushes validated XML to sifxml.validated" do
			sleep 1
                       begin
                                a = @xmlconsumer.fetch
                                expect(a).to_not be_nil
                                expect(a.empty?).to be false
                                expect(a[0].value).to eq out.to_s
                        rescue Poseidon::Errors::OffsetOutOfRange
                            puts "[warning] - bad offset supplied, resetting..."
                            offset = :latest_offset
                            retry
                        end
		end
		after(:example) do
			#@xmlconsumer.close
		end
	end

end
