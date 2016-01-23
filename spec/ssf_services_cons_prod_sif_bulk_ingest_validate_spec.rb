
require "net/http"
require "spec_helper"
require 'poseidon' 


xml = <<XML
<Invoice RefId="82c725ac-1ea4-4395-a9be-c35e5657d1cc">
  <InvoicedEntity SIF_RefObject="Debtor">e1759047-09c4-4ba0-a69e-eec215de3d80</InvoicedEntity>
  <BillingDate>2014-02-01</BillingDate>
  <TransactionDescription>Activity Fees</TransactionDescription>
  <BilledAmount Currency="AUD" Type="Debit">24.93</BilledAmount>
  <Ledger>Family</Ledger>
  <TaxRate>10.0</TaxRate>
  <TaxAmount Currency="AUD">2.49</TaxAmount>
</Invoice>
XML

header = '<Invoices xmlns="http://www.sifassociation.org/au/datamodel/3.4">'
footer = '</Invoices>'

xml_malformed = xml.gsub(%r{</BillingDate>}, "")
xml_invalid = xml.gsub(%r{Ledger}, "Leleledger")

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
