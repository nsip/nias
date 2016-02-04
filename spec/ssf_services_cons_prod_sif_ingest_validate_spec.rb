
require "net/http"
require "spec_helper"
require 'poseidon' 


xml = <<XML
<Invoices xmlns="http://www.sifassociation.org/au/datamodel/3.4">
<Invoice RefId="82c725ac-1ea4-4395-a9be-c35e5657d1cc">
  <InvoicedEntity SIF_RefObject="Debtor">e1759047-09c4-4ba0-a69e-eec215de3d80</InvoicedEntity>
  <BillingDate>2014-02-01</BillingDate>
  <TransactionDescription>Activity Fees</TransactionDescription>
  <BilledAmount Type="Debit" Currency="AUD">24.93</BilledAmount>
  <Ledger>Family</Ledger>
  <TaxRate>10.0</TaxRate>
  <TaxAmount Currency="AUD">2.49</TaxAmount>
</Invoice>
</Invoices>
XML

xml_malformed = xml.gsub(%r{</BillingDate>}, "")
xml_invalid = xml.gsub(%r{Ledger}, "Leleledger")

@service_name = 'ssf_services_cons_prod_sif_ingest_validate_spec'
puts @service_name


describe "SIF Ingest/Produce" do

def post_xml(xml) 
	Net::HTTP.start("localhost", "9292") do |http|
		request = Net::HTTP::Post.new("/rspec/test")
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
			post_xml(xml_malformed)
			sleep 1
                       begin
                                a = @xmlconsumer.fetch
                                expect(a).to_not be_nil
                                expect(a.empty?).to be false
                                expect(a[0].value).to match(/well-formedness error/)
                                expect(a[0].value).to match(/rspec\.test/)
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

	context "Invalid XML" do
		before(:example) do
			#@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.errors", 0, :latest_offset)
		end
		it "pushes error to sifxml.errors" do
			puts "Next offset    = #{@xmlconsumer.next_offset}"
			post_xml(xml_invalid)
			sleep 10
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
		after(:example) do
			#@xmlconsumer.close
		end
	end

	context "Valid XML" do
		before(:example) do
			#@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.errors", 0, :latest_offset)
		end
		it "pushes validated XML to sifxml.validated" do
			puts "Next offset    = #{@xmlvalidconsumer.next_offset}"
			post_xml(xml)
			sleep 10
                       begin
                                a = @xmlvalidconsumer.fetch
                                expect(a).to_not be_nil
                                expect(a.empty?).to be false
				expected = "TOPIC: rspec.test\n" + xml.lines[1..-2].join
				expected.gsub!(/ xmlns="[^"]+"/, "")
				a[0].value.gsub!(/ xmlns="[^"]+"/, "")
                                expect(a[0].value.chomp).to eq expected.chomp
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
