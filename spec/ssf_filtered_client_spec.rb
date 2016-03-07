ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "net/http"
require "spec_helper"
require_relative '../ssf/filtered_client.rb'
require_relative '../niasconfig'


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

@service_name = 'spec-ssf-filtered-client'

$config = NiasConfig.new


describe "FilteredClient" do

    describe "GET /filtered/rspec/test/low" do
        before(:example) do
            Net::HTTP.start("#{$config.get_host}", "#{$config.get_sinatra_port}") do |http|
                request = Net::HTTP::Post.new("/rspec/test")
                request.body = xml
                request["Content-Type"] = "application/xml"
                http.request(request)
            end
            sleep 3
        end
        it "returns filtered XML" do
            get "/filtered/rspec/test/low"
            expect(last_response).to be_ok
            expect(last_response.body).to match(/<div class='record'>/)
        end
    end
end
