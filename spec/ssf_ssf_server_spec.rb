#ENV['RACK_ENV'] = 'test'

require "spec_helper"
require_relative '../ssf/ssf_server.rb'
require 'poseidon' 


xml = <<XML
<Invoice RefId="82c725ac-1ea4-4395-a9be-c35e5657d1cc">
  <InvoicedEntity SIF_RefObject="Debtor">e1759047-09c4-4ba0-a69e-eec215de3d80</InvoicedEntity>
  <BillingDate>2014-02-01</BillingDate>
  <TransactionDescription>Activity Fees</TransactionDescription>
  <BilledAmount Type="Debit" Currency="AUD">24.93</BilledAmount>
  <Ledger>Family</Ledger>
  <TaxRate>10.0</TaxRate>
  <TaxAmount Currency="AUD">2.49</TaxAmount>
</Invoice>
XML

xmlheader = <<XML
<Invoices xmlns="http://www.sifassociation.org/au/datamodel/3.4">
XML


xmlfooter = <<XML
</Invoices>
XML



json = <<JSON
{
	"test" : "hello"
}
JSON


json_out = '["test","hello"]'

csvheader = "label,value\n"

csv = <<CSV
test,hello
CSV

bad_csv = <<CSV
test,"hello
CSV

# need CSV to count columns
unbalanced_csv = <<CSV
label,value,value2
test,hello
CSV


csv_out = '{"label":"test","value":"hello","__linenumber":1}'

@service_name = 'spec-ssf-ssf-server'

describe "SSFServer" do

    describe "GET ssf" do
        it "Gets a topic list" do
            get "/ssf"
            expect(last_response).to be_ok
        end
    end


    describe "POST /" do
        it "rejects request as not specifying a topic" do
            post "/", xmlheader + xml + xmlfooter, "CONTENT_TYPE" => "application/xml"
            expect(last_response.status).to eq 404
        end
    end

    describe "POST rspec" do
        it "rejects request as not specifying a stream" do
            post "/rspec", xmlheader + xml + xmlfooter, "CONTENT_TYPE" => "application/xml"
            expect(last_response.status).to eq 404
        end
    end


    describe "POST rspec/test" do
        before(:example) do
            @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.ingest", 0, :latest_offset)
        end
        context "SIF/XML non-sif small" do
            it "posts SIF/XML to Kafka topic SSFServer.settings.xmltopic with header rspec.test" do
                puts "Next offset    = #{@xmlconsumer.next_offset}"
                post "/rspec/test", xmlheader + xml + xmlfooter, "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @xmlconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq "TOPIC: rspec.test\n#{xmlheader + xml + xmlfooter}" 
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
                before(:example) do
            @genericconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "rspec.test", 0, :latest_offset)
        end
        context "JSON small" do
            it "posts parsed JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test", json, "CONTENT_TYPE" => "application/json"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @genericconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq json_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
                before(:example) do
            @genericconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "rspec.test", 0, :latest_offset)
        end
        context "CSV small" do
            it "posts CSV converted to JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test", csvheader + csv, "CONTENT_TYPE" => "text/csv"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @genericconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq csv_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
                before(:example) do
            @csvconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        end
        context "CSV small malformed" do
            it "posts error to Kafka topic csv.error" do
                puts "Next offset    = #{@csvconsumer.next_offset}"
                post "/rspec/test", csvheader + bad_csv, "CONTENT_TYPE" => "text/csv"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @csvconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
                before(:example) do
            @csvconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        end
        context "CSV small inconsistent number of columns" do
            it "posts error to Kafka topic csv.error" do
                puts "Next offset    = #{@csvconsumer.next_offset}"
                post "/rspec/test", csvheader + unbalanced_csv, "CONTENT_TYPE" => "text/csv"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @csvconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
                context "payload more than 1 MB" do
            it "rejects payload" do
                post "/rspec/test", xml * 100000, "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 400
            end
        end
        context "message other than CSV, JSON, XML" do
            it "rejects payload" do
                post "/rspec/test", csv, "CONTENT_TYPE" => "text/plain"
                expect(last_response.status).to eq 415
            end
        end
    end

describe "POST rspec/test/bulk" do
        before(:example) do
            @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.bulkingest", 0, :latest_offset)
        end
        context "SIF/XML non-sif 3 MB" do
            it "posts SIF/XML to Kafka topic SSFServer.settings.bulkxmltopic in two 1 MB-sized pieces, with header rspec.test" do
                puts "Next offset    = #{@xmlconsumer.next_offset}"
                inputxml = xmlheader + xml * 5000 + xmlfooter
                post "/rspec/test/bulk", inputxml, "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @xmlconsumer.fetch(:max_bytes => 10000000)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    payload = ""
                    a.each do |m|
                        payload << m.value
                    end
                    payload = payload.gsub(/\n===snip [0-9]*===\n/, "")
                    expect(payload).to eq "TOPIC: rspec.test\n#{inputxml}"
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        before(:example) do
            @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.bulkingest", 0, :latest_offset)
        end
        context "SIF/XML non-sif small" do
            it "posts SIF/XML to Kafka topic SSFServer.settings.xmltopic with header rspec.test" do
                puts "Next offset    = #{@xmlconsumer.next_offset}"
                post "/rspec/test/bulk", xmlheader + xml + xmlfooter , "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @xmlconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq "TOPIC: rspec.test\n#{xmlheader + xml + xmlfooter}"
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        before(:example) do
            @genericconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "rspec.test", 0, :latest_offset)
        end
        context "JSON small" do
            it "posts parsed JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test/bulk", json, "CONTENT_TYPE" => "application/json"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @genericconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq json_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        before(:example) do
            @genericconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "rspec.test", 0, :latest_offset)
        end
        context "CSV 3 MB" do
            it "posts JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test/bulk", csvheader + csv * 250000, "CONTENT_TYPE" => "text/csv"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @genericconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq csv_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        before(:example) do
            @genericconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "rspec.test", 0, :latest_offset)
        end
        context "CSV small" do
            it "posts JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test/bulk", csvheader + csv, "CONTENT_TYPE" => "text/csv"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @genericconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq csv_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "message other than CSV, JSON, XML" do
            it "rejects payload" do
                post "/rspec/test/bulk", csvheader + csv, "CONTENT_TYPE" => "text/plain"
                expect(last_response.status).to eq 415
            end
        end
    end
end
