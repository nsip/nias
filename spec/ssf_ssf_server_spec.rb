#ENV['RACK_ENV'] = 'test'

require "spec_helper"
require_relative '../ssf/ssf_server.rb'
require 'poseidon_cluster' 


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

csv2 = <<CSV
test2,hello2
CSV

bad_csv = <<CSV
test,"hello
CSV

# need CSV to count columns
unbalanced_csv = <<CSV
label,value,value2
test,hello
CSV


csv_out = '{"label":"test","value":"hello","__linenumber":1,"__linecontent":"test,hello"}'
csv2_out = '{"label":"test2","value":"hello2","__linenumber":1,"__linecontent":"test2,hello2"}'

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
        before(:all) do
            #@xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.ingest", 0, :latest_offset)
            @xmlconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "sifxml.ingest", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
            @xmlconsumer.claimed.each { |x| @xmlconsumer.checkout { |y| puts y.next_offset }}
            @genericconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "rspec.test", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
            @genericconsumer.claimed.each { |x| @genericconsumer.checkout { |y| puts y.next_offset }}
            @csvconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "csv.errors", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
            @csvconsumer.claimed.each { |x| @csvconsumer.checkout { |y| puts y.next_offset }}
        end
        context "SIF/XML non-sif small" do
	    before(:example) do
                post "/rspec/test", xmlheader + xml + xmlfooter, "CONTENT_TYPE" => "application/xml"
                sleep 1
	    end
            it "posts SIF/XML to Kafka topic SSFServer.settings.xmltopic with header rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@xmlconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq "TOPIC: rspec.test\n#{xmlheader + xml + xmlfooter}" 
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "JSON small" do
        	before(:example) do
               		post "/rspec/test", json, "CONTENT_TYPE" => "application/json"
                	sleep 1
        	end
            it "posts parsed JSON to Kafka topic rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@genericconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq json_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "CSV small" do
        	before(:example) do
               		post "/rspec/test", csvheader + csv, "CONTENT_TYPE" => "text/csv"
                	sleep 1
        	end
            it "posts CSV converted to JSON to Kafka topic rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@genericconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq csv_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "CSV small malformed" do
        	before(:example) do
               		post "/rspec/test", csvheader + bad_csv, "CONTENT_TYPE" => "text/csv"
                	sleep 1
        	end
            it "posts error to Kafka topic csv.error" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@csvconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "CSV small inconsistent number of columns" do
        	before(:example) do
               		post "/rspec/test", csvheader + unbalanced_csv, "CONTENT_TYPE" => "text/csv"
                	sleep 1
        	end
            it "posts error to Kafka topic csv.error" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@csvconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "payload more than 1 MB" do
	before(:example) do
                post "/rspec/test", xml * 100000, "CONTENT_TYPE" => "application/xml"
	end
            it "rejects payload" do
                expect(last_response.status).to eq 400
            end
        end
        context "message other than CSV, JSON, XML" do
	before(:example) do
                post "/rspec/test", csv, "CONTENT_TYPE" => "text/plain"
	end
            it "rejects payload" do
                expect(last_response.status).to eq 415
            end
        end
	after(:all) do
	   @xmlconsumer.close
	   @genericconsumer.close
	   @csvconsumer.close
	   sleep 5
	end
    end

describe "POST rspec/test/bulk" do
	before(:all) do
            @xmlbulkconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "sifxml.bulkingest", trail: true, socket_timeout_ms: 6000, max_wait_ms: 100, max_bytes: 10000000)
            @xmlbulkconsumer.claimed.each { |x| @xmlbulkconsumer.checkout { |y| puts y.next_offset }}
            @genericconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "rspec.test", trail: true, socket_timeout_ms:6000, max_wait_ms:100, max_bytes: 10000000)
            @genericconsumer.claimed.each { |x| @genericconsumer.checkout { |y| puts y.next_offset }}
            @csvconsumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "csv.errors", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
            @csvconsumer.claimed.each { |x| @csvconsumer.checkout { |y| puts y.next_offset }}
            @inputxml = xmlheader + xml * 5000 + xmlfooter
	end
        context "SIF/XML non-sif 3 MB" do
        	before(:example) do
               		post "/rspec/test/bulk", @inputxml, "CONTENT_TYPE" => "application/xml"
                	sleep 1
        	end
            it "posts SIF/XML to Kafka topic SSFServer.settings.bulkxmltopic in two 1 MB-sized pieces, with header rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    #a = @xmlbulkconsumer.fetch(:max_bytes => 10000000)
                    a = groupfetch(@xmlbulkconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    payload = ""
                    a.each do |m|
                        payload << m.value
                    end
                    payload = payload.gsub(/\n===snip [0-9]*===\n/, "")
                    expect(payload).to eq "TOPIC: rspec.test\n#{@inputxml}"
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
        context "SIF/XML non-sif small" do
        	before(:example) do
               		post "/rspec/test/bulk", xmlheader + xml + xmlfooter , "CONTENT_TYPE" => "application/xml"
                	sleep 1
        	end
            it "posts SIF/XML to Kafka topic SSFServer.settings.xmltopic with header rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@xmlbulkconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq "TOPIC: rspec.test\n#{xmlheader + xml + xmlfooter}"
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "JSON small" do
        	before(:example) do
               		post "/rspec/test/bulk", json, "CONTENT_TYPE" => "application/json"
                	sleep 1
        	end
            it "posts parsed JSON to Kafka topic rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@genericconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq json_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "CSV 3 MB" do
        	before(:example) do
               		post "/rspec/test/bulk", csvheader + csv * 250000, "CONTENT_TYPE" => "text/csv"
                	sleep 1
        	end
            it "posts JSON to Kafka topic rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@genericconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
		    received = a[0].value
		    received.gsub!(/"__linenumber":[0-9]+,/,'"__linenumber":1,')
                    expect(received).to eq csv_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "CSV small" do
        	before(:example) do
                    	(1..5).each {|x| groupfetch(@genericconsumer)}  # flush any pending errors
               		post "/rspec/test/bulk", csvheader + csv2, "CONTENT_TYPE" => "text/csv"
                	sleep 1
        	end
            it "posts JSON to Kafka topic rspec.test" do
                expect(last_response.status).to eq 202
                begin
                    a = groupfetch(@genericconsumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
                    expect(a[0].value).to eq csv2_out
                    expect(a[0].key).to eq "test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "message other than CSV, JSON, XML" do
	    before(:example) do
                post "/rspec/test/bulk", csvheader + csv, "CONTENT_TYPE" => "text/plain"
	    end
            it "rejects payload" do
                expect(last_response.status).to eq 415
            end
        end
	after(:all) do
	   @xmlbulkconsumer.close
	   @genericconsumer.close
	   @csvconsumer.close
	   sleep 5
	end
	end
end
