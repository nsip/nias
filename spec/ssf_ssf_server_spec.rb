#ENV['RACK_ENV'] = 'test'

#require "rspec"
#require "rack/test"
require "spec_helper"
require_relative '../ssf/ssf_server.rb'
require 'poseidon' 


xml = <<XML
<xmldocument>
  <test>hello</test>
</xmldocument>
XML

json = <<JSON
{
	"test" : "hello"
}
JSON

json_out = '["test","hello"]'

csv = <<CSV
label,value
test,hello
CSV

csv_out = '{"label":"test","value":"hello"}'

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
            post "/", xml, "CONTENT_TYPE" => "application/xml"
            expect(last_response.status).to eq 404
        end
    end

    describe "POST rspec" do
        it "rejects request as not specifying a stream" do
            post "/rspec", xml, "CONTENT_TYPE" => "application/xml"
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
				post "/rspec/test", xml, "CONTENT_TYPE" => "application/xml"
				expect(last_response.status).to eq 202
				sleep 1
				begin
                    a = @xmlconsumer.fetch
					expect(a).to_not be_nil
					expect(a.empty?).to be false
					expect(a[0].value).to eq "TOPIC: rspec.test\n#{xml}" 
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
			it "posts JSON to Kafka topic rspec.test" do
				puts "Next offset    = #{@genericconsumer.next_offset}"
				post "/rspec/test", csv, "CONTENT_TYPE" => "text/csv"
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
=begin
        context "SIF/XML non-sif 3 MB" do
            it "posts SIF/XML to Kafka topic SSFServer.settings.bulkxmltopic in two 1 MB-sized pieces, with header rspec.test" do
                puts "Next offset    = #{@xmlconsumer.next_offset}"
                post "/rspec/test/bulk", xml * 50000, "CONTENT_TYPE" => "application/xml"
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
                    payload = payload.gsub(/\n===snip===\n/, "")
                    expect(payload).to eq "TOPIC: rspec.test\n#{xml * 50000}"
                    expect(a[0].key).to eq "rspec.test"
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end
=end
        
        before(:example) do
            @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sifxml.bulkingest", 0, :latest_offset)
        end
        context "SIF/XML non-sif small" do
                it "posts SIF/XML to Kafka topic SSFServer.settings.xmltopic with header rspec.test" do
                puts "Next offset    = #{@xmlconsumer.next_offset}"
                post "/rspec/test/bulk", xml , "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 202
                sleep 1
                begin
                    a = @xmlconsumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].value).to eq "TOPIC: rspec.test\n#{xml}"
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
        context "CSV small" do
            it "posts JSON to Kafka topic rspec.test" do
                puts "Next offset    = #{@genericconsumer.next_offset}"
                post "/rspec/test/bulk", csv, "CONTENT_TYPE" => "text/csv"
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
=begin
        context "payload more than 500 MB" do
            it "rejects payload" do
                post "/rspec/test/bulk", xml * 5000000, "CONTENT_TYPE" => "application/xml"
                expect(last_response.status).to eq 202
            end
        end
=end
        context "message other than CSV, JSON, XML" do
            it "rejects payload" do
                post "/rspec/test/bulk", csv, "CONTENT_TYPE" => "text/plain"
                expect(last_response.status).to eq 415
            end
        end
    end
    
    
end
