#ENV['RACK_ENV'] = 'test'

#require "rspec"
#require "rack/test"
require "spec_helper"
require './ssf/equiv_ids_server'
require './ssf/ssf_server'
require 'poseidon' 
require 'json' 


@service_name = 'spec-ssf-equiv-ids-server'
puts @service_name


describe "EquivalenceServer" do

    describe "POST /equiv" do
	context "no ids parameters" do
        	it "rejects request as not containing two ids" do
            		post "/equiv", {:dummy => '1'}
            		expect(last_response.status).to eq 400
		end
        end
	context "one ids parameters" do
        	it "rejects request as not containing two ids" do
            		post "/equiv", {:ids => '1'}
            		expect(last_response.status).to eq 400
		end
        end

	before(:example) do
		@consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sms.indexer", 0, :latest_offset)
	end
	context "two ids parameters" do
        	it "post tuple identifying id 1 and id 2 to sms.indexer" do
			puts "Next offset    = #{@consumer.next_offset}"
            		post "/equiv", {:ids => 'test1,test2'}
			expect(last_response.status).to eq 200
			sleep 1
			begin
                    		a = @consumer.fetch
				expect(a).to_not be_nil
				expect(a.empty?).to be false
				value = JSON.parse(a[0].value)
				expect(value["id"]).to eq "test1"
				expect(value["equivalentids"]).to eq ["test2"]
                    	rescue Poseidon::Errors::OffsetOutOfRange
                            puts "[warning] - bad offset supplied, resetting..."
                            offset = :latest_offset
                            retry
			end
		end
        end

	before(:example) do
		@consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sms.indexer", 0, :latest_offset)
	end
	context "three ids parameters" do
        	it "post tuple identifying id 1 with id 2 and id 3 to sms.indexer" do
			puts "Next offset    = #{@consumer.next_offset}"
            		post "/equiv", {:ids => 'test1,test2,test3'}
			expect(last_response.status).to eq 200
			sleep 1
			begin
                    		a = @consumer.fetch
				expect(a).to_not be_nil
				expect(a.empty?).to be false
				value = JSON.parse(a[0].value)
				expect(value["id"]).to eq "test1"
				expect(value["equivalentids"]).to eq ["test2","test3"]
                    	rescue Poseidon::Errors::OffsetOutOfRange
                            puts "[warning] - bad offset supplied, resetting..."
                            offset = :latest_offset
                            retry
			end
		end
        end
    end


end
