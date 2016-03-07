#ENV['RACK_ENV'] = 'test'

#require "rspec"
#require "rack/test"
require "spec_helper"
require './ssf/equiv_ids_server'
require './ssf/ssf_server'
require 'poseidon_cluster' 
require 'json' 
require_relative '../niasconfig'

@service_name = 'spec-ssf-equiv-ids-server'



    describe "POST /equiv" do

        before(:all) do
		config = NiasConfig.new
        	@consumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["#{config.kafka}"], ["#{config.zookeeper}"], "sms.indexer", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
        	@consumer.claimed.each { |x| @consumer.checkout { |y| puts y.next_offset }}
	end

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

        context "two ids parameters" do
	    before(:example) do
                post "/equiv", {:ids => 'test1,test2'}
	    end
            it "post tuple identifying id 1 and id 2 to sms.indexer" do
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = groupfetch(@consumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
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

        context "three ids parameters" do
	    before(:example) do
                post "/equiv", {:ids => 'test1,test2,test3'}
	    end
            it "post tuple identifying id 1 with id 2 and id 3 to sms.indexer" do
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = groupfetch(@consumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0].nil?).to be false
                    expect(a[0].value.nil?).to be false
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

    after(:all) do
	@consumer.close
	sleep 5
    end
	end
