ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "spec_helper"
require './ssf/hookup_ids_server'
require 'poseidon_cluster' 
require 'json' 


@service_name = 'spec-ssf-hookup-ids-server'

describe "HookupServer" do

    describe "POST /hookup" do

        before(:all) do
                @consumer = Poseidon::ConsumerGroup.new("#{@service_name}_xml#{rand(1000)}", ["localhost:9092"], ["localhost:2181"], "sms.indexer", trail: true, socket_timeout_ms:6000, max_wait_ms:100)
                @consumer.claimed.each { |x| @consumer.checkout { |y| puts y.next_offset }}
        end


        context "no ids parameters" do
            it "rejects request as not containing ids" do
                post "/hookup", {:dummy => '1'}
                expect(last_response.status).to eq 400
            end
        end
        context "targetid parameters" do
            it "rejects request as not containing sourceid" do
                post "/hookup", {:targetid => '1'}
                expect(last_response.status).to eq 400
            end
        end
        context "sourceid parameters" do
            it "rejects request as not containing targetid" do
                post "/hookup", {:sourceid => '1'}
                expect(last_response.status).to eq 400
            end
        end

        context "sourceid and targetid single parameters" do
	    before(:example) do
                post "/hookup", {:sourceid => 'test1', :targetid => 'test2'}
	    end
            it "post tuple linking id 1 and id 2 to sms.indexer" do
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = groupfetch(@consumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0]).to_not be_nil
                    expect(a[0].value.nil?).to be false
                    value = JSON.parse(a[0].value)
puts a[0].value
                    expect(value["id"]).to eq "test1"
                    expect(value["links"]).to eq ["test2"]
                rescue Poseidon::Errors::OffsetOutOfRange
                    puts "[warning] - bad offset supplied, resetting..."
                    offset = :latest_offset
                    retry
                end
            end
        end

        context "sourceid and targetid two parameters" do
	    before(:example) do
                post "/hookup", {:sourceid => 'test1,test2', :targetid => 'test3,test4'}
	    end
            it "post tuple linking id 1 and id 2 with id 3 and id 4 to sms.indexer" do
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = groupfetch(@consumer)
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    expect(a[0]).to_not be_nil
                    expect(a[0].value.nil?).to be false
                    expect(a.size).to be > 1
                    value = JSON.parse(a[0].value)
                    expect(value["id"]).to eq "test1"
                    expect(value["links"]).to eq ["test3", "test4"]
                    value = JSON.parse(a[1].value)
                    expect(value["id"]).to eq "test2"
                    expect(value["links"]).to eq ["test3", "test4"]
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
end
