ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "spec_helper"
require './ssf/hookup_ids_server'
require 'poseidon' 
require 'json' 


@service_name = 'spec-ssf-hookup-ids-server'
puts @service_name

describe "HookupServer" do

    describe "POST /hookup" do
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

        before(:example) do
            @consumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "sms.indexer", 0, :latest_offset)
        end
        context "sourceid and targetid single parameters" do
            it "post tuple linking id 1 and id 2 to sms.indexer" do
                puts "Next offset    = #{@consumer.next_offset}"
                post "/hookup", {:sourceid => 'test1', :targetid => 'test2'}
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = @consumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
                    value = JSON.parse(a[0].value)
                    expect(value["id"]).to eq "test1"
                    expect(value["links"]).to eq ["test2"]
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
        context "sourceid and targetid two parameters" do
            it "post tuple linking id 1 and id 2 with id 3 and id 4 to sms.indexer" do
                puts "Next offset    = #{@consumer.next_offset}"
                post "/hookup", {:sourceid => 'test1,test2', :targetid => 'test3,test4'}
                expect(last_response.status).to eq 200
                sleep 1
                begin
                    a = @consumer.fetch
                    expect(a).to_not be_nil
                    expect(a.empty?).to be false
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
    end


end
