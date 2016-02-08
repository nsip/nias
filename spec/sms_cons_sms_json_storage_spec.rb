ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "net/http"
require "spec_helper"
require 'moneta' 
require 'securerandom'
require 'redis'


describe "JSON SMS Storage" do

    guid = SecureRandom.uuid
    localid = SecureRandom.uuid
    csv = <<CSV
id,something,somethingelse
#{guid},test,hello
CSV

    json = %Q!{"id"=>"#{guid}", "something"=>"test", "somethingelse"=>"hello", "__linenumber"=>1}!

    @service_name = 'sms_cons_sms_json_storage_spec'

    def remove_redis(key,localid,type) 
    end

    context "Post simple CSV message to rspec/test with id #{guid}" do
        before(:context) do
            @store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')
            #@redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)
            Net::HTTP.start("localhost", "9292") do |http|
                request = Net::HTTP::Post.new("/rspec/test")
                request.body = csv
                request["Content-Type"] = "text/csv"
                http.request(request)
            end
            sleep 2
        end

        it "stores XML to Moneta with key rspec.test::#{guid}" do
            result = @store["rspec.test::#{guid}"]
            expect(result).to_not be_nil
            expect(result.to_s).to eq json
        end

        after(:context) do
            @store.delete(guid)
            @store.close
            remove_redis(guid, localid, "SchoolInfo")
        end
    end
end
