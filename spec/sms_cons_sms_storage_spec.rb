ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "net/http"
require "spec_helper"
require 'moneta' 
require 'securerandom'
require 'redis'


describe "SMS Storage" do

    guid = SecureRandom.uuid
    localid = SecureRandom.uuid
    xml = <<XML
    <SchoolInfo RefId="#{guid}">
        <LocalId>#{localid}</LocalId>
        <SchoolName>Stivers College</SchoolName>
        <SchoolType>Sec</SchoolType>
        <ARIA>1.0</ARIA>
        <OperationalStatus>O</OperationalStatus>
        <Campus>
            <SchoolCampusId>3</SchoolCampusId>
            <CampusType>Sec</CampusType>
            <AdminStatus>Y</AdminStatus>
        </Campus>
        <SchoolSector>Gov</SchoolSector>
        <IndependentSchool>N</IndependentSchool>
        <Entity_Open>1990-01-01</Entity_Open>
    </SchoolInfo>
XML

    header = '<SchoolInfos xmlns="http://www.sifassociation.org/au/datamodel/3.4">'
    footer = '</SchoolInfos>'

    @service_name = 'sms_cons_sms_storage_spec'




    def remove_redis(key,localid,type) 
        @redis.hdel 'labels', key
        @redis.srem type, key
        @redis.hdel "oid:#{localid}", 'localid'
        @redis.srem "other:ids", "oid:#{localid}"
    end

    context "Post SchoolInfo record with RefID #{guid}" do
        before(:context) do
            @store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')
            @redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)
            Net::HTTP.start("localhost", "9292") do |http|
                request = Net::HTTP::Post.new("/rspec/test")
                request.body = header + xml + footer
                request["Content-Type"] = "application/xml"
                http.request(request)
            end
            sleep 2
        end

        it "stores XML to Moneta with key #{guid}" do
            result = @store[guid]
            expect(result).to_not be_nil
            result.gsub!(/\n[ ]*/, "")
            result.gsub!(/ xmlns="[^"]+"/, "")
            expected = '<?xml version="1.0"?>' + "\n" + xml
            expected.gsub!(/\n[ ]*/, "")
            expect(result).to eq expected
        end

        after(:context) do
            @store.delete(guid)
            @store.close
            remove_redis(guid, localid, "SchoolInfo")
        end
    end
end
