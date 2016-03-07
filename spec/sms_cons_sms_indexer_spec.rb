ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "net/http"
require "spec_helper"
require 'moneta' 
require 'securerandom'
require 'redis'
require_relative '../niasconfig'

describe "SMS Indexer" do

    guid = SecureRandom.uuid
    guid2 = SecureRandom.uuid
    linkid = SecureRandom.uuid
    localid = SecureRandom.uuid
    xml = <<XML
    <SchoolInfo RefId="#{guid}">
        <LocalId>#{localid}</LocalId>
        <SchoolName>Stivers College</SchoolName>
        <LEAInfoRefId>#{linkid}</LEAInfoRefId>
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

    @service_name = 'sms_cons_sms_indexer_spec'

    $config = NiasConfig.new

    def remove_redis(key, localid, guid2, linkid, type) 
        @redis.hdel 'labels', key
        @redis.srem type, key
        @redis.hdel "oid:#{localid}", "localid"
        @redis.srem "other:ids", "oid:#{localid}"
        @redis.srem "equivalent:id:#{guid2}", key
        @redis.srem "#{linkid}", key
    end

    def post_xml(xml, path)
        Net::HTTP.start("#{$config.get_host}", "#{$config.get_sinatra_port}") do |http|
            request = Net::HTTP::Post.new(path)
            request.body = xml unless xml.nil?
            request["Content-Type"] = "application/xml" unless xml.nil?
            http.request(request)
        end
        sleep 5
    end

    before(:all) do
        @store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')
        @redis = $config.redis
    end

    context "Post SchoolInfo record with RefID #{guid}" do
        before(:context) do
            post_xml(header + xml + footer, "/rspec/test")
        end

        it "add labels {RefID} = name of school" do
            key = @redis.hget 'labels', guid
            expect(key).to eq "Stivers College"
        end
        it "add RefID to SchoolInfo set" do
            key = @redis.sismember 'SchoolInfo', guid
            expect(key).to be true
        end
        it "add oid:(Local Id) {\"localid\"} = RefID" do
            key = @redis.hget "oid:#{localid}", "localid"
            expect(key).to eq guid
        end
        it "add oid:(Local Id) to other:ids set" do
            key = @redis.sismember "other:ids", "oid:#{localid}"
            expect(key).to be true
        end
        it "add RefID to set LEAInfoRefId" do
            key = @redis.sismember "#{linkid}", "#{guid}"
            expect(key).to be true
        end
        after(:context) do
            @store.delete(guid)
            remove_redis(guid, localid, guid2, linkid, "SchoolInfo")
        end
    end


    context "Post SchoolInfo record then post /equiv?ids=#{guid},#{guid2}" do
        before(:context) do
            post_xml(header + xml + footer, "/rspec/test")
            post "/equiv", {:ids => "#{guid},#{guid2}"}
            #post_xml(nil, "/equiv?ids=#{guid},#{guid2}")
            sleep 2
        end

        it "adds #{guid} to set equivalent:ids:#{guid2}" do
            puts "guid2 set" + (@redis.smembers "equivalent:ids:#{guid2}").join(" ")
            key = @redis.sismember "equivalent:ids:#{guid2}", guid
            expect(key).to be true
        end
        it "adds links from #{guid} set to #{guid2} set" do
            key = @redis.sismember "#{guid2}", linkid
            expect(key).to be true
        end
        after(:context) do
            @store.delete(guid)
            remove_redis(guid, localid, guid2, linkid, "SchoolInfo")
        end
    end

    after(:all) do
        @store.close
    end

end
