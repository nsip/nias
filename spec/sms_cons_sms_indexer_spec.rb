ENV['RACK_ENV'] = 'test'

require "rspec"
require "rack/test"
require "net/http"
require "spec_helper"
require 'moneta' 
require 'securerandom'
require 'redis'


describe "SMS Indexer" do

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

@service_name = 'sms_cons_sms_indexer_spec'


	def remove_redis(key,type) 
		@redis.hdel 'labels', key
		@redis.srem type, key
		@redis.hdel "oid:#{localid}", "localid"
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
			sleep 5
		end

		it "add labels { #{guid} } = name of school" do
			key = @redis.hget 'labels', guid
                        expect(key).to eq "Stivers College"
		end
		it "add #{guid} to SchoolInfo set" do
			key = @redis.sismember 'SchoolInfo', guid
                        expect(key).to be true
		end
		it "add oid:(Local Id) {\"localid\"} = #{guid}" do
			key = @redis.hget "oid:#{localid}", "localid"
                        expect(key).to eq guid
		end
		it "add #(Local Id) to other:ids set" do
			key = @redis.sismember "other:ids", "oid:#{localid}"
                        expect(key).to be true
		end

		after(:context) do
			@store.delete(guid)
			@store.close
			remove_redis(guid, "SchoolInfo")
		end
	end
end
