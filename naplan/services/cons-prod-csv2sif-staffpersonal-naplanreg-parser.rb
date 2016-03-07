# cons-prod-csv2sif-staffpersonal-naplanreg-parser.rb
# Consumer that reads in StaffPersonal CSV records conforming to the NAPLAN Registration specification from naplan/csv_staff stream,
# and generates SIF/XML equivalent records in naplan/sifxmlout_staff stream.

# Underlying assumption: there will only be one class code


require 'nokogiri'
require 'json'
require 'poseidon'
require 'hashids'
require 'csv'
require 'securerandom'
require_relative 'cvsheaders-naplan'
require 'json-schema'
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'
require_relative '../../niaserror'

@inbound = 'naplan.csv_staff'
@outbound = 'sifxml.ingest'
@errbound = 'csv.errors'
#@outbound = 'naplan.sifxmlout_staff'

@idgen = Hashids.new( 'nsip random temp uid' )

@servicename = 'cons-prod-csv2sif-staffpersonal-naplanreg-parser'

# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }


producers = KafkaProducers.new(@servicename, 10)
#@pool = producers.get_producers.cycle

# default values
@default_csv = {'StaffSchoolRole' => 'principal', 'AdditionalInfo' => 'N' }

# JSON schema
@jsonschema = JSON.parse(File.read("#{__dir__}/naplan.staff.json"))

# http://stackoverflow.com/questions/3450641/removing-all-empty-elements-from-a-hash-yaml
class Hash
	def compact
		delete_if { |k, v| v.nil? or (v.respond_to?('empty?') and v.strip.empty?) }
	end
end


        messages = []
        outbound_messages = []
        #messages = consumer.fetch
        consumer.each do |m|
            row = JSON.parse(m.value) 
            # Carriage return unacceptable
            row.each_key do |key|
		next unless row[key].is_a? String
                row[key].gsub!("[ ]*\n[ ]*", " ")
            end
	    row.merge!(@default_csv) { |key, v1, v2| v1 }
                # delete any blank/empty values
                row = row.compact


	    #obsolete: there will only be one classcode in CSV
            #classcodes = row['ClassCode'].split(/,/)
            #classcodes_xml = ''
            #classcodes.each { |x| classcodes_xml << "      <ClassCode>#{x}</ClassCode>\n" }
	    # validate that we have received the right kind of record here, from the headers
	    if(row['LocalId'] and not row['LocalStaffId']) 
            	outbound_messages << Poseidon::MessageToSend.new( "#{@errbound}", 
			NiasError.new(0,1, "CSV Document Type Error", 
				"You appear to have submitted a StudentPersonal record instead of a StaffPersonal record\n#{row['__linecontent']}").to_s, 
			"rcvd:#{ sprintf('%09d:%d', m.offset, 0)}" )
	    else

                # validate against JSON Schema
                json_errors = JSON::Validator.fully_validate(@jsonschema, row)
                # any errors are on mandatory elements, so stop processing further
                unless(json_errors.empty?)
                        json_errors.each_with_index do |e, i|
                                puts e
                                outbound_messages << Poseidon::MessageToSend.new( "#{@errbound}", 
					NiasError.new(i, json_errors.length, "JSON Validation Error", "#{e}\n#{row['__linecontent']}").to_s, 
					"rcvd:#{ sprintf('%09d:%d', m.offset, i)}" )
                        end
                        producers.send_through_queue( outbound_messages )
			outbound_messages = []
                        next
                end



# inject the source CSV line number as a comment into the generated XML; errors found in the templated SIF/XML
# will be reported back in the csv.errors stream

                xml = <<XML
    <StaffPersonals xmlns="http://www.sifassociation.org/au/datamodel/3.4">
    <StaffPersonal RefId="#{SecureRandom.uuid}">
    <!-- CSV line #{row['__linenumber']} -->
    <!-- CSV content #{row['__linecontent']} -->
      <LocalId>#{row['LocalStaffId']}</LocalId>
      <PersonInfo>
        <Name Type="LGL">
          <FamilyName>#{row['FamilyName']}</FamilyName>
          <GivenName>#{row['GivenName']}</GivenName>
        </Name>
        <EmailList>
          <Email Type="01">#{row['EmailAddress']}</Email>
        </EmailList>
      </PersonInfo>
      <Title>#{row['StaffSchoolRole']}</Title>
      <MostRecent>
        <SchoolLocalId>#{row['LocalSchoolId']}</SchoolLocalId>
        <SchoolACARAId>#{row['ASLSchoolId']}</SchoolACARAId>
        <LocalCampusId>#{row['LocalCampusId']}</LocalCampusId>
        <NAPLANClassList>
          <ClassCode>#{row['ClassCode']}</ClassCode>
        </NAPLANClassList>
        <HomeGroup>#{row['HomeGroup']}</HomeGroup>
      </MostRecent>
      <SIF_ExtendedElements>
        <SIF_ExtendedElement Name="AdditionalInfo">#{row['AdditionalInfo']}</SIF_ExtendedElement>
      </SIF_ExtendedElements>
    </StaffPersonal>
    </StaffPersonals>
XML

    
    
                nodes = Nokogiri::XML( xml ) do |config|
                    config.nonet.noblanks
                end
                # remove empty nodes from anywhere in the document
                nodes.xpath('//*//child::*[not(node()) and not(text()[normalize-space()]) ]').each do |node|
                    node.remove
                end
                nodes.xpath('//*/child::*[text() and not(text()[normalize-space()])]').each do |node|
                    node.remove
                end
                outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", "TOPIC: naplan.sifxmlout_staff\n" + nodes.root.to_s, "rcvd:#{ sprintf('%09d', m.offset)}" )
            end

        # send results to indexer to create sms data graph
	producers.send_through_queue(outbound_messages)
	outbound_messages = []
end
