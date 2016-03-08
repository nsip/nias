# cons-prod-privacyfilter.rb

require 'poseidon'
require 'nokogiri' # xml support
require_relative '../../kafkaproducers'
require_relative '../../kafkaconsumers'

=begin
Consumer of validated SIF/XML messages. 
Messages are received from the single stream sifxml.processed. The header of the received message is "topic/stream". 
Each object in the stream is filtered according to privacy filters defined in ./privacyfilters/*.xpath.

The header of the received message is "topic"."stream". Each message is passed to a stram for each privacy setting simultaneously:
* "topic"."stream".none
* "topic"."stream".low
* "topic"."stream".medium
* "topic"."stream".high
* "topic"."stream".extreme

Privacy filters are expressed as XPaths over SIF/XML. Would have loved to have used CSS, but we need to specify on occasion
XML attributes for filtering, as well as elements, and CSS cannot select attributes.

The effects of filters is cumulative: e.g. the High filter filters out the content identified in filters Low, Medium, and High. 

In order to keep the SIF/XML syntactically valid, text within the sensitive areas are not deleted, in case they are mandatory elements, but are instead replaced with "REDACTED", or an equivalent string appropriate to the type of string edited.

The privacy filters consist of lines with two elements: the XPath and the redaction text, delimited by colon. Where no redaction text is specified,
"REDACTED" is assumed.

The script is currently inefficient: each xpath in each filter is applied independently of all others, using the Nokogiri xpath() method. If it proves expedient, a more efficient filter will need to be devised.
=end

@inbound = 'sifxml.processed'

@filter = {}
@sensitivities = [:none, :low, :medium, :high, :extreme]

# Read the xpaths for filtering at a given privacy level from the nominated file
def read_filter(filepath, level)
    File.open(filepath).each do |line| 
        next if line =~ /^#/  # ignore comments
        next unless line =~ /\S/  # ignore blanks
        a = line.chomp.split(/:/)  # delimit Xpath from filter text
        a[0].gsub!(%r#(/+)(?!@)#, "\\1xmlns:") # namespaces are in XML fragments, xpaths need to allude to root default namespace
        a[1] = "ZZREDACTED" if(a.size == 1)  # insert default filter text
        @filter[level] << {:path => a[0], :redaction => a[1]}
    end
end

@filter[:low] = []
read_filter("#{__dir__}/privacyfilters/extreme.xpath", :low)
# cumulative filters: the fields to filter in the next highest sensitivity are added on to the previous sensitivity's
@filter[:medium] = Array.new(@filter[:low])
read_filter("#{__dir__}/privacyfilters/high.xpath", :medium)
@filter[:high] = Array.new(@filter[:medium])
read_filter("#{__dir__}/privacyfilters/medium.xpath", :high)
@filter[:extreme] = Array.new(@filter[:high])
read_filter("#{__dir__}/privacyfilters/low.xpath", :extreme)

# redact all textual content of xml (a Node). Is recursive.
def redact(xml, redaction)
    if(xml.cdata? or xml.text?) then 
        xml.content = redaction
    end
    xml.children.each {|x| redact(x, redaction)}
    return xml
end

# Apply filtering rules in xpaths (array of XPath lines) to xml_orig. Do not change original xml_orig parameter. Returns filtered XML
def apply_filter(xml_orig, xpaths)
    xml = xml_orig.dup
    xpaths.each { |c| 
        xml.xpath(c[:path]).each {|x| redact(x, c[:redaction]) } 
    }
    return xml
end

@servicename = "cons-prod-privacyfilter"

# create consumer
consumer = KafkaConsumers.new(@servicename, @inbound)
Signal.trap("INT") { consumer.interrupt }

# set up producer pool - busier the broker the better for speed
pool = {}
@sensitivities.each do |x|
    #pool[x] = KafkaProducers.new(@servicename, 10).get_producers.cycle
    pool[x] = KafkaProducers.new(@servicename, 10)
end

# Hash of outbound messages, mapping privacy level to array of outbound messages
outbound_messages = {}
# Hash of filtered records, mapping privacy level to filtered record
out = {}
#loop do
#	begin
        @sensitivities.each { |x| outbound_messages[x] = [] }
        #messages = []
        #messages = consumer.fetch
        #messages.each do |m|
	consumer.each do |m|

            # Payload from sifxml.ingest contains as its first line a header line with the original topic
            header = m.value.lines[0]
            payload = m.value.lines[1..-1].join
            topic = header[/TOPIC: (\S+)/, 1]

            #puts "Privacy: processing message no.: #{m.offset}, #{m.key}: #{topic}... #{m.value.lines[1]}\n\n"

            input = Nokogiri::XML(payload) do |config|
                config.nonet.noblanks
            end

            #puts "\n\nInput:\n\n#{input.to_xml}\n\n"
                        if(input.errors.empty?) 
                item_key = "prv_filter:#{ sprintf('%09d', m.offset) }"
                                out[:none] = input
                out[:low] = apply_filter(input, @filter[:low])
                out[:medium] = apply_filter(out[:low], @filter[:medium])
                out[:high] = apply_filter(out[:medium], @filter[:high])
                out[:extreme] = apply_filter(out[:high], @filter[:extreme])

                # puts "\n\nOut\n = #{out.to_s}\n\n"

                @sensitivities.each do |x|
                    #puts "\n\nSending: to #{topic}.#{x}\n\n#{out[x].to_s.lines[1]}\n\nkey: #{item_key}"
                    outbound_messages[x] << Poseidon::MessageToSend.new( "#{topic}.#{x}", out[x].to_s, item_key ) 
                end 
            end
            #if(outbound_messages[:none].length > 20)
                @sensitivities.each do |x|
                    #outbound_messages[x].each_slice(20) do | batch |
                        #pool[x].next.send_messages( batch )
                        pool[x].send_through_queue( outbound_messages[x] )
                    #end
                end
                @sensitivities.each { |x| outbound_messages[x] = [] }
            #end
	end



