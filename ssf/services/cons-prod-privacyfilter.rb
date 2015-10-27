# cons-prod-privacyfilter.rb

require 'poseidon'
require 'nokogiri' # xml support

=begin
Consumer of validated SIF/XML messages. 
Messages are received from the single stream sifxml.validated. The key of the received message is "topic"."stream". 
Each object in the stream is filtered according to privacy filters defined in ./privacyfilters/*.xpath.

The key of the received message is "topic"."stream". Each message is passed to a stram for each privacy setting simultaneously:
* "topic"."stream".low
* "topic"."stream".medium
* "topic"."stream".high
* "topic"."stream".extreme

Privacy filters are expressed as XPaths over SIF/XML. Would have loved to have used CSS, but we need to specify on occasion
XML attributes for filtering, as well as elements, and CSS cannot select attributes.

The effects of filters is cumulative: High filters out the content identified in # filters low, medium, and high. 

In order to keep the SIF/XML syntactically valid, all text within the sensitive areas are not deleted, in case they are mandatory
elements, but replaced with "REDACTED", or an equivalent string appropriate to the type of string edited.

The privacy filters consist of lines with two elements: the XPath and the redaction text. Where no redaction text is specified,
"REDACTED" is assumed.
=end

@inbound = 'sifxml.validated'

@filter = {}
@sensitivies = [:low, :medium, :high, :extreme]

def read_filter(filepath, level)
	File.open(filepath).each do |line| 
		next if line =~ /^#/
		next unless line =~ /\S/
		a = line.chomp.split(/:/)
		a[1] = "REDACTED" if(a.size == 1)
		@filter[level] << {:path => a[0], :redaction => a[1]}
	end
end

@filter[:extreme] = []
read_filter("./ssf/services/privacyfilters/extreme.xpath", :extreme)


# cumulative filters: the fields to filter in the next lowest sensitivity are added on to the previous sensitivity's
@filter[:high] = Array.new(@filter[:extreme])
read_filter("./ssf/services/privacyfilters/high.xpath", :high)
@filter[:medium] = Array.new(@filter[:high])
read_filter("./ssf/services/privacyfilters/medium.xpath", :medium)
@filter[:low] = Array.new(@filter[:medium])
read_filter("./ssf/services/privacyfilters/low.xpath", :low)

# redact all textual content of xml (a Node)
def redact(xml, redaction)
	if(xml.cdata? or xml.text?) then 
		xml.content = redaction
	end
	xml.children.each {|x| redact(x, redaction)}
	return xml
end

# apply filtering rules in xpaths (array of XPath lines) to xml. Do not change original xml parameter. Returned filtered XML
def apply_filter(xml_orig, xpaths)
	xml = xml_orig.dup
	xpaths.each {|c| 
		xml.xpath(c[:path]).each {|x| redact(x, c[:redaction]) } 
	}
	return xml
end


# create consumer
consumer = Poseidon::PartitionConsumer.new("cons-prod-privacyfilter", "localhost", 9092, @inbound, 0, :latest_offset)

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], "cons-prod-ingest", {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
pool = producers.cycle

out = {}
loop do
  begin
  	    outbound_messages = []
  	    messages = []
	    messages = consumer.fetch
	    messages.each do |m|
      	    puts "processing message no.: #{m.offset}, #{m.key}\n\n"

		input = Nokogiri::XML(m.value) do |config|
        		config.nonet.noblanks
		end
		if(input.errors.empty?) 
	      		item_key = "ts_entry:#{ sprintf('%09d', m.offset) }"
			out[:extreme] = apply_filter(input, @filter[:extreme])
			out[:high] = apply_filter(out[:extreme], @filter[:high])
			out[:medium] = apply_filter(out[:high], @filter[:medium])
			out[:low] = apply_filter(out[:medium], @filter[:low])
			[:low, :medium, :high, :extreme].each {|x|
				outbound_messages << Poseidon::MessageToSend.new( "#{m.key}.#{x}", out[x].to_s, item_key ) 
			}
		end

		outbound_messages.each_slice(20) do | batch |
			pool.next.send_messages( batch )
	   end
	
	end
		# puts "cons-prod-ingest:: Resuming message consumption from: #{consumer.next_offset}"
  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\ncons-prod-ingest service shutting down...\n\n"
    consumer.close
    exit 130 
  } 

  sleep 1
end

