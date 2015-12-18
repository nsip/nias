# cons-prod-privacyfilter.rb

require 'poseidon'
require 'nokogiri' # xml support

=begin
Consumer of validated SIF/XML messages. 
Messages are received from the single stream sifxml.validated. The header of the received message is "topic/stream". 
Each object in the stream is filtered according to privacy filters defined in ./privacyfilters/*.xpath.

The header of the received message is "topic"."stream". Each message is passed to a stram for each privacy setting simultaneously:
* "topic"."stream".none
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
		a[1] = "ZZREDACTED" if(a.size == 1)
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
	p = Poseidon::Producer.new(["localhost:9092"], "cons-prod-privacyfilter", {:partitioner => Proc.new { |key, partition_count| 0 } })
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

               # Payload from sifxml.ingest contains as its first line a header line with the original topic
                header = m.value.lines[0]
                payload = m.value.lines[1..-1].join
				topic = header[/TOPIC: (.+)/, 1]

      	    	# puts "Privacy: processing message no.: #{m.offset}, #{m.key}: #{topic}... #{m.value.lines[1]}\n\n"

				input = Nokogiri::XML(payload) do |config|
        			config.nonet.noblanks
				end

				# puts "\n\nInput:\n\n#{input.to_xml}\n\n"
		
				if(input.errors.empty?) 
		      		item_key = "prv_filter:#{ sprintf('%09d', m.offset) }"
					
					out[:none] = input
					out[:low] = apply_filter(input, @filter[:low])
					out[:medium] = apply_filter(out[:low], @filter[:medium])
					out[:high] = apply_filter(out[:medium], @filter[:high])
					out[:extreme] = apply_filter(out[:high], @filter[:extreme])

					# puts "\n\nOut\n = #{out.to_s}\n\n"

					[:none, :low, :medium, :high, :extreme].each {|x|
						if x == :none
							# puts "\n\nSending: to #{topic}.unfiltered\n\n#{out[x].to_s}\n\nkey: #{item_key}"
							outbound_messages << Poseidon::MessageToSend.new( "#{topic}", out[x].to_s, item_key ) 	
						else
							# puts "\n\nSending: to #{topic}.#{x}\n\n#{out[x].to_s}\n\nkey: #{item_key}"
							outbound_messages << Poseidon::MessageToSend.new( "#{topic}.#{x}", out[x].to_s, item_key ) 
						end
					}
				end
	
		end

		outbound_messages.each_slice(20) do | batch |
			pool.next.send_messages( batch )
   		end
		
		# puts "cons-prod-ingest:: Resuming message consumption from: #{consumer.next_offset}"
  
  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\ncons-prod-privacyfilter service shutting down...\n\n"
    consumer.close
    exit 130 
  } 

  sleep 1
end


