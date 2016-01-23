# cons-prod-sif2scv-studentpersonal-naplanreg-parser.rb

# consumer that reads in studentpersonal records from naplan/sifxml stream, 
# and generates csv equivalent records in naplan/csvstudents stream
 

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'

@inbound = 'naplan.sifxml_staff.none'
@outbound = 'naplan.csvstaff_out'

@servicename = 'cons-prod-sif2csv-staffpersonal-naplanreg-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)
#consumer = Poseidon::ConsumerGroup.new(@servicename, ["localhost:9092"], ["localhost:2181"], @inbound)

#puts "#{@servicename} fetching offset #{ consumer.offset(0) } "

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
@pool = producers.cycle

def lookup_xpath(nodes, xpath)
	@ret = nodes.at_xpath(xpath)
	return "" if @ret.nil?
	return @ret.child
end

def lookup_xpath_multi(nodes, xpath)
	@ret = nodes.xpath(xpath)
	return "" if @ret.nil?
	return @ret.map { |x| x.child.to_s }.join(",")
	# we don't want the native ruby csv encoding of nested arrays, which is "[""x", ""y""]", but just "x,y"
end

def csv_object2array(csv)
	@ret = Array.new(@csvheaders_staff.length)
	@csvheaders_staff.each_with_index do |key, i|
		@ret[i] = csv[key]
	end
	return @ret
end

loop do

  begin
  	    messages = []
	    outbound_messages = []
	    messages = consumer.fetch

#puts "#{@servicename} fetching offset #{ consumer.offset(n) } "
#puts messages[0].value.lines[0..10].join("\n") + "\n\n" unless messages.empty?
	    messages.each do |m|

	    	# create csv object
		csv = { }
            	payload = m.value

      		# read xml message
      		nodes = Nokogiri::XML( payload ) do |config|
        		config.nonet.noblanks
			end      		

		        type = nodes.root.name
			next unless type == 'StaffPersonal'

			csv['LocalId'] = lookup_xpath(nodes, "//xmlns:LocalId")
			csv['FamilyName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
			csv['GivenName'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
			csv['Homegroup'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:HomeGroup")
			csv['ClassCode'] = lookup_xpath_multi(nodes, "//xmlns:MostRecent/xmlns:NAPLANClassList/xmlns:ClassCode")
			csv['ASLSchoolId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
			csv['SchoolLocalId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
			csv['LocalCampusId'] = lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
			csv['EmailAddress'] = lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:EmailList/xmlns:Email")

			# puts "\nParser Index = #{idx.to_json}\n\n"
			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", csv_object2array(csv).to_csv.chomp.gsub(/\s+/, " ") + "\n", "indexed" )
  		
  		end
  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
#puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
			@pool.next.send_messages( batch )
	   	end
		#end


      # puts "cons-prod-sif-parser: Resuming message consumption from: #{consumer.next_offset}"

  rescue Poseidon::Errors::UnknownTopicOrPartition
    puts "Topic #{@inbound} does not exist yet, will retry in 30 seconds"
    sleep 30
  end
  
  # puts "Resuming message consumption from: #{consumer.next_offset}"

  # trap to allow console interrupt
  trap("INT") { 
    puts "\n#{@servicename} service shutting down...\n\n"
    exit 130 
  } 

  sleep 1
  
end

