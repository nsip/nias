# cons-prod-sif-parser.rb

# simple consumer that reads sif xml messages from an input stream
# assumes that sif messages have already been validated for xml
# 
# parses message to find refid & type of message and to build index of
# all other references contained in the xml message
# 
# Extracts GUID id (RefID), other ids, type and [links] from each message 
#
# e.g. <refid> [OtherIdType => OtherId] <StudentSchoolEnrolment> [<StudentPersonalRefId><SchoolInfoRefId>] 
# 
# this  [ 'tuple' id - [equivalentids] - {otherids} - type - [links] ]
# 
# is then passed on to the sms indexing service
# 
# this is done so that indexer only deals with abstract tuples of this type, which can therefore come
# from ANY parsed input; doesn't have to be SIF messages, cna be IMS, CSV etc. etc.
# 

require 'json'
require 'nokogiri'
require 'poseidon'
require 'hashids'


# extract human readable label based on object
# id is GUID, nodes is Nokogiri-parsed XML
def extract_label(id, nodes)
	type = nodes.root.name
	ret = nil
	case type
	when "Activity"
		ret = nodes.at_xpath("//xmlns:Title").child
	when "AggregateStatisticInfo"
		ret = nodes.at_xpath("//xmlns:StatisticName").child
	when "Assessment", "Sif3Assessment"
		ret = nodes.at_xpath("//xmlns:Name").child
	when "AssessmentAdministration", "Sif3AssessmentAdministration"
		ret = nodes.at_xpath("//xmlns:AdministrationName").child
	when "Sif3AssessmentAsset"
		ret = nodes.at_xpath("//xmlns:AssetName").child
	when "AssessmentForm", "Sif3AssessmentForm"
		ret = nodes.at_xpath("//xmlns:FormName").child
	when "AssessmentItem", "Sif3AssessmentItem"
		ret = nodes.at_xpath("//xmlns:ItemLabel").child
	when "Sif3AssessmentRubric"
		ret = nodes.at_xpath("//xmlns:RubricName").child
	when "Sif3AssessmentScoreTable"
		ret = nodes.at_xpath("//xmlns:ScoreTableName").child
	when "Sif3AssessmentSection"
		ret = nodes.at_xpath("//xmlns:SectionName").child
	when "Sif3AssessmentSession"
		ret = nodes.at_xpath("//xmlns:SessionName").child
	when "AssessmentSubTest"
		ret = nodes.at_xpath("//xmlns:Name").child
	when "Sif3AssessmentSubTest"
		ret = nodes.at_xpath("//xmlns:SubTestName").child
	when "CalendarDate"
		ret = nodes.at_xpath("//xmlns:@Date").content
	when "CalendarSummary"
		ret = nodes.at_xpath("//xmlns:LocalId").child
	when "ChargedLocationInfo"
		ret = nodes.at_xpath("//xmlns:Name").child
	when "Debtor"
		bname = nodes.at_xpath("//xmlns:BillingName")
		unless(bname.nil?)
			ret = bname.child
		end
	when "EquipmentInfo"
		ret = nodes.at_xpath("//xmlns:Name").child
	when "FinancialAccount"
		ret = nodes.at_xpath("//xmlns:AccountNumber").child
	when "GradingAssignment"
		ret = nodes.at_xpath("//xmlns:Description").child
	when "Invoice"
		fnumber = nodes.at_xpath("//xmlns:FormNumber")
		unless(fnumber.nil?)
			ret = fnumber.child
		end
	when "LEAInfo"
		ret = nodes.at_xpath("//xmlns:LEAName").child
	when "LearningResource"
		ret = nodes.at_xpath("//xmlns:Name").child
	when "LearningStandardDocument"
		ret = nodes.at_xpath("//xmlns:Title").child
	when "LearningStandardItem"
		ret = nodes.at_xpath("//xmlns:StatementCodes/xmlns:StatementCode[1]").child
	when "PaymentReceipt"
		ret = nodes.at_xpath("//xmlns:ReceivedTransactionId").child
	when "PurchaseOrder"
		ret = nodes.at_xpath("//xmlns:FormNumber").child
	when "ReportAuthorityInfo"
		ret = nodes.at_xpath("//xmlns:AuthorityName").child
	when "ResourceBooking"
		ret1 = nodes.at_xpath("//xmlns:ResourceLocalId").child 
		ret2 = nodes.at_xpath("//xmlns:ResourceLocalId").child 
		ret = ret1.to_s + " " + ret2.to_s
	when "RoomInfo"
		ret = nodes.at_xpath("//xmlns:RoomNumber").child
	when "ScheduledActivity"
		ret = nodes.at_xpath("//xmlns:ActivityName").child
	when "SchoolCourse"
		ret = nodes.at_xpath("//xmlns:CourseCode").child
	when "SchoolInfo"
		ret = nodes.at_xpath("//xmlns:SchoolName").child
	when "SectionInfo"
		ret = nodes.at_xpath("//xmlns:LocalId").child
	when "StaffPersonal", "StudentContactPersonal", "StudentPersonal"
		fname = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Name/xmlns:FullName")
		if(fname.nil?)
			ret1 = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName").child 
			ret2 = nodes.at_xpath("//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName").child 
			ret = ret1.to_s + " " + ret2.to_s
		else
			ret = fname.child
		end
	when "StudentActivityInfo"
		ret = nodes.at_xpath("//xmlns:Title").child
	when "TeachingGroup"
		ret = nodes.at_xpath("//xmlns:ShortName").child
	when "TermInfo"
		ret = nodes.at_xpath("//xmlns:TermCode").child
	when "TimeTable"
		ret = nodes.at_xpath("//xmlns:Title").child
	when "TimeTableCell"
		ret = nodes.at_xpath("//xmlns:DayId").child.to_s + ":" + nodes.at_xpath("//xmlns:PeriodId").child.to_s
	when "TimeTableSubject"
		ret = nodes.at_xpath("//xmlns:SubjectLocalId").child
	when "VendorInfo"
		fname = nodes.at_xpath("//xmlns:Name/FullName").child
		if(fname.nil?)
			ret1 = nodes.at_xpath("//xmlns:Name/xmlns:GivenName").child 
			ret2 = nodes.at_xpath("//xmlns:Name/xmlns:FamilyName").child 
			ret = ret1.to_s + " " + ret2.to_s
		else
			ret = fname.child
		end
	end
	ret = id if ret.nil?  
	return ret
end







@inbound = 'sifxml.validated'
@outbound = 'sms.indexer'

@servicename = 'cons-prod-sif-parser'

@idgen = Hashids.new( 'nsip random temp uid' )

# create consumer
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092, @inbound, 0, :latest_offset)

# set up producer pool - busier the broker the better for speed
producers = []
(1..10).each do | i |
	p = Poseidon::Producer.new(["localhost:9092"], @servicename, {:partitioner => Proc.new { |key, partition_count| 0 } })
	producers << p
end
@pool = producers.cycle

loop do

  begin
  		messages = []
	    messages = consumer.fetch
	    outbound_messages = []
	    
	    messages.each do |m|

	    	# create 'empty' index tuple
			idx = { :type => nil, :id => @idgen.encode( rand(1...999) ), :otherids => {}, :links => [], :equivalentids => [], :label => nil }      	

			header = m.value.lines[0]
            payload = m.value.lines[1..-1].join

      		# read xml message
      		nodes = Nokogiri::XML( payload ) do |config|
        		config.nonet.noblanks
			end      		

			# for rare nodes like StudentContactRelationship can be no mandatory refid
			# optional refid will already be captured in [links] as child node
			# but need to parse for the object type and assign the optional refid back to the object
			
			# type is always first node
			idx[:type] = nodes.root.name

			# concatenate name and see id refid exists, if not create a random one
			refs = nodes.css( "#{nodes.root.name}RefId" )
			idx[:id] = refs.children.first unless refs.empty?

			# ...now deal with vast majority of normal sif xml types

			# get any pure refids
			root_types = nodes.xpath("//@RefId")  
			root_types.each do | node |
				# puts node.parent.name
				# puts node.child
				# puts "\n\nType: #{node.parent.name} - ID: #{node.child}\n\n"
				idx[:type] = node.parent.name
				idx[:id] = node.child
			end

			# any node attributes that have refid suffix
			references = nodes.xpath( "//@*[substring(name(), string-length(name()) - 4) = 'RefId']" )
			references.each do | node |
				# puts node.name
				# puts node.content
				idx[:links] << node.content
			end

			# any nodes that have refid suffix
			references = nodes.xpath( "//*[substring(name(), string-length(name()) - 4) = 'RefId']" )
			references.each do | node |
				# puts node.name
				# puts node.content
				idx[:links] << node.content
			end

			# any objects that are reference objects
			ref_objects = nodes.xpath("//@SIF_RefObject")
			ref_objects.each do | node |
				# puts node.child
				# puts node.parent.content
				idx[:links] << node.parent.content
			end

			# any LocalIds
			localids = nodes.xpath("//xmlns:LocalId")
			localids.each do |node|
				idx[:otherids][:localid] = node.child
			end

			# any StateProvinceIds
			stateprovinceids = nodes.xpath("//xmlns:StateProvinceId")
			stateprovinceids.each do |node|
				idx[:otherids][:stateprovinceids] = node.child
			end

			# any ACARAIds
			acaraids = nodes.xpath("//xmlns:ACARAId")
			acaraids.each do |node|
				idx[:otherids][:acaraids] = node.child
			end

			# any Electronic IDs
			electronicids = nodes.xpath("//xmlns:ElectronicIdList/xmlns:ElectronicId")
			electronicids.each do |node|
				idx[:otherids]["electronicid"+node.attribute("Type")] = node.child
			end

			# any Other IDs
			otherids = nodes.xpath("//xmlns:OtherIdList/xmlns:OtherId")
			otherids.each do |node|
				idx[:otherids][node.attribute("Type")] = node.child
			end

			idx[:label] = extract_label(idx[:id], nodes)

			#puts "\nParser Index = #{idx.to_json}\n\n"

			outbound_messages << Poseidon::MessageToSend.new( "#{@outbound}", idx.to_json, "indexed" )
  		
  		end

  		# send results to indexer to create sms data graph
  		outbound_messages.each_slice(20) do | batch |
			@pool.next.send_messages( batch )
	   	end


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

