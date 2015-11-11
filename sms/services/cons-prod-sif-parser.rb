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
		ret = nodes.at_xpath("//Title").child
	when "AggregateStatisticInfo"
		ret = nodes.at_xpath("//StatisticName").child
	when "Assessment", "Sif3Assessment"
		ret = nodes.at_xpath("//Name").child
	when "AssessmentAdministration", "Sif3AssessmentAdministration"
		ret = nodes.at_xpath("//AdministrationName").child
	when "Sif3AssessmentAsset"
		ret = nodes.at_xpath("//AssetName").child
	when "AssessmentForm", "Sif3AssessmentForm"
		ret = nodes.at_xpath("//FormName").child
	when "AssessmentItem", "Sif3AssessmentItem"
		ret = nodes.at_xpath("//ItemLabel").child
	when "Sif3AssessmentRubric"
		ret = nodes.at_xpath("//RubricName").child
	when "Sif3AssessmentScoreTable"
		ret = nodes.at_xpath("//ScoreTableName").child
	when "Sif3AssessmentSection"
		ret = nodes.at_xpath("//SectionName").child
	when "Sif3AssessmentSession"
		ret = nodes.at_xpath("//SessionName").child
	when "AssessmentSubTest"
		ret = nodes.at_xpath("//Name").child
	when "Sif3AssessmentSubTest"
		ret = nodes.at_xpath("//SubTestName").child
	when "CalendarDate"
		ret = nodes.at_xpath("//@Date").content
	when "CalendarSummary"
		ret = nodes.at_xpath("//LocalId").child
	when "ChargedLocationInfo"
		ret = nodes.at_xpath("//Name").child
	when "Debtor"
		bname = nodes.at_xpath("//BillingName")
		unless(bname.nil?)
			ret = bname.child
		end
	when "EquipmentInfo"
		ret = nodes.at_xpath("//Name").child
	when "FinancialAccount"
		ret = nodes.at_xpath("//AccountNumber").child
	when "GradingAssignment"
		ret = nodes.at_xpath("//Description").child
	when "Invoice"
		ret = nodes.at_xpath("//FormNumber").child
	when "LEAInfo"
		ret = nodes.at_xpath("//LEAName").child
	when "LearningResource"
		ret = nodes.at_xpath("//Name").child
	when "LearningStandardDocument"
		ret = nodes.at_xpath("//Title").child
	when "LearningStandardItem"
		ret = nodes.at_xpath("//StatementCodes/StatementCode[1]").child
	when "PaymentReceipt"
		ret = nodes.at_xpath("//ReceivedTransactionId").child
	when "PurchaseOrder"
		ret = nodes.at_xpath("//FormNumber").child
	when "ReportAuthorityInfo"
		ret = nodes.at_xpath("//AuthorityName").child
	when "ResourceBooking"
		ret1 = nodes.at_xpath("//ResourceLocalId").child 
		ret2 = nodes.at_xpath("//ResourceLocalId").child 
		ret = ret1.to_s + " " + ret2.to_s
	when "RoomInfo"
		ret = nodes.at_xpath("//RoomNumber").child
	when "ScheduledActivity"
		ret = nodes.at_xpath("//ActivityName").child
	when "SchoolCourse"
		ret = nodes.at_xpath("//CourseCode").child
	when "SchoolInfo"
		ret = nodes.at_xpath("//SchoolName").child
	when "SectionInfo"
		ret = nodes.at_xpath("//LocalId").child
	when "StaffPersonal", "StudentContactPersonal", "StudentPersonal"
		fname = nodes.at_xpath("//PersonInfo/Name/FullName")
		if(fname.nil?)
			ret1 = nodes.at_xpath("//PersonInfo/Name/GivenName").child 
			ret2 = nodes.at_xpath("//PersonInfo/Name/FamilyName").child 
			ret = ret1.to_s + " " + ret2.to_s
		else
			ret = fname.child
		end
	when "StudentActivityInfo"
		ret = nodes.at_xpath("//Title").child
	when "TeachingGroup"
		ret = nodes.at_xpath("//ShortName").child
	when "TermInfo"
		ret = nodes.at_xpath("//TermCode").child
	when "TimeTable"
		ret = nodes.at_xpath("//Title").child
	when "TimeTableCell"
		ret = nodes.at_xpath("//DayId").child.to_s + ":" + nodes.at_xpath("//PeriodId").child.to_s
	when "TimeTableSubject"
		nodes1 = nodes.at_xpath("//CourseLocalId")
		nodes1 = nodes.at_xpath("//SubjectShortName").child if nodes1.nil? 
		nodes1 = nodes.at_xpath("//SubjectLongName").child if nodes1.nil? 
		ret = nodes1.child
	when "VendorInfo"
		fname = nodes.at_xpath("//Name/FullName").child
		if(fname.nil?)
			ret1 = nodes.at_xpath("//Name/GivenName").child 
			ret2 = nodes.at_xpath("//Name/FamilyName").child 
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
consumer = Poseidon::PartitionConsumer.new(@servicename, "localhost", 9092,
                                           @inbound, 0, :latest_offset)


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
			localids = nodes.xpath("//LocalId")
			localids.each do |node|
				idx[:otherids][:localid] = node.child
			end

			# any StateProvinceIds
			stateprovinceids = nodes.xpath("//StateProvinceId")
			stateprovinceids.each do |node|
				idx[:otherids][:stateprovinceids] = node.child
			end

			# any ACARAIds
			acaraids = nodes.xpath("//ACARAId")
			acaraids.each do |node|
				idx[:otherids][:acaraids] = node.child
			end

			# any Electronic IDs
			electronicids = nodes.xpath("//ElectronicIdList/ElectronicId")
			electronicids.each do |node|
				idx[:otherids]["electronicid"+node.attribute("Type")] = node.child
			end

			# any Other IDs
			otherids = nodes.xpath("//OtherIdList/OtherId")
			otherids.each do |node|
				idx[:otherids][node.attribute("Type")] = node.child
			end

			idx[:label] = extract_label(idx[:id], nodes)

			# puts "\nParser Index = #{idx.to_json}\n\n"

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

