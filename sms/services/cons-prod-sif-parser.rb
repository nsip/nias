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

# extract human readable label based on object
def extract_label(id, nodes)
	type = nodes.root.name
	ret = nil
	case type
	when "Activity"
		ret = nodes.xpath("//Title")
	when "AggregateStatisticInfo"
		ret = nodes.xpath("//StatisticName")
	when "Assessment", "Sif3Assessment"
		ret = nodes.xpath("//Name")
	when "AssessmentAdministration", "Sif3AssessmentAdministration"
		ret = nodes.xpath("//AdministrationName")
	when "Sif3AssessmentAsset"
		ret = nodes.xpath("//AssetName")
	when "AssessmentForm", "Sif3AssessmentForm"
		ret = nodes.xpath("//FormName")
	when "AssessmentItem", "Sif3AssessmentItem"
		ret = nodes.xpath("//ItemLabel")
	when "Sif3AssessmentRubric"
		ret = nodes.xpath("//RubricName")
	when "Sif3AssessmentScoreTable"
		ret = nodes.xpath("//ScoreTableName")
	when "Sif3AssessmentSection"
		ret = nodes.xpath("//SectionName")
	when "Sif3AssessmentSession"
		ret = nodes.xpath("//SessionName")
	when "AssessmentSubTest"
		ret = nodes.xpath("//Name")
	when "Sif3AssessmentSubTest"
		ret = nodes.xpath("//SubTestName")
	when "CalendarDate"
		ret = nodes.xpath("//@Date")
	when "CalendarSummary"
		ret = nodes.xpath("//LocalId")
	when "ChargedLocationInfo"
		ret = nodes.xpath("//Name")
	when "Debtor"
		ret = nodes.xpath("//BillingName")
	when "EquipmentInfo"
		ret = nodes.xpath("//Name")
	when "FinancialAccount"
		ret = nodes.xpath("//AccountNumber")
	when "GradingAssignment"
		ret = nodes.xpath("//Description")
	when "Invoice"
		ret = nodes.xpath("//FormNumber")
	when "LEAInfo"
		ret = nodes.xpath("//LEAName")
	when "LearningResource"
		ret = nodes.xpath("//Name")
	when "LearningStandardDocument"
		ret = nodes.xpath("//Title")
	when "LearningStandardItem"
		ret = nodes.xpath("//StatementCodes/StatementCode[1]")
	when "PaymentReceipt"
		ret = nodes.xpath("//ReceivedTransactionId")
	when "PurchaseOrder"
		ret = nodes.xpath("//FormNumber")
	when "ReportAuthorityInfo"
		ret = nodes.xpath("//AuthorityName")
	when "ResourceBooking"
		ret = nodes.xpath("//ResourceLocalId") + " " nodes.xpath("//StartDateTime")
	when "RoomInfo"
		ret = nodes.xpath("//RoomNumber")
	when "ScheduledActivity"
		ret = nodes.xpath("//ActivityName")
	when "SchoolCourse"
		ret = nodes.xpath("//CourseCode")
	when "SchoolInfo"
		ret = nodes.xpath("//SchoolName")
	when "SectionInfo"
		ret = nodes.xpath("//LocalId")
	when "StaffPersonal", "StudentContactPersonal", "StudentPersonal"
		ret = nodes.xpath("//PersonInfo/Name/FullName")
		ret = nodes.xpath("//PersonInfo/Name/GivenName") + " " + nodes.xpath("//PersonInfo/Name/FamilyName") if ret.nil? or ret.empty?
	when "StudentActivityInfo"
		ret = nodes.xpath("//Title")
	when "TeachingGroup"
		ret = nodes.xpath("//ShortName")
	when "TermInfo"
		ret = nodes.xpath("//TermCode")
	when "TimeTable"
		ret = nodes.xpath("//Title")
	when "TimeTableCell"
		ret = nodes.xpath("//DayId") + ":" + nodes.xpath("//PeriodId")
	when "TimeTableSubject"
		ret = nodes.xpath("//CourseLocalId") 
		ret = nodes.xpath("//SubjectShortName") if ret.nil? or ret.empty?
		ret = nodes.xpath("//SubjectLongName") if ret.nil? or ret.empty?
	when "VendorInfo"
		ret = nodes.xpath("//Name/FullName")
		ret = nodes.xpath("//Name/GivenName") + " " + nodes.xpath("//Name/FamilyName") if ret.nil? or ret.empty?
	end
	ret = id if ret.nil?  or ret.empty?
	return ret
end


