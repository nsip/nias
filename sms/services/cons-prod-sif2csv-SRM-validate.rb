# cons-prod-sif2csv-SRM-validate.rb

# Consumer that replicates the validation rules of the NAPLAN Student Registration Management System
# Assumes that records are already validated against default SIF/XML schema. Pushes errors to sifxml.errors, and to
# csv.errors if the file was originally CSV

require 'json'
require 'nokogiri'
require 'poseidon'
require 'poseidon_cluster' # to track offset, which seems to get lost for bulk data
require 'hashids'
require 'csv'
require_relative 'cvsheaders-naplan'

@inbound = 'sifxml.processed'
@outbound1 = 'sifxml.errors'
@outbound2 = 'csv.errors'

@servicename = 'cons-prod-sif2csv-staffpersonal-naplanreg-parser'

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

@naplan_topics = %w(naplan.sifxml naplan.sifxml_staff naplan.sifxmlout naplan.sifxmlout_staff)

# state or territory for which validation is specific, passed as command line option
@stateterritory = ARGF.argv[0] || nil
@stateterritory = nil if @stateterritory.empty?
@asl_ids = Set.new

# read in ASL file as csv
CSV.foreach(File.path("#{__dir__}/asl_schools.csv"), :headers => true) do |row|
	next if(!@stateterritory.nil? and not (row['State'] == @stateterritorry))
	# if no state specified, record all ACARA IDs; if state specific, record only those for the current state
	@asl_ids << row['ACARA ID']
end
#puts @asl_ids.inspect

def validate_staff(nodes)
	ret = []
       	emailaddress = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:EmailList/xmlns:Email")
       	additionalinfo = CSVHeaders.lookup_xpath(nodes, "//xmlns:SIF_ExtendedElements/xmlns:SIF_ExtendedElement[@Name = 'AdditionalInfo']")
	ret << "Error: Email Address #{emailaddress.to_s} is malformed" if emailaddress and not emailaddress.to_s.match(/^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-zA-Z][a-zA-Z]+$/)
	if(additionalinfo)
		unless additionalinfo.to_s.match(/^[ynYN]$/)
			ret << "Error: Addition Info #{additionalinfo.to_s} is not Y or N"
		end
	end
       	localid = CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId")
	ret << "Error: 'LocalId is mandatory" unless localid
	return ret
end

def valid_birthdate(yearlevel, birthdatestr)
                        thisyear = Date.today.year
                        startdate = Date.new(thisyear-yearlevel-6,1,1)
                        enddate = Date.new(thisyear-yearlevel-5,7,31)
                        birthdate = Date.parse(birthdatestr)
                        return (birthdate >= startdate and birthdate <= enddate) 
end

def validate_student(nodes)
	ret = []
        testlevel = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:TestLevel/xmlns:Code")
	ret << "Error: '#{testlevel}' is not a valid value for TestLevel: expect 3, 5, 7, 9" if testlevel and not testlevel.to_s.match(/^[3579]$/)
	#ret << "Error: 'TestLevel is mandatory" unless testlevel

        sex = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:Sex")
	#ret << "Error: 'Sex is mandatory" unless sex

        birthdatestr = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:BirthDate")
	#ret << "Error: 'BirthDate is mandatory" unless birthdate

	studentCountryOfBirth = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:CountryOfBirth")
	#ret << "Error: StudentCountryOfBirth is mandatory" unless studentCountryOfBirth

        yearlevel_string = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:YearLevel/xmlns:Code")
	#ret << "Error: YearLevel is mandatory" unless yearlevel_string

	begin
		yearlevel = Integer(yearlevel_string.to_s)
	rescue ArgumentError
		yearlevel = 0
	end

	if(yearlevel_string and testlevel)
		case yearlevel_string.to_s
			when '3'
				ret << "Error: School Year level '#{yearlevel_string}' does not match Test Level '#{testlevel}'" unless testlevel.to_s == '3'
			when '5'
				ret << "Error: School Year level '#{yearlevel_string}' does not match Test Level '#{testlevel}'" unless testlevel.to_s == '5'
			when '7'
				ret << "Error: School Year level '#{yearlevel_string}' does not match Test Level '#{testlevel}'" unless testlevel.to_s == '7'
			when '9'
				ret << "Error: School Year level '#{yearlevel_string}' does not match Test Level '#{testlevel}'" unless testlevel.to_s == '9'
			when 'UG'
				ret << "Warning: School Year level is Ungraded"
			else
				ret << "Error: School Year level '#{yearlevel_string}' is not appropriate for NAPLAN"
		end
	end


	# numeric year level?
	if(yearlevel_string and birthdatestr and yearlevel)
		if(yearlevel >= 1 and yearlevel <= 12)
			unless valid_birthdate(yearlevel, birthdatestr.to_s)
				ret << "Warning: Date of Birth '#{birthdatestr} is inconsistent with Year Level #{yearlevel}" 
			end
		end
	end
	if(birthdatestr and yearlevel_string.to_s == 'UG' and testlevel)
		begin
			testlevelint = Integer(testlevel.to_s)
		rescue ArgumentError
			testlevelint = 0
		end
		if(testlevelint)
			unless valid_birthdate(testlevelint, birthdatestr.to_s)
				ret << "Warning: Date of Birth '#{birthdatestr} is inconsistent with Test Level #{testlevel}" 
			end
		end
	end

       	postcode = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:PostalCode")
	ret << "Error: '#{postcode}' is not a valid value for Postcode: expect four digits" if postcode and not postcode.to_s.match(/^\d\d\d\d$/)
        
	stateprovince = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:StateProvince")
	ret << "Error: '#{stateprovince}' is not a valid value for State/Province: expect standard abbreviation" if stateprovince and not stateprovince.to_s.match(/^(NSW|VIC|ACT|TAS|SA|WA|QLD|NT)$/)

        localid = CSVHeaders.lookup_xpath(nodes, "//xmlns:LocalId")
	ret << "Error: LocalId '#{localid}' is too long" if localid and localid.to_s.length > 15

        sectorId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'SectorStudentId']")
	ret << "Error: SectorId '#{sectorId}' is too long" if sectorId and sectorId.to_s.length > 15

        diocesanId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'DiocesanStudentId']")
	ret << "Error: DiocesanId '#{diocesanId}' is too long" if diocesanId and diocesanId.to_s.length > 15

        otherId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'OtherStudentId']")
	ret << "Error: OtherId '#{otherId}' is too long" if otherId and localid.to_s.length > 15

        tAAId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'TAAStudentId']")
	ret << "Error: TAAId '#{tAAId}' is too long" if tAAId and tAAId.to_s.length > 15

        stateProvinceId = CSVHeaders.lookup_xpath(nodes, "//xmlns:StateProvinceId")
	ret << "Error: StateProvinceId '#{stateProvinceId}' is too long" if stateProvinceId and stateProvinceId.to_s.length > 15

        nationalId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NationalStudentId']")
	ret << "Error: NationalId '#{nationalId}' is too long" if nationalId and nationalId.to_s.length > 15

        platformId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'NAPPlatformStudentId']")
	ret << "Error: PlatformId '#{platformId}' is too long" if platformId and platformId.to_s.length > 15

        previousLocalId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousLocalSchoolStudentId']")
	ret << "Error: PreviousLocalId '#{previousLocalId}' is too long" if previousLocalId and previousLocalId.to_s.length > 15

        previousSectorId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousSectorStudentId']")
	ret << "Error: PreviousSectorId '#{previousSectorId}' is too long" if previousSectorId and previousSectorId.to_s.length > 15

        previousDiocesanId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousDiocesanStudentId']")
	ret << "Error: PreviousDiocesanId '#{previousDiocesanId}' is too long" if previousDiocesanId and previousDiocesanId.to_s.length > 15

        previousOtherId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousOtherStudentId']")
	ret << "Error: PreviousOtherId '#{previousOtherId}' is too long" if previousOtherId and previousOtherId.to_s.length > 15

        previousTAAId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousTAAStudentId']")
	ret << "Error: PreviousTAAId '#{previousTAAId}' is too long" if previousTAAId and previousTAAId.to_s.length > 15

        previousStateProvinceId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousStateProvinceId']")
	ret << "Error: PreviousStateProvinceId '#{previousStateProvinceId}' is too long" if previousStateProvinceId and previousStateProvinceId.to_s.length > 15

        previousNationalId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNationalStudentId']")
	ret << "Error: PreviousNationalId '#{previousNationalId}' is too long" if previousNationalId and previousNationalId.to_s.length > 15

        previousPlatformId = CSVHeaders.lookup_xpath(nodes, "//xmlns:OtherIdList/xmlns:OtherId[@Type = 'PreviousNAPPlatformStudentId']")
	ret << "Error: PreviousPlatformId '#{previousPlatformId}' is too long" if previousPlatformId and previousPlatformId.to_s.length > 15


            familyName = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:FamilyName")
	    ret << "Error: FamilyName '#{familyName}' is too long" if familyName and familyName.to_s.length > 40
	    #ret << "Error: 'FamilyName is mandatory" unless familyName 

            givenName = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:GivenName")
	    ret << "Error: GivenName '#{givenName}' is too long" if givenName and givenName.to_s.length > 40
	    #ret << "Error: 'GivenName is mandatory" unless givenName 

            preferredName = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:PreferredGivenName")
	    ret << "Error: PreferredName '#{preferredName}' is too long" if preferredName and preferredName.to_s.length > 40

            middleName = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Name/xmlns:MiddleName")
	    ret << "Error: MiddleName '#{middleName}' is too long" if middleName and middleName.to_s.length > 40

            visaCode = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:VisaSubClass")
	    ret << "Error: VisaCode '#{visaCode}' is too long" if visaCode and visaCode.to_s.length > 3
	    ret << "Error: VisaCode '#{visaCode}' is wrong format" unless visaCode and visaCode.to_s.match(/^\d\d\d$/)

	    ffpos = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:FFPOS")
	    #ret << "Error: 'FullFeePayingStudent is mandatory" unless ffpos

	    indigenousStatus = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:IndigenousStatus")
	    #ret << "Error: 'IndigenousStatus is mandatory" unless indigenousStatus

	    studentLOTE = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:Demographics/xmlns:LanguageList/xmlns:Language[xmlns:LanguageType = 4]/xmlns:Code")
	    #ret << "Error: 'StudentLOTE is mandatory" unless studentLOTE

            homegroup = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Homegroup")
	    ret << "Error: Homegroup '#{homegroup}' is too long" if homegroup and homegroup.to_s.length > 10

            classcode = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ClassCode")
	    ret << "Error: ClassCode '#{classcode}' is too long" if classcode and classcode.to_s.length > 10

            aslSchoolId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolACARAId")
	    ret << "Error: ASLSchoolId '#{aslSchoolId}' is too long" if aslSchoolId and aslSchoolId.to_s.length > 5
	    #ret << "Error: 'ASLSchoolId is mandatory" unless aslSchoolId
	    ret << "Error: ASLSchoolId '#{aslSchoolId}' is not recognised" if !@stateprovince and !@asl_ids.include?(aslSchoolId.to_s)
	    ret << "Error: ASLSchoolId '#{aslSchoolId}' is not recognised for this state" if @stateprovince and !@asl_ids.include?(aslSchoolId.to_s)

            schoolLocalId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:SchoolLocalId")
	    ret << "Error: SchoolLocalId '#{schoolLocalId}' is too long" if schoolLocalId and schoolLocalId.to_s.length > 9

            localCampusId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:LocalCampusId")
	    ret << "Error: LocalCampusId '#{localCampusId}' is too long" if localCampusId and localCampusId.to_s.length > 9

            otherSchoolId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:OtherEnrollmentSchoolACARAId")
	    ret << "Error: OtherSchoolId '#{otherSchoolId}' is too long" if otherSchoolId and otherSchoolId.to_s.length > 9

            reportingSchoolId = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:ReportingSchoolId")
	    ret << "Error: ReportingSchoolId '#{reportingSchoolId}' is too long" if reportingSchoolId and reportingSchoolId.to_s.length > 9

            parent1SchoolEducation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1SchoolEducationLevel")
	    #ret << "Error: 'Parent1SchoolEducation is mandatory" unless parent1SchoolEducation

            parent2SchoolEducation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2SchoolEducationLevel")
	    #ret << "Error: 'Parent2SchoolEducation is mandatory" unless parent2SchoolEducation

            parent1NonSchoolEducation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1NonSchoolEducation")
	    #ret << "Error: 'Parent1NonSchoolEducation is mandatory" unless parent1NonSchoolEducation

            parent2NonSchoolEducation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2NonSchoolEducation")
	    #ret << "Error: 'Parent2NonSchoolEducation is mandatory" unless parent2NonSchoolEducation

            parent1Occupation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1EmploymentType")
	    #ret << "Error: 'Parent1Occupation is mandatory" unless parent1Occupation

            parent2Occupation = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2EmploymentType")
	    #ret << "Error: 'Parent2Occupation is mandatory" unless parent2Occupation

            parent1LOTE = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent1Language")
	    #ret << "Error: 'Parent1LOTE is mandatory" unless parent1LOTE

            parent2LOTE = CSVHeaders.lookup_xpath(nodes, "//xmlns:MostRecent/xmlns:Parent2Language")
	    #ret << "Error: 'Parent2LOTE is mandatory" unless parent2LOTE

            addressLine1 = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line1")
	    ret << "Error: AddressLine1 '#{addressLine1}' is too long" if addressLine1 and addressLine1.to_s.length > 40

            addressLine2 = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:Street/xmlns:Line2")
	    ret << "Error: AddressLine2 '#{addressLine2}' is too long" if addressLine2 and addressLine2.to_s.length > 40

            locality = CSVHeaders.lookup_xpath(nodes, "//xmlns:PersonInfo/xmlns:AddressList/xmlns:Address[@Role = '012B']/xmlns:City")
	    ret << "Error: Locality '#{locality}' is too long" if locality and locality.to_s.length > 40

	return ret
end


loop do

    begin
        messages = []
        outbound_messages = []
        messages = consumer.fetch

        messages.each do |m|

	    header = m.value.lines[0]
            topic = header.chomp.gsub(/TOPIC: /,"")
            payload = m.value.lines[1..-1].join
            # we are only interested in XML in NAPLAN topics
            next unless @naplan_topics.grep(topic) 
            fromcsv = payload["<!-- CSV line"]
	    csvline = payload[/<!-- CSV line (\d+) /, 1]
            csvcontent = payload[/<!-- CSV content (.+) -->/, 1]

            # read xml message
            nodes = Nokogiri::XML( payload ) do |config|
                config.nonet.noblanks
            end      		

            type = nodes.root.name
            next unless type == 'StaffPersonal' or type == 'StudentPersonal'
            errors = validate_staff(nodes) if type == 'StaffPersonal'
            errors = validate_student(nodes) if type == 'StudentPersonal'

	    errors.each do |e|
            	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound1}", e + "\n" + payload, "invalid" )
            	outbound_messages << Poseidon::MessageToSend.new( "#{@outbound2}", "CSV line #{csvline}: " + e + "\n" + csvcontent, "invalid" ) if fromcsv
	    end
        end
        # send results to error streams
        outbound_messages.each_slice(20) do | batch |
            #puts batch[0].value.lines[0..10].join("\n") + "\n\n" unless batch.empty?
            @pool.next.send_messages( batch )
        end
        #end


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

