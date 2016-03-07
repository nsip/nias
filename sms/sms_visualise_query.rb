# sms_visualise_query.rb
require 'redis'
require 'hashids'
require 'date'
require 'json'
# query interface for the redis datasets to produce visualisation queries
require 'moneta'
require 'nokogiri'
require_relative '../niasconfig'

# Queries specific to NIAS visualisation. These are example queries, and production systems should use their own queries.


class SMSVizQuery

	@languagecodes = {}

	def initialize
		config = NiasConfig.new
                @redis = config.redis
                @hashid = Hashids.new( 'nsip sms_visualise_query' )
                @expiry_seconds = 120 #update this for production
                @rand_space = 10000
		@store = Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')
		@languagecodes = {'1201' => 'English', '7100' => 'Chinese, nfd', '2201' => 'Greek',
			'5203' => 'Hindi', '9601' => 'Klingon' }
        end

	# counts ids in all collections that an id is connected to
	def find_linked_collections( id )
		results = []
		collections = @redis.smembers('known:collections')
		tally = {}
		collections.each do |collection|
			datapoints = @redis.sinter id, collection
			datapoints = datapoints.reject{|x| x == id}
			if datapoints.nil? or datapoints.empty?
				# indirect links
				tmp = @hashid.encode( rand(1...999) )
                        	q = []
                        	q = @redis.smembers id
                        	next unless !q.empty? # return empty results if item not in db
                               	@redis.sunionstore tmp, q.to_a
                                results1 = @redis.sinter tmp, collection
                                @redis.expire tmp, 5
				tally[collection] = results1.size if !results1.nil? and !results.empty?
			else
				tally[collection] = datapoints.size
			end
		end
		tally.each { | key, value| results << {:collection => key, :data => value } }
		return results
	end

	# get all direct and indirect ids  that an id is connected to, and identify their collections
	def linked_collections_and_types( id )
		results = []
		nodes = {}  # maps GUIDs in graph to ordinal numbers, used to describe node connections in graph
		nodes[id] = 0
		idx = 0
		collections = @redis.smembers('known:collections')
		collections.each do |collection|
			datapoints = @redis.sinter id, collection
			datapoints = datapoints.reject{|x| x == id}
			if datapoints.nil? or datapoints.empty?
				# indirect links
				tmp = @hashid.encode( rand(1...999) )
                        	q = []
                        	#q = @redis.smembers id
 				q = @redis.sdiff id, "SchoolInfo"
                        	next unless !q.empty? # return empty results if item not in db
				q.each do |q1|
					unless nodes.has_key?(q1)
						idx+=1
						nodes[q1] = idx
					end
                                	results1 = @redis.sinter q1, collection
					results1.each do |x|
						unless nodes.has_key?(x)
							idx+=1
							nodes[x] = idx
						end
						label = @redis.hget 'labels', x # retrieve human-readable label
						# if label is GUID, fallback on collection name
						label = "[#{collection}]" if label[/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}/]
						results << { :collection => collection, :link => 'indirect', :id => x, :label => label , :origin => nodes[q1], :target => nodes[x] } unless (nodes[q1] == 0 and nodes[x] == 0)
					end
				end
			else
				# direct links
				datapoints.each do |x|
					label = @redis.hget 'labels', x  # retrieve human-readable label
					unless nodes.has_key?(x)
						idx+=1
						nodes[x] = idx
					end
					# if label is GUID, fallback on collection name
					label = "[#{collection}]" if label[/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}/]
					results << { :collection => collection, :link => 'direct', :id => x, :label => label, :origin => 0 , :target => nodes[x] } unless nodes[x] == 0
				end
			end
		end
		return results
	end


	# produce a count of attendances records for all students. Assumes that attendance is only recorded if the student was present. If that is not the case, recode the method.
	def attendance_counts
		results = {}
		students = @redis.smembers('StudentPersonal')
		students.each do |s|
			absences = @redis.sinter s, 'StudentDailyAttendance'
			results[s] = absences.size
		end
		return results
	end

	# average counts of attendance for each class
	def attendance_counts_per_class
		absences_per_class = {} # hash of class label against array of attendance counts for each student in the class
		results = []
		attendances = attendance_counts() # get attendance counts for all students
		students = attendances.keys
		classes = @redis.smembers('TeachingGroup')
		students.each do |student|
			classes1 = @redis.sinter student, 'TeachingGroup'
			classes1.each do |class1|
				label = @redis.hget 'labels', class1
				absences_per_class[label] = [] if absences_per_class[label].nil?
				absences_per_class[label] << attendances[student]
			end
		end
		absences_per_class.each do |key, value|
			results << {:class => key, "average absences" => value.inject(0.0) { |sum, el| sum + el } / value.size } # calculate average
		end
		return results
	end

	# 20 most absent students
	def most_absent_students
		attendances = attendance_counts() 
		absentees = attendances.keys.sort { |x, y| attendances[y] <=> attendances[x] }
		results = []
		absentees[0..20].each do |x|
			label = @redis.hget 'labels', x
			results << {:student => label, :absences => attendances[x] }
		end
		return results
	end



	# track invoices vs paymentreceipts for each debtor
	def payment_delinquency
		debtors = @redis.smembers('Debtor')
		labels = {}
		results = []
		# do only every third debtor, to make data more tractable to visualise
		debtors.each do |d|
			studentcontact = @redis.sinter d, 'StudentContactPersonal'
			labels[d] = @redis.hget 'labels', studentcontact[0]
			invoices = @redis.sinter d, 'Invoice'
			receipts = @redis.sinter d, 'PaymentReceipt'
			results << {:debtor => labels[d], :delinquency => invoices.size - receipts.size }
		end	
		# chop out more results at higher delinquency, to make data more tractable to visualise
		results.select! {|x| x[:delinquency]-7 < rand(5) } 
		return results.sort {|a, b| b[:delinquency] <=> a[:delinquency] }
	end

	# the foregoing queries illustrated visualisations based on Redis. Most of the object attributes are not in Redis,
	# and are accessed only in LMDB. This query illustrates visualisation based on parsing the XML in the object store.

	def language_background_against_debtors
		debtors = @redis.smembers('Debtor')
		labels = {}
		results = []
		debtors.each do |d|
			studentcontact = @redis.sinter d, 'StudentContactPersonal'
			label = @redis.hget 'labels', studentcontact[0]
			studentcontact.each do |sc|
				xml = Nokogiri::XML(@store[sc])				
				xml.xpath("//xmlns:LanguageList/xmlns:Language[xmlns:LanguageType='1']/xmlns:Code").each do |x| 
					results << {:debtor => label, :language => @languagecodes[x.child.to_s] } 
				end
			end
		end
		return results
	end
end

