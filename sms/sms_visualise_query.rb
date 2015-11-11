# sms_visualise_query.rb
require 'redis'
require 'hashids'
require 'date'
require 'json'
# query interface for the redis datasets to produce visualisation queries

class SMSVizQuery

	def initialize
                @redis = Redis.new(:url => 'redis://localhost:6381', :driver => :hiredis)
                @hashid = Hashids.new( 'nsip sms_visualise_query' )
                @expiry_seconds = 120 #update this for production
                @rand_space = 10000
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


	# produce a count of attendances for all students
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
		absences_per_class = {}
		results = []
		attendances = attendance_counts()
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
			results << {:class => key, "average absences" => value.inject(0.0) { |sum, el| sum + el } / value.size }
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
		debtors.each do |d|
			studentcontact = @redis.sinter d, 'StudentContactPersonal'
			labels[d] = @redis.hget 'labels', studentcontact[0]
			invoices = @redis.sinter d, 'Invoice'
			receipts = @redis.sinter d, 'PaymentReceipt'
			results << {:debtor => labels[d], :delinquency => invoices.size - receipts.size }
		end
		return results.sort {|a, b| b[:delinquency] <=> a[:delinquency] }
	end
end

