# Class to standardise error reporting. The ordinals and totals are used to calculate total numbers of errors for reporting
# Always pass NiasError.to_s to Poseidon error constructor

class NiasError

	def initialize(ordinal, total_errors, record_id, errorclass, message)
		@ordinal = ordinal # ordinal of error
		@total_errors = total_errors # total errors being reported in session. If 0, then the errors are being reported per record
		@errorclass = errorclass # class of error generator
		@record_id = record_id # if errors are being reported per record, not for the entire file upload, an arbitrary record id; else 0
		@message = message
	end

	# header : (error ordinal):(error totals for the class):(recordnumber, for error reporting per record, else 0) (errorclass)
	def to_s
		return "#{@ordinal}:#{@total_errors}:#{@record_id} #{@errorclass}\n#{@message}"
	end


end
