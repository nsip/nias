# Class to standardise error reporting. The ordinals and totals are used to calculate total numbers of errors for reporting
# Always pass NiasError.to_s to Poseidon error constructor

class NiasError

	def initialize(ordinal, total_errors, errorclass, message)
		@ordinal = ordinal
		@total_errors = total_errors
		@errorclass = errorclass
		@message = message
	end

	def to_s
		return "#{@ordinal}:#{@total_errors} #{@errorclass}\n#{@message}"
	end


end
