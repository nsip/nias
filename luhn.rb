# https://gist.github.com/henrik/1409815 Henrik Nyh

class Luhn
    def self.checksum(number)
        digits = number.to_s.reverse.scan(/\d/).map { |x| x.to_i }
        digits = digits.each_with_index.map { |d, i|
            d *= 2 if i.even?
            d > 9 ? d - 9 : d
        }
        sum = digits.inject(0) { |m, x| m + x }
        mod = 10 - sum % 10
        mod==10 ? 0 : mod
    end

    def self.validate(number)
        checkdigit = number % 10
        checkdigit1 = self.checksum(number / 10)
        return checkdigit == checkdigit1
    end

    def self.add_checksum(number)
        return number * 10 + self.checksum(number)   
    end
end
