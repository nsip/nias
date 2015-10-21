require 'nokogiri' # xml support

xsd = Nokogiri::XML::Schema(File.open("SIF_Message1.3_3.x.xsd"))
doc = Nokogiri::XML(File.open("test.xml")) do |config| 
	config.nonet.noblanks
end

puts doc.errors

if(doc.errors.empty?) then 
	xsd.validate(doc).each do |error|
		puts  error.message
end
end
