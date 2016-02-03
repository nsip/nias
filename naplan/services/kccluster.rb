# kccluster.rb
require 'kafka-consumer'
require 'nokogiri'
require 'json'

zookeeper = "localhost:2181"
name      = "kccon"
topics    = ["naplan.json.cluster"]

consumer = Kafka::Consumer.new(name, topics, zookeeper: zookeeper)

@xsd = Nokogiri::XML::Schema(File.open( '../ssf/services/xsd/sif3.4/SIF_Message3.4.xsd' ))


Signal.trap("INT") { consumer.interrupt }

consumer.each do |message|
  # process message
			
			counter = counter + 1

			msg_hash = JSON.parse( message.value )
		    
		    
		    # get xml content
		    xml_doc = Nokogiri::XML( msg_hash['sif_xml'] )


		    # structure validation
		    if !xml_doc.errors.empty?
		    	puts xml_doc.errors
		    end
		    
		    # schema validation
		    # xsd_errors = @xsd.validate( xml_doc )
		    if !xsd_errors.empty?
		    	puts xsd_errors
		    end

  
end







