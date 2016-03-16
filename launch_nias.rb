#!/usr/bin/env ruby
require 'fileutils'
require_relative './niasconfig'

=begin
Launcher of NIAS microservices. Must be run after ./launcher_core.rb

Options: 
* ./launcher_nias.rb launches NIAS microservices
* ./launcher_nias.rb -K shuts down NIAS microservices

Dependencies
1. Script expects that ./core.pid file already exists, confirming that NIAS infrastructure services have been launched.

2. Script creates a ./nias.pid file storing the process ids of NIAS microservices. 
Launch presupposes the file does not exist, and creates it.
Shutdown presupposes the file does exist, and deletes it, after shutting down each process it references.

Functionality:

1. Creates all the Kafka topics that are known in advance to be required. There is latency in auto-creating Kafka topics, which can lead to messages being dropped.

2. Starts SSF microservices

1. Starts SMS microservices
=end

puts "\n\nStarting in: #{__dir__}\n\n"

def banner( text )

    puts "\n\n********************************************************"
    puts "**"
    puts "**                #{text}"
    puts "**"
    puts "**"
    puts "********************************************************\n\n"

end



@pids = {}
@core_pid_file = 'core.pid'
@pid_file = 'nias.pid'

# check core pid file exists, if not core probably not running
if !File.exist?( @core_pid_file )
    banner 'No core.pid file found, likely core not running, run lauch_core.rb before launch.nias'
    exit 130
end


def launch
    if ( File.exist?( @pid_file) ) then
        puts "The file #{@pid_file} exists: run ./launch_nias.rb -K to terminate any existing processes"
        exit
    end

  banner 'Creating known topics'

    # slight wait to ensure kafka is initialised
  # otherwise topics may fail to create - will be created dynamically when called, but with  
  # startup overhead that is better dealt with here.
  #sleep 5

  config = NiasConfig.new

  # see if any of the topics have already been created
  topic_describe = ` ./kafka/bin/kafka-topics.sh  --describe  --zookeeper #{config.zookeeper} `

  topics = [
        'sifxml.validated',
        'sifxml.processed',
        'sms.indexer',
        'sifxml.ingest',
        'sifxml.errors',
        'csv.errors',
        'oneroster.validated',
        'nsip.test',
        'test.test1',
        'json.test',
        'rspec.test',
        'json.storage',
            ]

  sleep 5 # creating too early truncates topics
  topics.each do | topic |
    unless topic_describe[/Topic:#{topic}/]
    puts "Creating #{topic}"
    pid = Process.spawn( './kafka/bin/kafka-topics.sh',
                                                      "--zookeeper #{config.zookeeper}",
                                                      '--create',
                                                      "--topic #{topic}",
                                                      '--partitions 5',
                                                      '--replication-factor 1')
    Process.wait pid
    end
  end

  # topics that are not idempotent -- must keep messages in order
  topics_single = [
        'sifxml.bulkingest',
  ]
  topics_single.each do | topic |
    unless topic_describe[/Topic:#{topic}/]
    puts "Creating #{topic}"
    pid = Process.spawn( './kafka/bin/kafka-topics.sh',
                                                      "--zookeeper #{config.zookeeper}",
                                                      '--create',
                                                      "--topic #{topic}",
                                                      '--partitions 1',
                                                      '--replication-factor 1')
    Process.wait pid
    end
  end


  banner 'Core topics created.'



    banner 'Starting NIAS SSF services'

    ssf_services = [
        {:name => 'cons-prod-privacyfilter.rb', :options  => ''},
        {:name =>'cons-prod-sif-ingest-validate.rb', :options => './ssf/services/xsd/sif3.4/SIF_Message3.4.xsd'},
        {:name =>'cons-prod-sif-bulk-ingest-validate.rb', :options => './ssf/services/xsd/sif3.4/SIF_Message3.4.xsd'},
        {:name => 'cons-prod-sif-process.rb', :options  => ''},
    ]


    ssf_services.each_with_index do | service, i |

        @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/ssf/services/#{service[:name]}", service[:options] )

    end
    

    banner 'Starting NIAS SMS services'

    sms_services = [
	{:name => 'cons-prod-sif-parser.rb', :options => ''},
	{:name => 'cons-sms-indexer.rb', :options => ''},
	{:name => 'cons-sms-storage.rb', :options => ''},
	{:name => 'cons-sms-json-storage.rb', :options => ''},
	{:name => 'cons-oneroster-sms-storage.rb', :options => ''}, 
	{:name => 'cons-prod-oneroster-parser.rb', :options => ''},
    ]

    sms_services.each_with_index do | service, i |

        @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/sms/services/#{service[:name]}", service[:options] )

    end

  # Web services depend on Kafka queues
  banner 'Starting Web Services'
  @pids['web'] = Process.spawn( "rackup --host #{config.get_host}" )

  banner "Web services running on #{config.get_host}:#{config.get_sinatra_port}/"


    # write pids to file for shutdown
    File.open(@pid_file, 'w') {|f|
        @pids.each do | _, pid |
            f.puts pid
        end
    }


    banner "pid file written to #{@pid_file}"



end


def shut_down

    banner "\n NIAS Services shutting down...\n\n"

    File.readlines( @pid_file ).each do |line|
        begin
            Process.kill :INT, line.chomp.to_i
            sleep 2
        rescue Exception => e  
            puts e.message  
            puts e.backtrace.inspect  
        end

    end

    File.delete( @pid_file ) if File.exist?( @pid_file )

    banner "All NIAS services shut down"

end


if ARGV[0] == '-K' then
    shut_down
    exit 130
else
    launch
    exit 130
end









