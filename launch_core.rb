#!/usr/bin/env ruby
require 'fileutils'

=begin
Launcher of NIAS infrastructure. Must be run before ./launcher_nias.rb

Options: 
* ./launcher_core.rb launches NIAS infrastructure
* ./launcher_core.rb -K shuts down NIAS infrastructure

Dependencies
Script creates a ./core.pid file storing the process ids of NIAS infrastructure services. 
Launch presupposes the file does not exist, and creates it.
Shutdown presupposes the file does exist, and deletes it, after shutting down each process it references.

Functionality:
1. Creates /tmp/nias, with subdirectories redis, moneta. Subdirectory kafka-logs is created through Kafka config

2. Launches the following infrastructure service processes:
* Zookeeper
* Kafka
* Redis
* Rackup

3. Creates all the Kafka topics that are known in advance to be required. There is latency in auto-creating Kafka topics, which can lead to messages being dropped.
=end

puts "\n\nStarting in: #{__dir__}\n\n"

# create root node for data files in /tmp
FileUtils.mkdir '/tmp/nias' unless File.directory? '/tmp/nias'

# create storage area for redis - directory needs to pre-exist
FileUtils.mkdir '/tmp/nias/redis' unless File.directory? '/tmp/nias/redis'

# create working area for moneta key-value store - needs to pre-exist
FileUtils.mkdir '/tmp/nias/moneta' unless File.directory? '/tmp/nias/moneta'

def banner( text )

    puts "\n\n********************************************************"
    puts "**"
    puts "**                #{text}"
    puts "**"
    puts "**"
    puts "********************************************************\n\n"

end



@pids = {}
@pid_file = 'core.pid'

def launch

    # if an old pid file exists, abort, should be running -K instead
    if ( File.exist?( @pid_file) ) then
        puts "The file #{@pid_file} exists: run ./launch_core.rb -K to terminate any existing processes"
        exit
    end
        # daemonise launched processes
    Process.daemon( true, true )

    banner 'Starting zookeeper' 
    @pids['zk'] = Process.spawn( './kafka/bin/zookeeper-server-start.sh', './kafka/config/zookeeper.properties' )

    banner 'Waiting for ZK to come up'
    sleep 5


    banner 'Starting kafka' 
    @pids['kafka'] = Process.spawn( './kafka/bin/kafka-server-start.sh', './kafka/config/server.properties' )


    banner 'Starting SMS Redis'
    @pids['sms-redis'] = Process.spawn( 'redis-server', './sms/sms-redis.conf' )

    banner 'Starting Web Services'
    @pids['web'] = Process.spawn( 'rackup' )

    banner 'Web services running on localhost:9292/'  


    banner 'Kafka logs will be created under /tmp/nias/kafka'
    banner 'Zookeeper logs will be created under /tmp/nias/zookeeper'
    banner 'Redis backups will be created under /tmp/nias/redis'
    banner 'SIF Key Value store will be created under /tmp/nias/moneta'

    File.open(@pid_file, 'w') {|f| 
        f.puts "#{@pids['kafka']}"
        f.puts "#{@pids['zk']}"
        f.puts "#{@pids['sms-redis']}"
        f.puts "#{@pids['web']}"
    }

    banner "pid file written to #{@pid_file}"

    banner 'Creating known topics'

    topics = [
=begin
        'sifxml.validated',
        'sms.indexer',
        'sifxml.ingest',
        'sifxml.bulkingest',
        'sifxml.errors',
        'csv.errors',
        'oneroster.validated',
        'nsip.test',
        'naplan.sifxml',
        'naplan.sifxml.none',
        'naplan.sifxml.low',
        'naplan.sifxml.medium',
        'naplan.sifxml.high',
        'naplan.sifxml.extreme',
        'naplan.sifxmlout',
=end
        'naplan.csv',
=begin
        'naplan.csvstudents',
        'naplan.csv_staff',
        'naplan.csvstaff',
        'naplan.sifxml_staff',
        'naplan.sifxml_staff.none',
        'naplan.sifxml_staff.low',
        'naplan.sifxml_staff.medium',
        'naplan.sifxml_staff.high',
        'naplan.sifxml_staff.extreme',
        'naplan.sifxmlout_staff',
        'naplan.csvstaff_out',
        'test.test1',
        'json.test',
        'rspec.test',
        'json.storage',
=end
    ]

    sleep 5 # creating too early can lead to some topics being skipped
    topics.each do | topic |

        puts "Creating #{topic}"
        pid = Process.spawn( './kafka/bin/kafka-topics.sh', 
            '--zookeeper localhost:2181', 
            '--create',
            "--topic #{topic}",
            '--partitions 1',
        '--replication-factor 1')
        Process.wait pid 
            end

    banner 'Core topics created.'

    banner 'Core NIAS services are up.'

end


def shut_down
    banner "\n Core Services shutting down...\n\n"

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

    banner "All core services shut down"

end


if ARGV[0] == '-K' then
    shut_down
    exit 130
else
    launch
    exit 130
end






