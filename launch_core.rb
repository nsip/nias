#!/usr/bin/env ruby
require 'fileutils'

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

  # just in case an old one gets left behind, delete on startup
  File.delete( @pid_file ) if File.exist?( @pid_file )
  
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
  # @pids['ssf'] = Process.spawn( 'ruby', './ssf/ssf_server.rb', '-e', 'production', '-p', '4567' )
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
              'sifxml.validated',
              'sms.indexer',
              'sifxml.ingest',
              'sifxml.errors',
              'oneroster.validated',
              'nsip.test',
              'naplan.sifxml',
              'naplan.csv',
              'test.test1',
              'json.test'
            ]

  topics.each do | topic |

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






