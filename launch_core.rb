#!/usr/bin/env ruby
require 'fileutils'
require_relative './niasconfig'

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
@PID_FILE = 'core.pid'

def launch

  config = NiasConfig.new()
puts config.get_host
  # just in case an old one gets left behind, delete on startup
  #File.delete( @pid_file ) if File.exist?( @pid_file )
  # if an old pid file exists, abort, should be running -K instead
  if ( File.exist?( @PID_FILE) ) then
    puts "The file #{@PID_FILE} exists: run ./launch_core.rb -K to terminate any existing processes"
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
  # @pids['ssf'] = Process.spawn( 'ruby', './ssf/ssf_server.rb', '-e', 'production', '-p', '4567' )
  @pids['web'] = Process.spawn( 'rackup' )

  banner "Web services running on #{config.get_host}:#{config.get_sinatra_port}/"  


  banner 'Kafka logs will be created under /tmp/nias/kafka'
  banner 'Zookeeper logs will be created under /tmp/nias/zookeeper'
  banner 'Redis backups will be created under /tmp/nias/redis'
  banner 'SIF Key Value store will be created under /tmp/nias/moneta'

  File.open(@PID_FILE, 'w') {|f| 
    f.puts "#{@pids['kafka']}"
    f.puts "#{@pids['zk']}"
    f.puts "#{@pids['sms-redis']}"
    f.puts "#{@pids['web']}"
  }

  banner "pid file written to #{@PID_FILE}"

  banner 'Core NIAS services are up.'

end


def shut_down
    banner "\n Core Services shutting down...\n\n"

    File.readlines( @PID_FILE ).each do |line|
        begin
            Process.kill :INT, line.chomp.to_i
            sleep 2
        rescue Exception => e  
            puts e.message  
            puts e.backtrace.inspect  
        end
    end

    File.delete( @PID_FILE ) if File.exist?( @PID_FILE )

    banner "All core services shut down"

end


if ARGV[0] == '-K' then
    shut_down
    exit 130
else
    launch
    exit 130
end






