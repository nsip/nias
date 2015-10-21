#!/usr/bin/env ruby
require 'fileutils'

puts "\n\nStarting in: #{File.expand_path(File.dirname(__FILE__))}\n\n"

dir = File.expand_path(File.dirname(__FILE__))

# create log area for redis - directory needs to pre-exist
FileUtils.mkdir '/tmp/redis' unless File.directory? '/tmp/redis'

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

  banner 'Starting SSF server'
  @pids['ssf'] = Process.spawn( 'ruby', './ssf/ssf_server.rb', '-e', 'production', '-p', '4567' )
  banner 'SSF server running on localhost:4567/'  

  banner 'Kafka logs will be created under /tmp/kafka'
  banner 'Zookeeper logs will be created under /tmp/zookeeper'
  banner 'Redis backups will be created under /tmp/redis'

  File.open(@pid_file, 'w') {|f| 
    f.puts "#{@pids['kafka']}"
    f.puts "#{@pids['zk']}"
    f.puts "#{@pids['sms-redis']}"
    f.puts "#{@pids['ssf']}"
  }

  banner "pid file written to #{@pid_file}"


end


def shut_down

    banner "\n Core Services shutting down...\n\n"

    File.readlines( @pid_file ).each do |line|

        Process.kill :INT, line.chomp.to_i
        sleep 2

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






