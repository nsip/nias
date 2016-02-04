#!/usr/bin/env ruby
require 'fileutils'

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
@pid_file = 'naplan_cluster.pid'

# check core pid file exists, if not core probably not running
if !File.exist?( @core_pid_file )
  banner 'No core.pid file found, likely core not running, run lauch_core.rb before launch.nias'
  exit 130
end


def launch

  # just in case an old one gets left behind, delete on startup
  File.delete( @pid_file ) if File.exist?( @pid_file )

  banner 'Creating topic & partitions in kafka'

  # create the demo topic and assign multiple partitions so the consumer can run
  # parallel instances one per partition
  topic = 'naplan.json.cluster'
  pid = Process.spawn( './kafka/bin/kafka-topics.sh', 
                                                      '--zookeeper localhost:2181', 
                                                      '--create',
                                                      "--topic #{topic}",
                                                      '--partitions 5',
                                                      '--replication-factor 1')
  Process.wait pid 

  
  banner 'Starting consumer cluster'


  cluster_services = ['kccluster.rb']

  cluster_services.each_with_index do | service, i |

      @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/naplan/services/#{service}" )

  end

  # write pids to file for shutdown
  File.open(@pid_file, 'w') {|f|
    @pids.each do | _, pid |
      f.puts pid
    end
  }


  banner "pid file written to #{@pid_file}"



end


def shut_down

    banner "\n Cluster Services shutting down...\n\n"

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

    banner "All Cluster services shut down"

end


if ARGV[0] == '-K' then
  shut_down
  exit 130
else
  launch
  exit 130
end









