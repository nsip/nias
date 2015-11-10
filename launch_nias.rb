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
@pid_file = 'nias.pid'

# check core pid file exists, if not core probably not running
if !File.exist?( @core_pid_file )
  banner 'No core.pid file found, likely core not running, run lauch_core.rb before launch.nias'
  exit 130
end


def launch

  # just in case an old one gets left behind, delete on startup
  File.delete( @pid_file ) if File.exist?( @pid_file )

  banner 'Starting NIAS SSF services'

  ssf_services = [
                {:name => 'cons-prod-privacyfilter.rb', :options  => ''},
		            # {:name => 'filtered_client.rb', :options => '-p 1234'},
                {:name =>'cons-prod-sif-ingest-validate.rb', :options => ''}

              ]


  ssf_services.each_with_index do | service, i |

      @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/ssf/services/#{service[:name]}", service[:options] )

  end
  


  banner 'Starting NIAS SMS services'

  sms_services = [
                  'cons-prod-sif-parser.rb',
                  'cons-sms-indexer.rb',
                  'cons-sms-storage.rb',
                  'cons-oneroster-sms-storage.rb', 
                  'cons-prod-oneroster-parser.rb'
                ]

  sms_services.each_with_index do | service, i |

      @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/sms/services/#{service}" )

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









