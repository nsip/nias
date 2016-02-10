#!/usr/bin/env ruby
require 'fileutils'

=begin
Launcher of NIAS microservices specific to NAPLAN. Replaces generic launch_nias.rb. Must be run after ./launcher_core.rb

Options: 
* ./launcher_naplan.rb launches NIAS microservices for NAPLAN
* ./launcher_naplan.rb -K shuts down NIAS microservices for NAPLAN

Dependencies
1. Script expects that ./core.pid file already exists, confirming that NIAS infrastructure services have been launched.

2. Script creates a ./nias.pid file storing the process ids of NIAS microservices. 
Launch presupposes the file does not exist, and creates it.
Shutdown presupposes the file does exist, and deletes it, after shutting down each process it references.

Functionality:
1. Starts SSF microservices for NAPLAN (with NAPLAN-specific cut-down XSD)

2. Starts SMS microservices for NAPLAN
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


    banner 'Starting NIAS SSF services for NAPLAN'

    ssf_services = [
        {:name => 'cons-prod-privacyfilter.rb', :options  => ''},
        {:name =>'cons-prod-sif-ingest-validate.rb', :options => './ssf/services/xsd/sif3.4/NAPLANRegistrationDraft.xsd'},
        {:name =>'cons-prod-sif-bulk-ingest-validate.rb', :options => './ssf/services/xsd/sif3.4/NAPLANRegistrationDraft.xsd'}
    ]


    ssf_services.each_with_index do | service, i |

        @pids["#{service}:#{i}"] = Process.spawn( 'ruby', "#{__dir__}/ssf/services/#{service[:name]}", service[:options] )

    end
    

    banner 'Starting NIAS SMS services for NAPLAN'

    sms_services = [
        'cons-prod-sif-parser.rb',
        'cons-sms-indexer.rb',
        'cons-sms-storage.rb',
        'cons-sms-json-storage.rb',
        'cons-prod-sif2scv-studentpersonal-naplanreg-parser.rb',
        'cons-prod-csv2sif-studentpersonal-naplanreg-parser.rb',
        'cons-prod-csv2sif-staffpersonal-naplanreg-parser.rb',
        'cons-prod-sif2csv-staffpersonal-naplanreg-parser.rb',
        'cons-prod-sif2csv-SRM-validate.rb',
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

    banner "\n NIAS Services for NAPLAN shutting down...\n\n"

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

    banner "All NIAS services for NAPLAN shut down"

end


if ARGV[0] == '-K' then
    shut_down
    exit 130
else
    launch
    exit 130
end









