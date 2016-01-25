# sif_privacy_server.rb
require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/content_for'
require 'json'
require 'csv'
require 'hashids' # temp non-colliding client & producer id generator
require 'nokogiri' # xml support



# tiny web service to handle the editing and maintenance of privacy profiles
class SPSServer < Sinatra::Base

	helpers Sinatra::ContentFor

	get "/sps" do

		# Expose the contents of each privacy profile for editing
		@profiles = {}
		['low','medium','high','extreme'].each do | prf_name |

			file = File.open("#{__dir__}/services/privacyfilters/#{prf_name}.xpath", "r")
			contents = file.read
			file.close
			@profiles[prf_name] = contents

		end

		erb :privacyfilters

	end

	# receive edit of privacy profile
	post "/privacy" do
		@profile = params['profile']
		if !@profile.nil?
			@logfile = File.open("#{__dir__}/services/privacyfilters/#{@profile}.xpath","w")
	    		@logfile.truncate(@logfile.size)
	    		@logfile.write(params[:file])
	    		@logfile.close
	    		puts "\n\nPrivacy Profile #{@profile} has been updated\n\n"
		end
	end


end # end of sinatra wrapper class
