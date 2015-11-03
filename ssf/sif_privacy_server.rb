# sif_privacy_server.rb
require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'json'
require 'csv'
require 'hashids' # temp non-colliding client & producer id generator
require 'nokogiri' # xml support



# tiny web service to handle the editing and maintenance of privacy profiles
class SPSServer < Sinatra::Base


	get "/sps" do
		@profile = params['profile']
		if @profile.nil? 
			# List known privacy profiles
			@d = Dir["#{__dir__}/services/privacyfilters/*.xpath"].map{|x| x[/\/([^\/]+).xpath$/, 1]}
			erb :privacyfilters
		else
			# Edit privacy profile named
			@file = File.open("#{__dir__}/services/privacyfilters/#{@profile}.xpath", "r")
			@contents = @file.read
			@file.close
			erb :create
		end
	end

	# receive edit of privacy profile
	post "/privacy" do
		@profile = params['profile']
		if !@profile.nil?
			@logfile = File.open("#{__dir__}/services/privacyfilters/#{@profile}.xpath","w")
	    		@logfile.truncate(@logfile.size)
	    		@logfile.write(params[:file])
	    		@logfile.close
	    		redirect '/sps'
		end
	end


end # end of sinatra wrapper class
