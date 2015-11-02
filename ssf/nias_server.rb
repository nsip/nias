# nias_server.rb

require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/base'


# tiny web service to act as frontpage to nias web services
class NIASServer < Sinatra::Base


	get "/" do

		erb :nias

	end


	get "/nias" do

		erb :nias

	end


end # end of sinatra wrapper class