# nias_server.rb

require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/contrib'

# tiny web service to act as frontpage to nias web services
class NIASServer < Sinatra::Base

    helpers Sinatra::ContentFor

    get "/" do

        erb :nias

    end


    get "/nias" do

        erb :nias

    end


end # end of sinatra wrapper class
