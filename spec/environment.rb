require "rubygems"
require "bundler"

Bundler.require(:default)                   # load all the default gems
Bundler.require(Sinatra::Base.environment)  # load all the environment specific gems

#require "active_support/deprecation"
#require "active_support/all"

require_relative '../ssf/ssf_server'
require_relative '../ssf/nias_server'
require_relative '../ssf/sif_privacy_server'
require_relative '../ssf/hookup_ids_server.rb'
require_relative '../ssf/equiv_ids_server.rb'
require_relative '../ssf/filtered_client.rb'
require_relative '../sms/sms_query_server'
require_relative '../sms/graph_server.rb'




