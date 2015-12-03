# graph_server.rb

require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/contrib'
require 'json'

require_relative 'sms_visualise_query'


class GraphServer < Sinatra::Base

	helpers Sinatra::ContentFor

	get "/las" do

		erb :las
		
	end


	get "/graph_data/linked_collections" do
		id = params['id']
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.find_linked_collections( id )
		return results.to_json
	end

	get "/graph_data/attendance_per_class" do
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.attendance_counts_per_class
		return results.to_json
	end

	get "/graph_data/most_absent_students" do
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.most_absent_students
		return results.to_json
	end

	get "/graph_data/payment_delinquency" do
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.payment_delinquency
		return results.to_json
	end

	get "/graph_data/debtor_languages" do
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.language_background_against_debtors
		return results.to_json
	end

	get "/graph_data/local_network" do
		id = params['id']
		viz = SMSVizQuery.new
	        content_type 'application/json'
		results = viz.linked_collections_and_types( id )
		return results.to_json
	end

end
