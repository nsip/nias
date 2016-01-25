# graph_server.rb

require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/contrib'
require 'json'

require_relative 'sms_visualise_query'

=begin
Web client to return results of visualisation queries in JSON. Supported queries are:
* /graph_data/linked_collections?id=xxx : counts ids in all collections that an id is connected to
* /graph_data/attendance_per_class : give list of attendance numbers per class
* /graph_data/most_absent_students : give list of students with the most absences
* /graph_data/payment_delinquency : give list of invoices vs paymentreceipts for each debtor
* /graph_data/debtor_languages : give proportions of home languages for all debtors (used to illustrate combination of Redis and Xpath querying)
* /graph_data/local_network : get all direct and indirect ids  that an id is connected to, and identify their collections
=end


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
