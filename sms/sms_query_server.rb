# sms_query_server.rb


# simple sinatra web service that offers only two methods:
# 
# 'find' and 'collections' 
# 
# Find: 
# 
# find [collection name] [item id] optional parameter defer=true/false
# 
# collection name is a string representing a collection of objects in the memory store, these are based on the data
# that has been provided to the store in the form of SIF (and other e.g. IMS) messages
# 
# calling find with just a collection name will return the members of that collection, collections would be
# such objects as SchoolInfo, StudentPersonal etc. etc.
# 
# calling find with just an item id (typucally the refid of a sif object) will simply return the message of that id
# 
# Calling find with both 'collection' & 'itemid' is where things get interesting. In this case the SMS will walk
# all known associations between the collection and the item and return the result of that relationship graph.
# 
# This makes for a very simple yet powerful query interface that will find the relationships bewteen objects and then
# return the resulting data. For the vast majority of integration use cases (as opposed to operational queries), this is 
# enough to provide a workable data set for use in client processing.
# 
# as the sms and query can walk all relationships in all directions there are some benficial side-effects that help to keep
# the query interface simple.
# 
# For example in the AU model, input data would include SchoolInfos, StudentPersonals and StudentSchoolEnrolments would act as 
# the joining objects to link schools and students.
# 
# A query where itemid is the id of a school and collection is StudentPersonals, however, will return all students in a school, 
# since the query walks all relationships the intermediate join objects become invisible to the user.
# 
# These relationship-based queries will find any relationship, such that queries can ask, for instance, for all invoices or 
# attendance records within an LEA or District - since invoices/attendance are linked to schools and schools are linked to
# LEAs then again the intermediate objects are rendered invisible, but the correct dataset is returned.
#
# The optional 'defer' parameter can be passed to any query (it's assumed to be false if not present).
# 
# Passing true for this parameter means that results of a query will be posted to a temporary topic/queue on the
# SSF server.
# 
# This is to allow client systems to work with data at their own pace since SSF topics are durable & can be read many times 
# 
# In particular it is used when the results of a query are likely to produce a very large dataset, for example all students 
# in an LEA - for a large jurisdiction this could potentially produce a results set too large for a single html
# request to handle. Rather than getting into paging and cursor management, by putting the results onto an SSF queue
# the client is then free to consume the data at their own pace, as many messages at a time as they can cope with:
# the queues are persistent so there is no pressure on the client for immediate consumption.
# 
# The query interface does contain a limit for returned items set to a sensible value, so that any query that produces 
# a result-set larger than that value will automatically be deferred in order to prevent any user or the system from being
# overwhelmed by the volume of data.
# 
# The optional 'include_messages' parameter will return the full xml of all found entities, when this parameter is set to 
# false (the default) then only relevant references are returned to keep payload small.

require 'sinatra'
require 'sinatra/reloader' if development?
require 'sinatra/base'
require 'sinatra/contrib'
require 'json'
require 'csv'
require 'moneta'

require_relative 'sms_query'
require_relative 'oneroster_sif_merge'


class SMSQueryServer < Sinatra::Base

	helpers Sinatra::ContentFor

	configure do
		 set :store, Moneta.new( :LMDB, dir: '/tmp/nias/moneta', db: 'nias-messages')
	end

	get "/sms" do
		smsq = SMSQuery.new
		@coll_result = smsq.known_collections
		erb :collections
	end


	get "/sms/collections" do
		@result = []
		smsq = SMSQuery.new
		@result = smsq.known_collections
		return @result.to_json
	end

	# Local id search method
	# takes 2 parameters
	# id: an object's local id
	# collection: name of an object collection
	# resolves local id to GUID, and redirects query to main search

	get "/sms/localfind" do
		collection = params['collection'] || nil
		id = params['id'] || nil
		smsq = SMSQuery.new
		guid = smsq.local_id_resolver(id) || nil
		new_url = request.fullpath.gsub(/id=[^&]+/, 'id='+guid ).gsub(/\/localfind/, '/find')
		redirect new_url
	end

	# Main search method...
	# 
	# takes 2 paramters:
	# 
	# id: an object's unique id
	# collection: name of an object collection
	# 
	# if neither is provided will throw an error
	# 
	# optional parameters:
	# 
	# include_messages: boolean (false by default) - if set to true query will return all xml mesasges associated
	# with the query; by default will just return all object references.
	# 
	# defer: boolean (false by default) - if set to true results will not be returned directly but will be written
	# to a temporary ssf queue - the url of the queue will be returned as the query result
	# 
	get "/sms/find" do


		collection = params['collection'] || nil
		id = params['id'] || nil

		# handle the fact that empty but non-nil params can be passed via url
		if collection != nil && collection.length == 0
			collection = nil
		end
		if id != nil && id.length == 0
			id = nil
		end
 
		# puts "\n\ncollection: #{collection.inspect}"
		# puts "id: #{id.inspect}\n\n"

		if params['include_messages'] == 'true'
			include_messages = true
		else
			include_messages = false
		end

		if params['defer'] == 'true'
			defer = true
		else
			defer = false
		end


		puts "\n\nCollection : #{collection}\n\nId : #{id}\n\n"
		halt( 400, "Must have at least one query parameter 'id' or 'collection'." ) unless ( collection || id )

		results = []
		smsq = SMSQuery.new

		if collection && id
			results = smsq.find( id, collection )
		elsif collection
			results = smsq.collection_only( collection )
		else
			# assume if only single item requested the whole message is wanted
			include_messages = true
			results << id
		end

		return [{id: 0, data: 'No results found'}].to_json unless !results.empty?

		response = []
    	results.each do | result |
    		
    		record = {}
    		
    		record[:id] = result
    		if include_messages
    			record[:data] = settings.store[result]
    		end
			record[:label] = smsq.get_label(result)

    		response << record

    	end
		return response.to_json


	end

	# Requests to merge OneRoster and SIF records that share the same local ID

	get "/sms/merge_ids" do
	
		orsm = OneRosterSifMerge.new

		begin
			orsm.merge_ids
		rescue 
			return 500, 'Error executing SIF OneRoster id merge.'
		end

		return 200, 'SIF - OneRoster id merge complete'

	end



end




































