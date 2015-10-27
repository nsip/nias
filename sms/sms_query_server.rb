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
# that has been provided to the store in the form of sif (and other e.g. ims) messages
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
# Finally the optional 'defer' parameter can be passed to any query (it's assumed to be false if not present).
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

require 'sinatra'
require 'sinatra/reloader' if development?
require 'json'
require 'csv'
require 'redis'

require_relative 'sms_query'


configure do

	 set :smsq, SMSQuery.new

end

get "/" do

	@result = settings.smsq.known_collections
	erb :collections

end




































