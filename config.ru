
require './ssf/ssf_server'
require './ssf/nias_server'
require './ssf/sif_privacy_server'
require './ssf/hookup_ids_server.rb'
require './sms/sms_query_server'


use SMSQueryServer
use SPSServer
use SSFServer
use HookupServer

run NIASServer








