
require './ssf/ssf_server'
require './ssf/nias_server'
require './ssf/sif_privacy_server'
require './ssf/hookup_ids_server.rb'
require './ssf/equiv_ids_server.rb'
require './ssf/filtered_client.rb'
require './sms/sms_query_server'
require './sms/graph_server.rb'
require './naplan/naplan_server'

use Naplan_SSFServer
use GraphServer
use SMSQueryServer
use SPSServer
use SSFServer
use FilteredClient
use HookupServer
use EquivalenceServer


run NIASServer









