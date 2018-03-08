# Gemfile


source 'https://rubygems.org'

ruby '2.2.3'

# redis client interface
gem 'redis'
gem 'hiredis', '>= 0.6.0' 


gem 'eventmachine'

# web tools
gem 'sinatra', require: 'sinatra/base'
github 'sinatra/sinatra' do
  gem 'sinatra-contrib'
end

# evented server for sinatra
gem 'thin'

# web sockets
# sinatra-websocket uses old API of em-websocket
gem 'em-websocket', '< 0.3.8'
gem 'sinatra-websocket'

# lightweight unique ids
gem 'hashids'

# kafka client
gem 'poseidon'
gem 'poseidon_cluster'

# zookeeper client
gem 'zk'

# xml processing
gem 'nokogiri'

# interface for lmdb
gem 'moneta'
gem 'lmdb'

# CSV validation
#gem 'csvlint', :path => '/Users/nickn/Documents/Arbeit/csvlint.rb'
gem 'csvlint'

# JSON-Schema validation
gem 'json-schema'

gem 'kafka-consumer', github: 'nsip/kafka-consumer'

# process matrix parameters in URLs
# gem 'rack-matrix_params'

group :test do
	gem 'rspec', '~> 3.0'
	gem "rack-test"
end
