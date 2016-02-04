# Gemfile


source 'https://rubygems.org'

ruby '2.2.3'

# redis client interface
gem 'redis'
gem 'hiredis', '>= 0.6.0' 


# gem 'eventmachine'

# web tools
gem 'sinatra', require: 'sinatra/base'
gem 'sinatra-contrib', github: 'sinatra/sinatra-contrib'

# evented server for sinatra
gem 'thin'

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

gem 'kafka-consumer'

# process matrix parameters in URLs
gem 'rack-matrix_params'

group :test do
	gem 'rspec', '~> 3.0'
	gem "rack-test"
end
