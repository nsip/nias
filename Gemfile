# Gemfile


source 'https://rubygems.org'

ruby '2.2.3'

# redis client interface
gem 'redis'
gem 'hiredis', '>= 0.6.0' 

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

# CSV validation
gem 'csvlint', :path => '/Users/nickn/Documents/Arbeit/csvlint.rb'

# JSON-Schema validation
gem 'json-schema'


# process matrix parameters in URLs
# gem 'rack-matrix_params'

group :test do
	gem 'rspec', '~> 3.0'
	gem "rack-test"
end
