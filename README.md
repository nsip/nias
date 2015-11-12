
NSIP Integration As A Service - NIAS
====================================

**Important Note - This project is under heavy development, do not expect installation instructions to produce anything that actually runs at this point.**

Installation Notes

If you're running on mac you will need homebrew to install packages, and homebrew will need a suitable toolchain to build native code. If you have a mac developer account, homebrew will offer to fetch the command-line tools as part of its installation. The easiest alternative is just to install XCode which includes the tools.

Ensure Ruby is at 2.2.3 as a minimum.

rvm install ruby-2.2.3

You will also need to install the 'bundler' gem to help with ruby code module dependencies. After rvm is installed just do

> gem install bundler

NIAS (& timesheet) use Redis. This is distributed from Redis.io as source and can be compiled for any platform, but most likely route to installation is to use package manager for the platform

On mac 'brew install redis' is the easiest and quickest

apt-get, yum etc. for linux

check Redis has been added to your binary path by running the command 'redis-server' from the commandline - you should get a redis instance, Ctrl+C to close again. 

Java resources are cross-platform & so zookeeper and kafka are included in the distribution.

LMDB/GDBM may be used for the key-value store component of SMS as data volumes increase - 
like redis these are  C source distribution which can be built on the platform or installed i.e.

'brew install lmdb'
'brew install gdbm'
'gem install gdbm'


when services are running all kafka logs, redis dump files etc. will be created under /tmp/nias e.g. /tmp/nias/kafka-logs /tmp/nias/redis tmp/zookeeper

this is a reliable location on mac/linux but check it's writeable from current user account.

There is a Gemfile in the root nias directory - 
cd into /nias and then run 'bundle install' to pull in all gems required by the projects. 

the layout of the code is as follows

nias
-- launch_xxx.rb - launchers for each block of services, run launchers with -K
on the command-line to shut services down

launch_core.rb should always be run first, this brings up kafka/zookeeper and then the SSF (sif store & forward) and the SMS (sif memory store)

launch_nias.rb will bring up all of the integration related services - under development!

launch_timesheet.rb will bring up the ingest and query/ui services for the timesheet reporting solution.

So:

bash --login
./launch_core.rb
./launch_nias.rb
./launch_nias.rb -K
./launch_core.rb -K

If crashed out of Kafka/Zookeeper and need to delete them:

rm -fr  /tmp/nias/kafka-logs
rm -fr  /tmp/nias/zookeeper

or to get rid of everything (does no harm, but will not work if core/nias are still running)

rm -fr /tmp/nias

/kafka
    contains the latest kafka/zookeeper distro, the config files in /kafka/config are the ones used to configure the tools

/sms
    contains the Sif Memory Store, currently a stub redis config only, will grow as the indexer and web ui are built out

/ssf
    The Sif Store & Forward adapter - rest interface in sinatra that sits in front of kafka for our purposes and handles xml/json/csv

/test_data
    bunch of handy files to send to ssf 

We also recommend the installation of the 'httpie' package as a user-friendly curl replacement that we use in many of the testing scripts:

> brew install httpie

This will allow you to run ingestion test scripts in the /test_data area.

Example:

Using httpie


http post :9292/test/test1 Content-Type:application/xml < test_data/timetable.xml

http post :9292/oneroster/validated Content-Type:text/csv < test_data/users.csv

http post :9292/test/json < 1k_student_personals.json
(note json is the default content type so doesn't need to be expliclty stated, for xml and csv it does)

All services can be found for ui at:

localhost:9292/nias

which has links to all the other services.






