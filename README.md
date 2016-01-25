
NSIP Integration As A Service - NIAS
====================================

**Important Note - This project is under heavy development, do not expect installation instructions to produce anything that actually runs at this point.**

# Overview

NIAS is a suite of open-source components designed to enable as many different users as possible to quickly and easily solve issues of system integration using the Australian [SIF Data Model](http://specification.sifassociation.org/Implementation/AU/1.4/html/) for school education.

The product was developed by harnessing existing open source middleware components, including:
* The [Apache Kafka](http://kafka.apache.org) message broker
* The [Nokogiri](http://www.rubydoc.info/github/sparklemotion/nokogiri) XML library for Ruby
* The [Sinatra](http://www.sinatrarb.com) web framework for Ruby
* The [Redis](http://redis.io) NoSQL database
* The [LMDB](http://symas.com/mdb/) key-value store
* The [D3](http://d3js.org) Javascript visualisation library, and visualisation APIs built over D3, including [Dimple](http://dimplejs.org) and [JSNetworkX](http://felix-kling.de/JSNetworkX/).

Over these components, two main modules have been built:
* The __SIF Store & Forward (SSF)__ is an opinionated message queueing system, which ingests very large quantities of data and stores them for delivery to clients. XML messages on the system are assumed by default to be in SIF. The SSF service builds an education-standards aware REST interface on top of Kafka, and provides a number of utility services to ease SIF-based integrations.
* The __SIF Memory Store (SMS)__ is a database that builds its internal structures from the data it receives, using RefIds both as keys to access stored messages, and to map out a network graph for SIF objects.

The code is open source, and is released through the NSIP Github repository. The code is currently in Ruby, although the microservice architecture means that modules in other languages can be added readily, and the core modules themselves can be ported readily to other languages.

## Scope

This product delivers the following high level functions: 
1.	Support for persistent and ordered queues of SIF messages, which can be reread multiple times.
2.	Support for asynchronous queues in both clients and servers.
3.	Support for format-agnostic messaging infrastructure.
4.	Support for data exchange through an event/subscribe model (in brokered environments)
5.	Support for message validation.
6.	Support for extracting arbitrary relations between object types within SIF (bypassing need to configure service path queries, and simplifying the query API for objects).
7.	Support for extracting arbitrary relations between object types from different standards (allowing multiple data standards to coexist in an integration, referring to the same entities).
8.	Support for privacy filtering in middleware (which releases object providers from having to do privacy filtering internally).
9.	Support for simple and extensible interactive analytics.
10.	Support for the ODDESSA data dictionary as a service.
11.	Support for data format conversions, including CSV to SIF, and SIF 2 to SIF 3.

This product only acts as middleware. It does not provide integration with the back ends of products (although this can be provided by combining NIAS with the [SIF Framework](https://github.com/nsip/sif3-framework-java)). It is not intended to deliver business value to end consumers, or to compete with existing market offerings.

The product delivers only exemplar analytics, and the SIF team is not committing to developing analytics and queries for all product users. Users that do develop their own analytics and queries are encouraged to contribute these back as open source.

The product delivers only exemplar integrations between multiple standards (SIF/XML and [IMS OneRoster](https://www.imsglobal.org/lis/index.html)/CSV), and the SIF team is not committing to developing standards integrations for all product users. Users that do develop their own standards integrations are encouraged to contribute these back as open source.

The product does not incorporate authentication or authorisation.



# Code structure

nias
-- launch_xxx.rb - launchers for each block of services, run launchers with -K
on the command-line to shut services down



/kafka
    contains the latest kafka/zookeeper distro, the config files in /kafka/config are the ones used to configure the tools

/sms
    contains the Sif Memory Store, currently a stub redis config only, will grow as the indexer and web ui are built out

/ssf
    The Sif Store & Forward adapter - rest interface in sinatra that sits in front of kafka for our purposes and handles xml/json/csv


# Running NIAS

Once you have installed NIAS, launch the NIAS infrastructure services (`launch_core.rb`) and micoservices (`launch_nias.rb`).
* `launch_core.rb` should always be run first: this brings up kafka/zookeeper and then the SSF (sif store & forward) and the SMS (sif memory store)
* `launch_nias.rb` will bring up all of the integration related services.

So:
    bash --login
    ./launch_core.rb
    ./launch_nias.rb
    
To shut NIAS down, shutdown `./launch_nias.rb` before `./launch_core.rb`:

    ./launch_nias.rb -K
    ./launch_core.rb -K

Kafka is by design quite robust in persisting its logs; Zookeeper is even more so. If you have crashed out of Kafka/Zookeeper, and need to delete all Kafka topics:

    rm -fr  /tmp/nias/kafka-logs
    rm -fr  /tmp/nias/zookeeper

Or to get rid of everything (does no harm, but will not work if core/nias are still running)

    rm -fr /tmp/nias

All services available through NIAS are exposed through the NIAS UI:

    http://localhost:9292/nias

which has links to all the other services.


# Installation Notes

If you're running on Mac you will need `homebrew` to install packages, and `homebrew` will need a suitable toolchain to build native code. If you have a Mac developer account, `homebrew` will offer to fetch the command-line tools as part of its installation. The easiest alternative is just to install XCode which includes the tools.

Ensure Ruby is at 2.2.3 as a minimum.

    rvm install ruby-2.2.3

You will also need to install the 'bundler' gem to help with ruby code module dependencies. After `rvm` is installed just do

    gem install bundler

NIAS uses Redis. This is distributed from Redis.io as source and can be compiled for any platform, but the most likely route to installation is to use the package manager for the platform.

On Mac `brew install redis` is the easiest and quickest. 

Use `apt-get`, `yum` etc. for linux

Check Redis has been added to your binary path by running the command `redis-server` from the command line. You should get a redis instance. Ctrl+C to close again. 

Java resources are cross-platform, so Zookeeper and Kafka are included in the distribution.

LMDB/GDBM may be used for the key-value store component of SMS as data volumes increase. 
Like Redis these are  C source distribution which can be built on the platform or installed, i.e.

    brew install lmdb
    brew install gdbm
    gem install gdbm 


When services are running all Kafka logs, Redis dump files etc. will be created under `/tmp/nias` e.g. `/tmp/nias/kafka-logs`, `/tmp/nias/redis`, `tmp/nias/zookeeper`

This is a reliable location on Mac/Linux, but check it's writeable from current user account.

There is a Gemfile in the root nias directory: 
* `cd .../nias` 
* run `bundle install` to pull in all gems required by the projects. 





# Testing
Rspec Unit tests for the methods are in place in the expected `/spec` directory.

See also the [NIAS Test Data](https://github.com/nsip/nias_testdata) repository for much larger test data.


