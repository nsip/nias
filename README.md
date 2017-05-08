
NSIP Integration As A Service - NIAS
====================================

**Important Note - This project has been superseded by [NIAS2](https://github.com/nsip/nias2).**

# 1. Overview

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

## 1.1. Scope

This product delivers the following high level functions: 

1. Support for persistent and ordered queues of SIF messages, which can be reread multiple times.
2. Support for asynchronous queues in both clients and servers.
3. Support for format-agnostic messaging infrastructure.
4. Support for data exchange through an event/subscribe model (in brokered environments)
5. Support for message validation.
6. Support for extracting arbitrary relations between object types within SIF (bypassing need to configure service path queries, and simplifying the query API for objects).
7. Support for extracting arbitrary relations between object types from different standards (allowing multiple data standards to coexist in an integration, referring to the same entities).
8. Support for privacy filtering in middleware (which releases object providers from having to do privacy filtering internally).
9. Support for simple and extensible interactive analytics.
10. Support for the ODDESSA data dictionary as a service.
11. Support for data format conversions, including CSV to SIF, and SIF 2 to SIF 3.

This product only acts as middleware. It does not provide integration with the back ends of products (although this can be provided by combining NIAS with the [SIF Framework](https://github.com/nsip/sif3-framework-java)). It is not intended to deliver business value to end consumers, or to compete with existing market offerings.

The product delivers only exemplar analytics, and the SIF team is not committing to developing analytics and queries for all product users. Users that do develop their own analytics and queries are encouraged to contribute these back as open source.

The product delivers only exemplar integrations between multiple standards (SIF/XML and [IMS OneRoster](https://www.imsglobal.org/lis/index.html)/CSV), and the SIF team is not committing to developing standards integrations for all product users. Users that do develop their own standards integrations are encouraged to contribute these back as open source.

The product does not incorporate authentication or authorisation.

## 1.2. Constraints

* The product is released with the SIF-AU 3.4 XSD schema, and validates against it. Other schemas can be used, but may require re-coding of some modules.
* The number of Kafka queues used by SSF can be large depending on the amount of privacy filtering and mapping done within it. The queue size is constrained by the storage available in the deployment, and should be configured to flush within a business-appropriate timeframe.

# 2. Code Structure

## 2.1. Directories

`/kafka`
* Contains the latest Kafka/Zookeeper distro. The config files in `/kafka/config` are used to configure the tools.

`/spec`
* Contains `rspec` unit tests

`/sms`
* Contains the SIF Memory Store

`/ssf`
* Contains the SIF Store & Forward Adapter 

## 2.2. Core Components

### 2.2.1. SIF Store & Forward (SSF)

This is the core messaging infrastructure of NIAS. Based on the Apache Kafka message broker it simply ingests very large quantities of data and stores them for delivery to clients. The transport is utterly format agnostic and will support XML (SIF/EdFi), CSV (including IMS OneRoster), and JSON.

The defining feature of Kafka message queues is that they are persistent and ordered, meaning they can be re-read multiple times without consequence. For integration tasks, this means that both clients and servers can be asynchronous in their behavior; the inability for providers to behave in this way, or for one party in an integration to be forced to play the role of always-available-provider, is a current issue in market adoption, in particular for interfaces between solution providers.

Since the Kafka protocol is entirely message driven, services to work with its data can be written in any development language.
The SSF service that is part of NIAS simply builds an education-standards aware REST interface on top of Kafka, and also provides a number of utility services to ease SIF-based integrations.

### 2.2.2. SIF Memory Store (SMS)

The SMS is a database that builds its internal structures from the data it receives, rather than from the imposition of a schema.
When a SIF data object is provided to the SMS (typically via an SSF queue), the SMS creates a graph of all references to and from that object, and adds each object to a collection based on its type. As more data is added the number of collections can grow, but producers and consumers of data are not required to implement any more collections than those they wish to work with. In an integration scenario, if parties wish to exchange only invoice data and student personal data, then they can. 

Allowing the data to drive the structure lowers the effort for data providers considerably. The net result is that users no longer have to know the relationships between the different parts of the SIF model:, by providing data in the correct format the relationships will build themselves. The simplest possible input API for data, then, becomes achievable: data providers simply need to know how to represent the entities in their own systems as the appropriate SIF object. Entire schools-worth of data can be ingested in a single operation, with no API needed for the producer.

When it comes to retrieving data from the SMS, the query service exploits the graph of references to find any relationship between the requested items. Queries can all be expressed in the form of two parameters: the ID of an item in the database, and the name of a collection. For instance, providing the ID of a school as the item and *students* as the collection will find all students who have a relationship with the school. A teaching-group ID and the collection *attendance time lists* will return the attendance information for that teaching group.

One important consequence of the SMS traversing all relationships is that user queries no longer need to be aware of intermediate join objects, such as the StudentSchoolEnrollment objects that currently join students to schools. This radically simplifies the necessary understanding of the SIF data model when undertaking integration tasks. Users can focus on core entities such as students and class groups, without having to handle the wider complexities of the data model.

The combination of SSF and SMS achieves a highly simplified interface for integration based on SIF. In effect the inbound API is simply a stream of SIF messages, and the outbound API is a single query requiring only two parameters to fulfil any service path available in the provided data.

### 2.2.3. Non-SIF data

A significant side-effect of using non-SQL tools to construct the data store is that non-SIF data can also be easily accommodated for integration purposes. For instance IMS OneRoster data can be ingested, and if provided at ingestion time with a linking object identity, will be inserted into the relationship graph at that point. The students imported via OneRoster become equal citizens as far as querying all further relationships are concerned. Hence the rest of the data model is now linked to the OneRoster students. The SMS can now be queried, for example, to retrieve all invoices for a OneRoster student, or all timetable subjects that they undertake.

The key point is that the receiving systems do not have to build out the whole of their data model to understand or implement IMS OneRoster, and it becomes an ongoing choice as to whether they ever need to ingest that data back into their core systems.

The goal of NIAS is to potentially allow multiple open standards with specialist areas of expertise to co-exist in the most productive way for end users. This removes the need for a single standard to cover all uses, and means that each open standard can focus on adding its particular value. There is no need to pick a winner in order to build out a comprehensive model that covers all possible activities.

## 2.3. NIAS Servces

The core components provide a lightweight architecture that works through pure exchange of data messages. To this foundation we can add a number of services, each of which helps to solve particular integration concerns that NSIP has identified through its work with stakeholders.

### 2.3.1. SIF Privacy Service (SPS)

This service allows users to attach privacy filters to any outbound stream of SIF messages. Filters are held independently of the data and can be edited or specialized for any particular purpose. An editing UI is provided.

The current filters implement the NSIP Privacy Framework constraints for profiles of SIF data against the APP ratings; the default profiles are, therefore, Low, Medium, High and Extreme.

All data transformed via the service is then exposed as SSF endpoints for consumption, meaning that privacy control for clients is achieved simply by pointing the client to the correct endpoint rather than managing any data access. This approach also means that data producers do not have to be concerned with implementing privacy policy in their own solutions, and that policy is applied consistently to all data passing through NIAS.

### 2.3.2. IMS Ingest 

This is a specialized input to receive IMS OneRoster information, with an additional object id parameter that allows the data to be connected to the main data model at an insertion point of the users choosing—thus linking the supplied data to all other queries available through the SMS.

IMS data in its original form can also be consumed from the endpoint. Thus, if the integrations scenario is focused on linking the data, IMS OneRoster clients can produce and consume their data from the service, but with no need for onward systems to ever ingest the data unless they choose to.

### 2.3.3. Lightweight Analytics Service (LAS)
 
This service extracts SIF data from the SMS via query (all attendance records for a school, for example), and creates data arrays suitable for presentation in the family of visualisation tools based on the D3 specifications (a specialized json infrastructure for visualising data).

D3-based clients are then easily instantiated in html pages for lightweight dashboarding and basic reporting. These same services can be used to provide interactive data analysis support to NAPLAN results users where no systemic BI capability is available.

###2.3.4. CSV-SIF Conversion

This is a simple service to support NAPLAN Online integrations. Validating CSV files is significantly harder than validating XML, but there will be a strong preference in schools and jurisdictions initially to produce registration data in CSV format. This service converts CSV input to the relevant SIF objects for onward transmission to the National Assessment Platform.

# 3. Running NIAS

Once you have installed NIAS, launch the NIAS infrastructure services (`launch_core.rb`) and microservices (`launch_nias.rb`, `launch_nias.rb`).
* `launch_core.rb` should always be run first: this brings up kafka/zookeeper and then the SSF (sif store & forward) and the SMS (sif memory store)
* `launch_nias.rb` will bring up all of the integration related services.
* `launch_nias.rb` will bring up integration related services specific to NAPLAN Online.

So:

    bash --login
    ./launch_core.rb
    ./launch_nias.rb
    (or: ./launch_naplan.rb)
    
To shut NIAS down, shutdown `./launch_nias.rb` before `./launch_core.rb`:

    ./launch_nias.rb -K
    (or: ./launch_naplan.rb -K)
    ./launch_core.rb -K

Kafka is by design quite robust in persisting its logs; Zookeeper is even more so. If you have crashed out of Kafka/Zookeeper, and need to delete all Kafka topics:

    rm -fr  /tmp/nias/kafka-logs
    rm -fr  /tmp/nias/zookeeper

Or to get rid of everything (does no harm, but will not work if core/nias are still running)

    rm -fr /tmp/nias

All services available through NIAS are exposed through the NIAS UI:

    http://localhost:9292/nias

which has links to all the other services.

## 3.1. Configuration

If topics are not already in place for Kafka, initialising them can take a long time. For that reason, `launch_core.rb` includes a list of Kafka topics expected to be used. You may need to add or remove from those topics.

Several microservices have command-line options, which can be configured in their launcher modules. In particular:

* `cons-prod-sif-ingest-validate.rb` and `cons-prod-sif-bulk-ingest-validate.rb` take the SIF schema they validate against. In `launch_nias.rb`, that is `./ssf/services/xsd/sif3.4/SIF_Message3.4.xsd`, the Data Model XSD for SIF-AU 3.4. In `launch_naplan.rb`, that is `/ssf/services/xsd/sif3.4/NAPLANRegistrationDraft.xsd`, a customisation of the SIF-AU schema which includes only the objects needed for NAPLAN registration, with several elements made mandatory rather than optional.
* If `cons-prod-studentpersonal-naplanreg-unique-ids-storage.rb` in `launch_naplan.rb` is run with option `psi`, a Platform Student Identifier is generated for each received student record, and added on ingest.
* If `cons-prod-sif2csv-SRM-validate.rb` in `launch_naplan.rb` is run with a state/territory abbreviation as an option, validations specific to that jurisdiction will be performed. In particular, ASL school IDs will be constrained to those specific to that jurisdiction.

The hosts and ports for the various services used by NIAS are defined in `./niasconfig.rb`. The version included in this distribution gives default port values for localhost.

# 4. Installation Notes

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

## 4.1. Testing
See also the [NIAS Test Data](https://github.com/nsip/nias_testdata) repository for much larger test data.

# 5. Future functionality

NIAS is under ongoing development to meet the needs of NSIP stakeholders. Support for NAPLAN registration data validation and upload has been identified as a national priority.

The following may be implemented as needed in future releases:

* Support for validation of very large XML files, using a streaming parser rather than current parser (Nokogiri). Currently XML validation becomes time-consuming for very large XML files (i.e. over 100 MB).

