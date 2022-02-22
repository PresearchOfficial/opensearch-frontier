# Opensearch implementation of URLFrontier service

This implementation of [URLFrontier](http://urlfrontier.net) service is compatible with the [version 1.1 of the URLFrontier API](https://github.com/crawler-commons/url-frontier/releases/download/urlfrontier-1.1/urlfrontier.proto).
It uses [Opensearch](https://opensearch.org/) as a backend for storing the URLs discovered or visited during a crawl and exposing its content to web crawlers using the URLFrontier API.

This implementation was designed with the following constraints in mind: instances of the Frontier service can work on the same crawls but don't have to be able to connect to each other. Each Frontier instance might also receive requests from a very large number of crawlers.

The crawl space is divided into a fixed number of *partitions*, which each queue (e.g. a domain or hostname) of the crawl mapped to a partition. The index *queues* contains all the queues and the mappings to the corresponding partition.
The frontier itself, i.e. all the URLs from the crawls, is stored in an index called *status*.

The Frontier instances get assigned a number of partitions, in proportion of the number of instances actively working. If two instances are working, each one will get 50% of the assignments. If a Frontier instance disappears, the remaining instances will detect that is has left and reassign its partitions. The index *frontiers* is used to keep track of the active instances, while the index *assignments* has up to N documents, each corresponding to a partition and is linked to a Frontier.

The assignment method is eventually consistent and it takes a few iterations for the number of assignments to stabilise.

## Prerequisites

* Java 8
* Maven
* Docker (to run the tests)

## Compilation

`mvn clean package`

or

`mvn clean package -DskipTests` to avoid running the tests.

## Run the service

The *docker-compose.yml* file can be used to spin an instance of [Opensearch](https://opensearch.org/) and opensearch-dashboards.

Starting the Frontier is done with 

`$ java -Xmx2G -cp target/urlfrontier-opensearch-service-*.jar crawlercommons.urlfrontier.service.URLFrontierServer -c config.ini`

The indices are created automatically in URLFrontier but the script *Opensearch_Delete_Indices.sh* can be used to delete them and restart a fresh crawl.


