# Opensearch implementation of URLFrontier service

This implementation of [URLFrontier](http://urlfrontier.net) service is compatible with the version 1.1 of the URLFrontier API.
It uses [Opensearch](https://opensearch.org/) as a backend for storing the URLs discovered or visited during a crawl and exposes its content using the URLFrontier API.

*TODO* describe general approach, partitions and assignments. Explain indices used.

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


