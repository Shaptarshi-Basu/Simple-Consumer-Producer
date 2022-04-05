# Simple-Consumer-Producer
Kafka consumer to read from 3 kafka topics and push that data in JSON into another topic. Used Dockerized Kafka setup

## Topic Details
As mentioned above there are 3 topics to be read from and one topic to be produced to as JSON

Topic to be read from
* Topic1
* Topic2
* Topic3

Topic to produced
* Topic4

## Run Integration test
Run command - make integration-test

Prerequisite: needs go version 1.17
and run go mod download to fetch dependant libraries

## Build Docker image
#### Use appropriate naming and tagging
Run command - docker build -t NAME:TAG .
  
## Run Docker image
#### Use appropriate naming and tagging
Run command - 
  1. docker-compose up -d 
  2. docker run --network=host NAME:TAG
  
  

*****

