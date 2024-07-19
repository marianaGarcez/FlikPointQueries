# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## Average Speed by MMSI
It is an aggregation query. Within this query, we calculate the average speed of vessels for each unique MMSI (id).


## Prerequisites
docker


## To compile
cd postgres
docker build -t postgres1 .
cd ..
cd kafka-producer
docker build -t kafka-producer1 .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor1 .
cd ..
docker-compose up -d 