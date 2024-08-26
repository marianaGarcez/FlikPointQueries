# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## 


## Prerequisites
docker
MEOS

## To compile
cd postgres
docker build -t postgres10 .
cd ..
cd kafka-producer
docker build -t kafka-producer10 .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor10 .
cd ..
docker-compose up -d 