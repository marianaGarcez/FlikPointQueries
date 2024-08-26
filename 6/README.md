# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## 


## Prerequisites
docker
MEOS

## To compile
cd postgres
docker build -t postgres6 .
cd ..
cd kafka-producer
docker build -t kafka-producer6 .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor6 .
cd ..
docker-compose up -d 