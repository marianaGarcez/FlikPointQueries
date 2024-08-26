# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## 

## Prerequisites
docker
MEOS


## To compile
cd postgres
docker build -t postgres8 .
cd ..
cd kafka-producer
docker build -t kafka-producer8 .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor8 .
cd ..
docker-compose up -d 