# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## Vessel Count by Area and Time Period
This query is a spatiotemporal aggregation. We count the number of distinct vessels in a specified geographic area and period. We use spatial filtering with bounding box and temporal filtering.


## To compile
cd postgres \\
docker build -t postgres . \\
cd .. \\
cd kafka-producer \\
docker build -t kafka-producer . \\
cd .. \\
cd flink-processor \\
mvn clean package \\
docker build -t flink-processor . \\
cd .. \\
docker-compose up -d \\