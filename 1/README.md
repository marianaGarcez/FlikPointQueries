# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## Average Speed by MMSI
It is an aggregation query. Within this query, we calculate the average speed of vessels for each unique MMSI (id).



## To compile
cd postgres
docker build -t postgres .
cd ..
cd kafka-producer
docker build -t kafka-producer .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor .
cd ..
docker-compose up -d 