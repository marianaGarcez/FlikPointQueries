# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

##  Port Arrival and Departure Times by MMSI with CEP
This query showcases event detection and Temporal Analysis. It detects the times vessels arrive at and depart from specified ports. Based on its AIS points and MEOS' atGeom function, it identifies when a vessel enters a port area and when it leaves.

# idea
use a sliding window, keep in memory if the ship was intersecting before or not. 


## To compile
cd postgres
docker build -t postgres3c .
cd ..
cd kafka-producer
docker build -t kafka-producer3c .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor3c .
cd ..
docker-compose up -d 