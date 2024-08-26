# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

## Vessel Density Heatmap
It is a spatiotemporal density estimation query.
Generate a heatmap showing the density of vessel presence over a geographic area within a specified period. Calculating the number of vessel points per grid cell in a spatial grid overlaid on the region of interest.


## To compile
cd postgres
docker build -t postgres4 .
cd ..
cd kafka-producer
docker build -t kafka-producer4 .
cd ..
cd flink-processor
mvn clean package  
docker build -t flink-processor4 .
cd ..
docker-compose up -d 