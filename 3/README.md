# Building Real-Time Data Streaming using Kafka, Apache Flink and Postgres

##  Port Arrival and Departure Times by MMSI
This query showcases event detection and Temporal Analysis. It detects the times vessels arrive at and depart from specified ports. Based on its AIS points and MEOS' atGeom function, it identifies when a vessel enters a port area and when it leaves.

# idea
use a sliding window, keep in memory if the ship was intersecting before or not. 