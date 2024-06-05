# Implementation of five spatiotemporal continuous queries:

## Average Speed by MMSI
It is an aggregation query. Within this query, we calculate the average speed of vessels for each unique MMSI (id).

## Vessel Count by Area and Period
This query is a spatiotemporal aggregation. We count the number of distinct vessels in a specified geographic area and period. We use spatial filtering with bounding boxes and temporal filtering.

## Port Arrival and Departure Times by MMSI
This query showcases event detection and Temporal Analysis. It detects the times vessels arrive at and depart from specified ports. Based on its AIS points and MEOS' atGeom function, it identifies when a ship enters a port area and when it leaves.

## Vessel Density Heatmap
It is a spatiotemporal density estimation query.
Generate a heatmap showing the density of vessel presence over a geographic area within a specified period. Calculating the number of vessel points per grid cell in a spatial grid overlaid on the region of interest.

## Outlier Detection in Vessel Movement
It is a spatiotemporal anomaly detection query. To answer it, we identify anomalous vessel movements based on deviation from typical paths or speeds \cite{OutlierGeo}. This query requires historical data analysis to determine movement patterns and then detect outliers that significantly differ from these patterns. 
