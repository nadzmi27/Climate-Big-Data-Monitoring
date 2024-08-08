# Background
This project is a part of my **Big Data Management and Processing** unit. The project is split into two parts: **Part A** (data model designing and building) and **Part B** (real-time streaming and batch processing).

Part A of the project focuses on designing and building a data model using [MongoDB](https://en.wikipedia.org/wiki/MongoDB), and moving existing climate data from CSV into the database. The reason is that the data being collected is growing in numbers, and a scalable database is needed. Furthermore, complex queries are required for processing and analysis.

Part B of the project focuses on real-time streaming and batch processing of the climate data using [Apache Kafka](https://en.wikipedia.org/wiki/Apache_Kafka) and [Apache Spark](https://en.wikipedia.org/wiki/Apache_Spark). The streamed and processed data will be visualised in real-time for monitoring purposes.  

# Architecture
![image](https://github.com/user-attachments/assets/df4a21e1-2b69-4d09-9bb1-9bc1229360a3)

- **Producer 1:** Feed the Climate data to the stream every 10 seconds.
- **Producer 2:** Feed the AQUA data to the stream every 2 seconds. AQUA is the satellite from NASA that reports the latitude, longitude, confidence and surface temperature of a location
- **Producer 3:** Feed the TERRA data to the stream every 2 seconds. TERRA is another satellite from NASA that reports THE latitude, longitude, confidence and surface temperature of a location
- **Streaming Application:** Written using the Apache Spark Structured Streaming API which processes data in batches of 10 seconds. The streaming application will receive streaming data from all three producers and process it as follows:
  - You can find if two locations are close to each other or not by using the [Geo-hashing](https://en.wikipedia.org/wiki/Geohash) algorithm using the precision of 3.  If the climate data and hotspot data are not close to each other we can ignore the hotspot data and just store the climate data.
  - If the streaming application has the data from only one producer (Producer 1), it implies that there was no fire at that time and we can store the climate data into MongoDB straight away.
  - If we receive the data from two different satellites AQUA and TERRA for the same location (to determine whether the two locations are the same or not, we will use Geohash with precision 5), then average the ‘surface temperature’ and ‘confidence’ from the two satellites and save it as a fire event.
  - If a fire was detected with an air temperature greater than 20 (°C) and a GHI greater than 180 (W/m2), then report the cause of the fire event as ‘natural’. Otherwise, report the cause of the fire event as ‘other’.

