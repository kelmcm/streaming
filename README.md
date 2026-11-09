# Streaming Transit Data

A streaming data pipeline for transit data. This repository holds python packages and scripts that:
- Poller - Read and extract data from GTFS real-time transit feed. Sends data to a Kafka topic.
- Producer - Receives messages from one or many GTFS feeds and publishes to topic. One topic per feed type.
- Consumer - Subscribes to the Kafka topic and reads messages and pushes to web socket
- Display - Website that opens websocket and receives later


The static data model looks like...

![gtfs-erd.png](images/gtfs-erd.png)

