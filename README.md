A small script to ping the Montreal STM GTFS-Realtime feed.

The static data model looks like...

![gtfs-erd.png](images/gtfs-erd.png)

Pipeline: 
- Poller - Read GTFS feed & push to Kafka topic - Always running script on a server
- Producer - Receives messages from one or many GTFS feeds and publishes to topic. One topic per feed type. 
- Consumer - Subscribes to the Kafka topic and reads messages and pushes to web socket
- Display - Website that opens websocket and receives later