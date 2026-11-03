A small script to ping the Montreal STM GTFS-Realtime feed. 

```yaml
[id: "40065"
vehicle {
  trip {
    trip_id: "302189930"
    start_date: "20261102"
    route_id: "138"
    direction_id: 1
  }
  position {
    latitude: 45.4692841
    longitude: -73.6418228
    bearing: 298
    speed: 1.3889
  }
  current_stop_sequence: 34
  current_status: IN_TRANSIT_TO
  timestamp: 1793656044
  congestion_level: UNKNOWN_CONGESTION_LEVEL
  stop_id: "50636"
  vehicle {
    id: "40065"
    wheelchair_accessible: WHEELCHAIR_ACCESSIBLE
  }
  occupancy_status: STANDING_ROOM_ONLY
}...
```

The static data model looks like...

![gtfs-erd.png](images/gtfs-erd.png)
