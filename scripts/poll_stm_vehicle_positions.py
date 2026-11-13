from config import APIS
from gtfs_realtime.poller import Poller
from gtfs_realtime.parsers.vehicle_positions import process as process_vehicle_positions


if __name__ == "__main__":
    poller_stm = Poller(agency_id="STM", url=APIS["STM"]['vehicle_positions'], process_func=process_vehicle_positions, api_key=APIS["STM"]['key'])
    poller_stm.poll()