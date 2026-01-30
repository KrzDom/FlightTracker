import time
import random
from datetime import datetime, timedelta

from db import connect_db, save_flights
from tracker_utilitis import get_flight, parse_response, check_flight_exists


# --- Flight info ---
origin = "VLC"
destination = "BER"
dict = {}
days_to_track = 50

for n in range(days_to_track):
    departure_date = datetime.today() + timedelta(days=n)
    departure_date_str = departure_date.strftime("%Y-%m-%d")

    print(departure_date_str)
    data = get_flight(origin, destination, departure_date_str)
    if check_flight_exists(data):
        dict.update(parse_response(data))
    # wait_time = random.randint(1, 3)
    # print(f"Waiting {wait_time}s to avoid API blocking...")
    time.sleep(2)


print(dict)

conn = connect_db()
save_flights(conn, dict)
