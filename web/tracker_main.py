import time
import requests
from datetime import datetime, timedelta

from db import connect_db, connect_db_raw, save_flights, save_raw_data
from tracker_utilitis import get_flight, parse_response, check_flight_exists


# --- Flight info ---
origin = "VLC"
destination = "BER"
dict = {}
days_to_track = 250

try:
    for n in range(days_to_track):
        # print(F"{n}/{days_to_track}")
        departure_date = datetime.today() + timedelta(days=n)
        departure_date_str = departure_date.strftime("%Y-%m-%d")

        data = get_flight(origin, destination, departure_date_str)
        conn = connect_db_raw()
        if check_flight_exists(data):
            save_raw_data(conn, data, origin, destination, departure_date_str)
            dict.update(parse_response(data))
        time.sleep(2)

    conn = connect_db()
    save_flights(conn, dict)

    requests.post("https://ntfy.sh/Krzysztof_is_doing_1234",
                  data="Tracker susccesful".encode(encoding='utf-8'))

except Exception as e:
    requests.post("https://ntfy.sh/Krzysztof_is_doing_1234",
                  data="Tracker unsusccesful".encode(encoding='utf-8'))
    raise
