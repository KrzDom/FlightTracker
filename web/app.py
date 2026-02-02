from flask import Flask, render_template
import sqlite3
import os

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_all_flights_with_latest_price():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1️⃣ Get all flights
    cursor.execute("""
        SELECT id, flight_number, departureAirport_cityName, arrivalAirport_cityName, departureDate
        FROM flights
    """)
    flights_rows = cursor.fetchall()

    # 2️⃣ Build nested dictionary
    flights_dict = {}

    for flight in flights_rows:
        flight_id = flight["id"]

        # 3️⃣ Get latest price for this flight using the timestamp
        cursor.execute("""
            SELECT price, days_before_departure
            FROM prices
            WHERE flight_id = ?
            ORDER BY query_date DESC
            LIMIT 1
        """, (flight_id,))
        price_row = cursor.fetchone()
        latest_price = price_row["price"] if price_row else None
        days_before_departure = price_row["days_before_departure"] if price_row else None

        # Build dictionary entry
        flights_dict[flight_id] = {
            "flight_number": flight["flight_number"],
            "departureAirport_cityName": flight["departureAirport_cityName"],
            "arrivalAirport_cityName": flight["arrivalAirport_cityName"],
            "departureDate": flight["departureDate"],
            "latest_price": latest_price,
            "days_before_departure": days_before_departure
        }

    conn.close()
    return flights_dict



def get_statistics():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Most expensive flight
    cursor.execute("SELECT * FROM prices ORDER BY price DESC LIMIT 1")
    expensive_flight = cursor.fetchone()
    price_expensive_flight = expensive_flight["price"] if expensive_flight else 0

    # Cheapest flight
    cursor.execute("SELECT * FROM prices ORDER BY price ASC LIMIT 1")
    cheapest_flight = cursor.fetchone()
    price_cheapest_flight = cheapest_flight["price"] if cheapest_flight else 0

    # Total number of flights
    cursor.execute("SELECT COUNT(*) AS total_flights FROM flights")
    row = cursor.fetchone()
    total_flights = row["total_flights"] if row else 0
    
    # Total number of prices
    cursor.execute("SELECT COUNT(*) AS total_prices FROM prices")
    row = cursor.fetchone()
    total_prices = row["total_prices"] if row else 0

    # Average price of Price
    cursor.execute("SELECT AVG(price) AS avg_price FROM prices")
    row = cursor.fetchone()
    average_price = round(row["avg_price"], 2) if row and row["avg_price"] is not None else 0

    # Close the connection
    conn.close()

    # Combine everything in a dictionary
    statistics_dict = {
        "cheapest_flight": price_cheapest_flight,
        "expensive_flight": price_expensive_flight,
        "total_flights": total_flights,
        "average_price": average_price,
        "total_prices": total_prices
    }

    return statistics_dict


def fetch_all_entries():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM flights " \
    "ORDER BY departureDate DESC")

    flights_rows = cursor.fetchall()
    flights = [dict(row) for row in flights_rows]
    return flights


def fetch_last_entries(limit=15):
    """Fetch last `limit` entries from flights and prices separately."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # ---- flights ----
    cursor.execute(f"""
        SELECT *
        FROM flights
        ORDER BY departureDate DESC
        LIMIT ?
    """, (limit,))
    flights_rows = cursor.fetchall()
    flights = [dict(row) for row in flights_rows]

    # ---- prices ----
    cursor.execute(f"""
        SELECT *
        FROM prices
        ORDER BY query_date DESC
        LIMIT ?
    """, (limit,))
    prices_rows = cursor.fetchall()
    prices = [dict(row) for row in prices_rows]

    conn.close()
    return flights, prices


@app.route("/report")
def report():
    flights, prices = fetch_last_entries(limit=5)
    return render_template("report.html", flights=flights, prices=prices)


@app.route("/")
def home():
    statistics_dict = get_statistics()
    flights_dict = get_all_flights_with_latest_price()
    return render_template("home.html", stats=statistics_dict, flights=flights_dict)


if __name__ == "__main__":
    app.run(debug=True)
