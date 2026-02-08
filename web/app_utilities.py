import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_statistics():
    '''
    Extracts:
        - Most expensive flight
        - Cheapest flight
        - Total number of flights
        - Total number of prices
        - Average price
    returns a dict
    '''
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Most expensive flight
    cursor.execute("SELECT * FROM prices ORDER BY price DESC LIMIT 1")
    expensive_flight = cursor.fetchone()
    price_expensive_flight = \
        expensive_flight["price"] if expensive_flight else 0

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
    average_price = round(row["avg_price"], 2) \
        if row and row["avg_price"] is not None else 0

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


def get_all_flights_with_average_price():
    '''
    Returns a list of all flights with some Information and the average price for all query
    '''
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all flights
    cursor.execute("""
        SELECT id, flight_number, departureAirport_cityName,
                   arrivalAirport_cityName, departureDate
        FROM flights
    """)
    flights_rows = cursor.fetchall()

    # Build nested dictionary
    flights_dict = {}

    for flight in flights_rows:
        flight_id = flight["id"]

        # Get all price queries for this flight using the id
        cursor.execute("""
            SELECT ROUND(AVG(price),2) AS avg_price, COUNT(*) AS num_queries
            FROM prices
            WHERE flight_id = ?
            """, (flight_id,))
        row = cursor.fetchone()
        average_price = round(row["avg_price"], 2) \
            if row and row["avg_price"] is not None else 0
        num_price_queries = row["num_queries"] \
            if row and row["num_queries"] is not None else 0

        # Build dictionary entry
        flights_dict[flight_id] = {
            "flight_number": flight["flight_number"],
            "departureAirport_cityName": flight["departureAirport_cityName"],
            "arrivalAirport_cityName": flight["arrivalAirport_cityName"],
            "departureDate": flight["departureDate"],
            "average_price": average_price,
            "num_price_queries": num_price_queries
        }

    conn.close()
    return flights_dict


def fetch_all_entries():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM flights "
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


def fetch_prices_sorted_by_flight_dow():
    """Fetch all price entries sorted by the flight's day of week."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            prices.*,
            flights.departure_dow
        FROM prices
        JOIN flights
            ON flights.id = prices.flight_id
        ORDER BY flights.departure_dow ASC, prices.query_date DESC
    """)
    prices_rows = cursor.fetchall()
    prices = [dict(row) for row in prices_rows]

    conn.close()
    return prices


def fetch_prices_sorted_by_query_dow():
    """Fetch all price entries sorted by the query day of week."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM prices
        ORDER BY request_dow ASC, query_date DESC
    """)
    prices_rows = cursor.fetchall()
    prices = [dict(row) for row in prices_rows]

    conn.close()
    return prices


def fetch_avg_price_dbd():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            days_before_departure,
            ROUND(AVG(price), 2) AS avg_price,
            COUNT(*) AS num_samples
        FROM prices
        GROUP BY days_before_departure
        ORDER BY days_before_departure DESC
    """)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["days_before_departure", "avg_price", "num_samples"])

    return df


def fetch_pricing_matrices():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # --- Query prices matrix ---
    cursor.execute("""
        SELECT
            query_time_slot AS time_slot,
            query_dow AS day_of_week,
            ROUND(AVG(price), 2) AS avg_price
        FROM prices
        GROUP BY query_time_slot, query_dow
        ORDER BY query_time_slot, query_dow
    """)
    rows = cursor.fetchall()
    df_queries = pd.DataFrame(rows, columns=["time_slot", "day_of_week", "avg_price"])
    matrix_queries = df_queries.pivot(
        index='time_slot',
        columns='day_of_week',
        values='avg_price'
    )

    # Ensure full 6x7 matrix (0-5 time slots, 0-6 days of week)
    matrix_queries = matrix_queries.reindex(index=range(6), columns=range(7))

    # --- Departure prices matrix ---
    cursor.execute("""
        SELECT
            f.departure_time_slot AS time_slot,
            f.departure_dow AS day_of_week,
            ROUND(AVG(p.price), 2) AS avg_price
        FROM flights f
        JOIN prices p ON f.id = p.flight_id
        GROUP BY f.departure_time_slot, f.departure_dow
        ORDER BY f.departure_time_slot, f.departure_dow
    """)
    rows = cursor.fetchall()
    df_flights = pd.DataFrame(rows, columns=["time_slot", "day_of_week", "avg_price"])
    print(df_flights)
    matrix_flights = df_flights.pivot(
        index='time_slot',
        columns='day_of_week',
        values='avg_price'
    )

    # Ensure full 6x7 matrix
    matrix_flights = matrix_flights.reindex(index=range(6), columns=range(7))

    conn.close()
    return matrix_queries, matrix_flights


def fetch_price_development_by_dow():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT
            p.days_before_departure,
            f.departure_dow AS day_of_week,
            AVG(p.price) AS avg_price
        FROM flights f
        JOIN prices p ON f.id = p.flight_id
        GROUP BY p.days_before_departure, f.departure_dow
        ORDER BY p.days_before_departure
    """

    df = pd.read_sql(query, conn)
    conn.close()

    return df
