from flask import Flask, render_template
import sqlite3
import os

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def fetch_last_entries(limit=5):
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
        ORDER BY timestamp DESC
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


if __name__ == "__main__":
    app.run(debug=True)
