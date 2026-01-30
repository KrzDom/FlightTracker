import os
import sqlite3
from datetime import datetime


# Get the path relative to this file (db.py)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")

def connect_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # create flights tables if not exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS flights (
        id TEXT PRIMARY KEY,
        flight_number TEXT,

        departureAirport_countryName TEXT,
        departureAirport_cityName TEXT,
        departureAirport_iataCode TEXT,
        departureAirport_macCode TEXT,
        departureAirport_seoName TEXT,

        arrivalAirport_countryName TEXT,
        arrivalAirport_cityName TEXT,
        arrivalAirport_iataCode TEXT,
        arrivalAirport_macCode TEXT,
        arrivalAirport_seoName TEXT,

        departureDate TEXT,
        arrivalDate TEXT
        )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        flight_id TEXT,
        price REAL,
        currencyCode TEXT,
        currencySymbol TEXT,
                   
        timestamp TEXT,
        PRIMARY KEY(flight_id, timestamp),
        FOREIGN KEY(flight_id) REFERENCES flights(id)
        )
    """)
    conn.commit()
    return conn


def save_flights(conn, all_flights):
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    for flight_id, info in all_flights.items():

        # -------- insert into flights table --------
        cursor.execute("""
            INSERT OR IGNORE INTO flights (
                id,
                flight_number,
                
                departureAirport_countryName,
                departureAirport_cityName,
                departureAirport_iataCode,
                departureAirport_macCode,
                departureAirport_seoName,

                arrivalAirport_countryName,
                arrivalAirport_cityName,
                arrivalAirport_iataCode,
                arrivalAirport_macCode,
                arrivalAirport_seoName,

                departureDate,
                arrivalDate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            flight_id,
            info.get("flight_number"),
            
            info.get("departureAirport_countryName"),
            info.get("departureAirport_cityName"),
            info.get("departureAirport_iataCode"),
            info.get("departureAirport_macCode"),
            info.get("departureAirport_seoName"),

            info.get("arrivalAirport_countryName"),
            info.get("arrivalAirport_cityName"),
            info.get("arrivalAirport_iataCode"),
            info.get("arrivalAirport_macCode"),
            info.get("arrivalAirport_seoName"),

            info.get("departureDate"),
            info.get("arrivalDate"),
        ))

        # -------- insert into prices table --------
        cursor.execute("""
            INSERT OR IGNORE INTO prices (
                flight_id,
                timestamp,
                price,
                currencyCode,
                currencySymbol
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            flight_id,
            now,
            info.get("price"),
            info.get("currencyCode"),
            info.get("currencySymbol"),
        ))

    conn.commit()
