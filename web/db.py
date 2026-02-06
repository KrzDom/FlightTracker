import os
import json
import gzip
import sqlite3
import hashlib
from datetime import datetime


# Get the path relative to this file (db.py)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")
DB_PATH_RAW = os.path.join(BASE_DIR, "data", "raw_data.db")


def get_current_time_slot():
    now = datetime.now().hour
    time_slot = (now - 23) % 24 // 4
    return time_slot


def compress_json(data: dict) -> bytes:
    raw = json.dumps(data).encode("utf-8")
    return gzip.compress(raw)


def hash_json(data: dict, timestamp) -> str:
    raw = json.dumps({"data": data, "timestamp": timestamp},
                     sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def connect_db_raw(db_path_raw=DB_PATH_RAW):
    conn = sqlite3.connect(db_path_raw)
    cursor = conn.cursor()
    # create flights tables if not exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_api_responses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        query_date TEXT NOT NULL,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        departure_date TEXT NOT NULL,

        response_gzip BLOB NOT NULL,
        response_hash TEXT NOT NULL
        )
    """)

    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_response_hash
        ON raw_api_responses (response_hash)
    """)

    conn.commit()
    return conn


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
        departure_time_slot INTEGER,
        arrivalDate TEXT,

        departure_dow INTEGER,
        is_weekend INTEGER,
        week_of_year INTEGER,
        month INTEGER,
        year INTEGER,
        is_holiday INTEGER
        )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        flight_id TEXT,
        query_date TEXT,

        price REAL,
        currencyCode TEXT,
        currencySymbol TEXT,
        days_before_departure INTEGER,
        query_dow INTEGER,
        query_time_slot INTEGER,

        PRIMARY KEY(flight_id, query_date),
        FOREIGN KEY(flight_id) REFERENCES flights(id)
        )
    """)
    conn.commit()
    return conn


def save_flights(conn, all_flights):
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    query_dow = datetime.now().weekday() 
    query_time_slot = get_current_time_slot()

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
                departure_time_slot,
                arrivalDate,

                departure_dow,
                is_weekend,
                week_of_year,
                month,
                year,
                is_holiday
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            info.get("departure_time_slot"),
            info.get("arrivalDate"),

            info.get("departure_dow"),
            info.get("is_weekend"),
            info.get("week_of_year"),
            info.get("month"),
            info.get("year"),
            info.get("is_holiday")
        ))

        # -------- insert into prices table --------
        cursor.execute("""
            INSERT OR IGNORE INTO prices (
                flight_id,
                query_date,
                price,
                currencyCode,
                currencySymbol,
                days_before_departure,
                query_dow,
                query_time_slot
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            flight_id,
            now,
            info.get("price"),
            info.get("currencyCode"),
            info.get("currencySymbol"),
            info.get("days_before_departure"),
            query_dow,
            query_time_slot
        ))

    conn.commit()


def save_raw_data(conn, api_response, origin, destination, departure_date):
    now = datetime.now().isoformat()
    cursor = conn.cursor()
    compressed = compress_json(api_response)
    response_hash = hash_json(api_response, now)

    cursor.execute("""
        INSERT INTO raw_api_responses (
            query_date,
            origin,
            destination,
            departure_date,
            response_gzip,
            response_hash
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        now,
        origin,
        destination,
        departure_date,
        compressed,
        response_hash
    ))
    conn.commit()
