import json
import sqlite3
import requests
from datetime import datetime, timedelta


def check_flight_exists(data):
    """
    Returns True if the API response contains at least one flight.
    Returns False if no flights are available or data is malformed.
    """
    return data.get('total', 0) > 0


def parse_response(data):
    """
    Extracts all needed flight and price information and returns a dict
    with a human-readable unique flight_id as key.

    Example flight_id:
    FR642_20260325T0600_VLC_STN
    """

    # Handle case where no fares are returned
    if "fares" not in data or not data["fares"]:
        return None

    fare = data["fares"][0]  # assuming one flight
    outbound = fare.get("outbound", {})

    departure_airport = outbound.get("departureAirport", {})
    arrival_airport = outbound.get("arrivalAirport", {})
    price_info = outbound.get("price", {})
    departure_city = departure_airport.get("city", {})
    arrival_city = arrival_airport.get("city", {})

    flight_number = outbound.get("flightNumber")
    departure_date = outbound.get("departureDate")
    arrival_date = outbound.get("arrivalDate")

    origin_iata = departure_airport.get("iataCode")
    arrival_iata = arrival_airport.get("iataCode")

    price_value = price_info.get("value")
    currency_code = price_info.get("currencyCode")
    currency_symbol = price_info.get("currencySymbol")

    # create a human-readable unique ID
    departure_safe = departure_date.replace(":", "").replace("-", "") if departure_date else "unknown"
    flight_id = f"{flight_number}_{departure_safe}_{origin_iata}_{arrival_iata}"

    return {
        flight_id: {
            # ---- flights table ----
            "flight_number": flight_number,

            "departure": departure_date,
            "arrival": arrival_iata,

            "departureAirport_countryName": departure_airport.get("countryName"),
            "departureAirport_cityName": departure_city.get("name"),
            "departureAirport_iataCode": departure_airport.get("iataCode"),
            "departureAirport_macCode": departure_city.get("macCode"),
            "departureAirport_seoName": departure_airport.get("seoName"),

            "arrivalAirport_countryName": arrival_airport.get("countryName"),
            "arrivalAirport_cityName": arrival_city.get("name"),
            "arrivalAirport_iataCode": arrival_airport.get("iataCode"),
            "arrivalAirport_macCode": arrival_city.get("macCode"),
            "arrivalAirport_seoName": arrival_airport.get("seoName"),

            "departureDate": departure_date,
            "arrivalDate": arrival_date,

            # ---- prices table ----
            "price": price_value,
            "currency": currency_code,
            "currencyCode": currency_code,
            "currencySymbol": currency_symbol,
        }
    }



def get_flight(origin, destination, departure_date):
    '''
    calls api gets date, origin, destination
    '''

    url = (
        f"https://services-api.ryanair.com/farfnd/3/oneWayFares?"
        f"&departureAirportIataCode={origin}"
        f"&arrivalAirportIataCode={destination}"
        f"&language=en"
        f"&limit=16"
        f"&market=en-gb"
        f"&offset=0"
        f"&outboundDepartureDateFrom={departure_date}"
        f"&outboundDepartureDateTo={departure_date}"
    )

    response = requests.get(url)
    data = response.json()
    return data
