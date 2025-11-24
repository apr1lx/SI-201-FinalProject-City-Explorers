# ============================================================
# Final Project Starter File
# API Keys and Base URLs for: OpenWeatherMap, OpenAQ, GeoDB Cities
# ============================================================

# -----------------------------
# API KEYS (fill these in)
# -----------------------------
# OpenWeatherMap requires an API key
OPENWEATHER_API_KEY = "adb50d52c8775272ca4a7fc399f99e2f"

# OpenAQ does NOT require an API key
OPENAQ_API_KEY = None

# GeoDB Free GraphQL API does NOT require any key
GEODB_API_KEY = None

# -----------------------------
# BASE URLS
# -----------------------------
# OpenWeatherMap (Weather)
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/"

# OpenAQ (Air Quality)
OPENAQ_BASE_URL = "https://api.openaq.org/v2/"

# GeoDB Free GraphQL API (NO API KEY REQUIRED)
GEODB_BASE_URL = "http://geodb-free-service.wirefreethought.com/graphql"

# ============================================================
# IMPORTS
# ============================================================
import requests
import sqlite3
import json
from create_database import create_database
import matplotlib.pyplot as plt


# ============================================================
# FETCH FUNCTIONS (to be completed by each team member)
# ============================================================

def fetch_weather(city_list):
    """Fetch weather data for a list of cities from OpenWeatherMap."""
    # TODO: April fills this in
    pass


def fetch_air_quality(city_list):
    """Fetch air quality (PM2.5) for each city from OpenAQ."""
    # TODO: Kyndal fills this in
    """Fetch air quality (PM2.5) for each city from OpenAQ."""
    results = []

    for city in city_list:
        # Build the request parameters for OpenAQ
        params = {
            "city": city,
            "parameter": "pm25",
            "limit": 1
        }

        try:
            # Call the OpenAQ /latest endpoint
            response = requests.get(OPENAQ_BASE_URL + "latest", params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            # If anything goes wrong, print the error and skip this city
            print(f"Error fetching air quality for {city}: {e}")
            continue

        # Make sure we actually got results back
        results_list = data.get("results", [])
        if not results_list:
            continue

        first_result = results_list[0]

        # Measurements is a list of different pollutants; we want pm25
        measurements = first_result.get("measurements", [])
        pm25_value = None
        pm25_unit = None

        for m in measurements:
            if m.get("parameter") == "pm25":
                pm25_value = m.get("value")
                pm25_unit = m.get("unit")
                break

        # If we never found a pm25 measurement, skip this city
        if pm25_value is None:
            continue

        # Coordinates and location name for the station
        coords = first_result.get("coordinates", {})
        location_name = first_result.get("location")

        results.append({
            "city": city,
            "location": location_name,
            "latitude": coords.get("latitude"),
            "longitude": coords.get("longitude"),
            "pm25": pm25_value,
            "unit": pm25_unit
        })

    return results


def fetch_city_data(limit=10, min_population=50000):
     # TODO: Sarah fills this in
    """
    Fetch city metadata (name, country, population, coordinates)
    from the GeoDB Free GraphQL API. No API key required.
    """
    query = f"""
    {{
      cities(limit: {limit}, offset: 0, minPopulation: {min_population}) {{
        id
        name
        country
        region
        population
        latitude
        longitude
      }}
    }}
    """

    response = requests.post(GEODB_BASE_URL, json={"query": query})

    if response.status_code != 200:
        print("Error fetching GeoDB Cities data:", response.text)
        return []

    data = response.json()
    return data.get("data", {}).get("cities", [])


# ============================================================
# STORE FUNCTIONS
# ============================================================

def store_weather_data(conn, weather_data):
    """Insert weather data into Cities + WeatherObservations tables."""
    # TODO: April fills this in
    pass


def store_air_quality_data(conn, aq_data):
    """Insert air-quality station + measurement data."""
    # TODO: Kyndal fills this in
    ## test test 
    """Insert air-quality station + measurement data."""
    cur = conn.cursor()

    # Create tables if they do not exist yet
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityStations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            location TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityMeasurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            pm25 REAL,
            unit TEXT,
            FOREIGN KEY (station_id) REFERENCES AirQualityStations(id)
        )
    """)

    # Insert one station row + one measurement row per item in aq_data
    for item in aq_data:
        city = item.get("city")
        location = item.get("location")
        lat = item.get("latitude")
        lon = item.get("longitude")
        pm25 = item.get("pm25")
        unit = item.get("unit")

        # Insert station
        cur.execute("""
            INSERT INTO AirQualityStations (city, location, latitude, longitude)
            VALUES (?, ?, ?, ?)
        """, (city, location, lat, lon))

        station_id = cur.lastrowid  # ID of the station we just inserted

        # Insert measurement linked to that station
        cur.execute("""
            INSERT INTO AirQualityMeasurements (station_id, pm25, unit)
            VALUES (?, ?, ?)
        """, (station_id, pm25, unit))

    conn.commit()


def store_city_data(conn, city_data):
    """Insert GeoDB city metadata into GeoCities + CityDetails tables."""
    # TODO: Sarah fills this in
    pass


# ============================================================
# ANALYSIS
# ============================================================

def calculate_city_stats(conn):
    """Combine weather, air quality, and city metadata into per-city stats."""
    # TODO: Kyndal + Sarah fill this in
    """Combine weather, air quality, and city metadata into per-city stats."""
    cur = conn.cursor()

    # NOTE: You may need to tweak table/column names to match your actual schema.
    query = """
        SELECT
            c.name AS city,
            AVG(w.temperature) AS avg_temp,
            AVG(aqm.pm25) AS avg_pm25,
            g.population AS population
        FROM Cities AS c
        JOIN WeatherObservations AS w
            ON w.city_id = c.id
        JOIN AirQualityStations AS aqs
            ON aqs.city = c.name
        JOIN AirQualityMeasurements AS aqm
            ON aqm.station_id = aqs.id
        LEFT JOIN GeoCities AS g
            ON g.name = c.name
        GROUP BY c.name
        ORDER BY c.name
    """

    cur.execute(query)
    rows = cur.fetchall()

    city_stats = []
    for row in rows:
        city_stats.append({
            "city": row[0],
            "avg_temp": row[1],
            "avg_pm25": row[2],
            "population": row[3]
        })

    return city_stats


# ============================================================
# VISUALIZATIONS
# ============================================================

def plot_temp_vs_pm25(city_stats):
    """Scatter plot of avg temperature vs avg PM2.5."""
    # TODO: Kyndal fills this in
    # Filter out any cities that are missing temp or pm25
    temps = []
    pm25_values = []
    labels = []

    for city_info in city_stats:
        avg_temp = city_info.get("avg_temp")
        avg_pm25 = city_info.get("avg_pm25")

        if avg_temp is None or avg_pm25 is None:
            continue

        temps.append(avg_temp)
        pm25_values.append(avg_pm25)
        labels.append(city_info.get("city"))

    # Nothing to plot? Just return.
    if not temps or not pm25_values:
        print("No data available to plot temperature vs PM2.5.")
        return

    plt.figure()
    plt.scatter(temps, pm25_values)

    # Label each point with the city name (small text so it doesn't get too messy)
    for x, y, label in zip(temps, pm25_values, labels):
        plt.text(x, y, label, fontsize=8)

    plt.xlabel("Average Temperature")
    plt.ylabel("Average PM2.5")
    plt.title("Average Temperature vs Average PM2.5 by City")
    plt.tight_layout()
    plt.show()



def plot_population_vs_pm25(city_stats):
    """Scatter plot of population vs PM2.5."""
    # TODO: Sarah fills this in
    pass


def plot_city_characteristics(city_stats):
    """Bar chart of city characteristics (e.g., population or elevation) with air-quality categories."""
    # TODO: April fills this in
    pass


# ============================================================
# WRITE RESULTS TO FILE
# ============================================================

def write_results_to_file(city_stats, filename="results.txt"):
    """Write final calculated statistics to a text file."""
    # TODO: April fills this in
    pass


# ============================================================
# MAIN FUNCTION
# ============================================================

def main():
    """Run all steps of the project in order."""
    create_database("final_project.db")

    # TODO: Put the workflow here
    pass


# ============================================================
# TEST CASE TEMPLATES (do NOT fill in yet)
# ============================================================

# -----------------------------
# Test Cases for April
# -----------------------------
def test_fetch_weather():
    """Test template for fetch_weather (April)."""
    # TODO: Valid city list, invalid city handling, response structure
    pass

def test_store_weather_data():
    """Test template for store_weather_data (April)."""
    # TODO: Insert rows, link to Cities table
    pass

def test_plot_city_characteristics():
    """Test template for plot_city_characteristics (April)."""
    # TODO: Chart creation, missing data handling
    pass

def test_write_results_to_file():
    """Test template for write_results_to_file (April)."""
    # TODO: File creation + formatting
    pass


# -----------------------------
# Test Cases for Kyndal
# -----------------------------
def test_fetch_air_quality():
    """Test template for fetch_air_quality (Kyndal)."""
    # TODO: Valid response, empty dataset handling
    pass

def test_store_air_quality_data():
    """Test template for store_air_quality_data (Kyndal)."""
    # TODO: Insert station + measurement rows
    pass

def test_plot_temp_vs_pm25():
    """Test template for plot_temp_vs_pm25 (Kyndal)."""
    # TODO: Scatter creation, missing values
    pass


# -----------------------------
# Test Cases for Sarah
# -----------------------------
def test_fetch_city_data():
    """Test template for fetch_city_data (Sarah)."""
    # TODO: Valid GraphQL response, filtering, field structure
    pass

def test_store_city_data():
    """Test template for store_city_data (Sarah)."""
    # TODO: Insert GeoCities + CityDetails rows
    pass

def test_plot_population_vs_pm25():
    """Test template for plot_population_vs_pm25 (Sarah)."""
    # TODO: Scatter creation, missing population
    pass


# -----------------------------
# Combined Function Tests
# -----------------------------
def test_calculate_city_stats():
    """Test template for calculate_city_stats (Kyndal + Sarah)."""
    # TODO: Join all three APIs, compute averages & metrics
    pass


# ============================================================
# RUN MAIN
# ============================================================

if __name__ == "__main__":
    main()
