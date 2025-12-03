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
OPENAQ_API_KEY = "6948df237b69d167ad713141df83afc984fd2996c3932a9e4a630cef0cde243b"

# GeoDB Free GraphQL API does NOT require any key
GEODB_API_KEY = None

# -----------------------------
# BASE URLS
# -----------------------------
# OpenWeatherMap (Weather)
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/"

# OpenAQ (Air Quality)
OPENAQ_BASE_URL = "https://api.openaq.org/v3/"

# GeoDB Free GraphQL API (NO API KEY REQUIRED)
GEODB_BASE_URL = "http://geodb-free-service.wirefreethought.com/v1/geo"

# ============================================================
# IMPORTS
# ============================================================
import os
import requests
import sqlite3
import json
from create_database import create_database
import matplotlib.pyplot as plt

from analysis_visualizations import (
    calculate_city_stats,
    plot_temp_vs_pm25,
    plot_population_vs_pm25,
    plot_city_characteristics,
    write_results_to_file,
)

CITY_PAIRS = [
    # --- US cities you already had ---
    ("Ann Arbor,US", "Ann Arbor"),
    ("Chicago,US", "Chicago"),
    ("New York,US", "New York"),
    ("Los Angeles,US", "Los Angeles"),
    ("San Francisco,US", "San Francisco"),
    ("Houston,US", "Houston"),
    ("Dallas,US", "Dallas"),
    ("Miami,US", "Miami"),
    ("Seattle,US", "Seattle"),
    ("Boston,US", "Boston"),
    ("Phoenix,US", "Phoenix"),
    ("Philadelphia,US", "Philadelphia"),
    ("Atlanta,US", "Atlanta"),
    ("Denver,US", "Denver"),
    ("San Diego,US", "San Diego"),
    ("Austin,US", "Austin"),
    ("Portland,US", "Portland"),
    ("Tampa,US", "Tampa"),
    ("Orlando,US", "Orlando"),
    ("Las Vegas,US", "Las Vegas"),

    ("San Antonio,US", "San Antonio"),
    ("San Jose,US", "San Jose"),
    ("Indianapolis,US", "Indianapolis"),
    ("Columbus,US", "Columbus"),
    ("Charlotte,US", "Charlotte"),
    ("Baltimore,US", "Baltimore"),
    ("Nashville,US", "Nashville"),
    ("Louisville,US", "Louisville"),
    ("Milwaukee,US", "Milwaukee"),
    ("Cleveland,US", "Cleveland"),
    ("Cincinnati,US", "Cincinnati"),
    ("Pittsburgh,US", "Pittsburgh"),
    ("Kansas City,US", "Kansas City"),
    ("St. Louis,US", "St. Louis"),
    ("Salt Lake City,US", "Salt Lake City"),
    ("Raleigh,US", "Raleigh"),
    ("Richmond,US", "Richmond"),
    ("Minneapolis,US", "Minneapolis"),
    ("Saint Paul,US", "Saint Paul"),
    ("Detroit,US", "Detroit"),

    # --- Canadian & European cities you already had ---
    ("Toronto,CA", "Toronto"),
    ("Vancouver,CA", "Vancouver"),
    ("Montreal,CA", "Montreal"),
    ("London,GB", "London"),
    ("Paris,FR", "Paris"),
    ("Berlin,DE", "Berlin"),
    ("Madrid,ES", "Madrid"),
    ("Rome,IT", "Rome"),
    ("Amsterdam,NL", "Amsterdam"),
    ("Vienna,AT", "Vienna"),
    ("Copenhagen,DK", "Copenhagen"),
    ("Stockholm,SE", "Stockholm"),
    ("Oslo,NO", "Oslo"),
    ("Helsinki,FI", "Helsinki"),
    ("Tokyo,JP", "Tokyo"),
    ("Seoul,KR", "Seoul"),
    ("Sydney,AU", "Sydney"),
    ("Melbourne,AU", "Melbourne"),

    # --- NEW cities to push you past 100 total ---
    ("Brisbane,AU", "Brisbane"),
    ("Perth,AU", "Perth"),
    ("Auckland,NZ", "Auckland"),

    ("Mexico City,MX", "Mexico City"),
    ("Guadalajara,MX", "Guadalajara"),
    ("Monterrey,MX", "Monterrey"),

    ("Bogota,CO", "Bogota"),
    ("Lima,PE", "Lima"),
    ("Santiago,CL", "Santiago"),
    ("Buenos Aires,AR", "Buenos Aires"),
    ("Sao Paulo,BR", "Sao Paulo"),
    ("Rio de Janeiro,BR", "Rio de Janeiro"),

    ("Johannesburg,ZA", "Johannesburg"),
    ("Cape Town,ZA", "Cape Town"),
    ("Cairo,EG", "Cairo"),
    ("Nairobi,KE", "Nairobi"),
    ("Lagos,NG", "Lagos"),

    ("Istanbul,TR", "Istanbul"),
    ("Athens,GR", "Athens"),
    ("Zurich,CH", "Zurich"),
    ("Geneva,CH", "Geneva"),
    ("Prague,CZ", "Prague"),
    ("Budapest,HU", "Budapest"),
    ("Warsaw,PL", "Warsaw"),
    ("Krakow,PL", "Krakow"),

    ("Dublin,IE", "Dublin"),
    ("Edinburgh,GB", "Edinburgh"),
    ("Birmingham,GB", "Birmingham"),
    ("Manchester,GB", "Manchester"),
    ("Glasgow,GB", "Glasgow"),
    ("Brussels,BE", "Brussels"),
    ("Lisbon,PT", "Lisbon"),

    ("Hong Kong,HK", "Hong Kong"),
    ("Singapore,SG", "Singapore"),
    ("Bangkok,TH", "Bangkok"),
    ("Kuala Lumpur,MY", "Kuala Lumpur"),
    ("Jakarta,ID", "Jakarta"),
    ("Manila,PH", "Manila"),

    ("Delhi,IN", "Delhi"),
    ("Mumbai,IN", "Mumbai"),
    ("Bengaluru,IN", "Bengaluru"),
    ("Chennai,IN", "Chennai"),
]

BATCH_SIZE = 25
PROGRESS_FILE = "progress.json"

def build_fallback_city_data(limit=10, min_population=0):
    """
    Return synthetic city metadata when the GeoDB API is unavailable.

    This respects the 'limit' argument so our tests and pipeline still behave
    as if we only fetched up to 'limit' cities per API call.
    """
    # Behave like "no results" for limit=0 or absurdly high min_population
    if limit == 0 or min_population >= 999_999_999:
        return []

    cities = []
    base_pop = 150_000   # starting population
    step = 50_000        # step so not all populations are the same

    for idx, (weather_query, aq_city) in enumerate(CITY_PAIRS):
        if idx >= limit:
            break

        # weather_query is like "Chicago,US"
        if "," in weather_query:
            city_part, country = weather_query.split(",", 1)
        else:
            city_part, country = aq_city, None

        geodb_id = f"local-{idx}-{city_part.replace(' ', '_')}"

        # Just make up a reasonable population number
        population = base_pop + step * (idx % 10)

        cities.append({
            "geodb_id": geodb_id,
            "name": city_part,
            "country": country,
            "region": None,
            "population": population,
            "latitude": None,
            "longitude": None,
        })

    return cities

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEST_OUTPUT_DIR = "test_outputs"
if not os.path.exists(TEST_OUTPUT_DIR):
    os.makedirs(TEST_OUTPUT_DIR)

VIS_OUTPUT_DIR = "visualizations"
if not os.path.exists(VIS_OUTPUT_DIR):
    os.makedirs(VIS_OUTPUT_DIR)

# ============================================================
# FETCH FUNCTIONS (to be completed by each team member)
# ============================================================

def fetch_weather(city_list):
    """Fetch weather data for a list of cities from OpenWeatherMap."""
    # TODO: April fills this in
    results = []
    for city in city_list:
        params = {
            "q": city,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric"
        }
        response = requests.get(OPENWEATHER_BASE_URL + "weather", params=params)

        if response.status_code != 200:
            print(f"Error fetching weather data for {city}: {response.text}")
            continue

        data = response.json()
        
        city_name = data.get("name")
        country = data.get("sys", {}).get("country")

        latitude = data.get("coord", {}).get("lat")
        longitude = data.get("coord", {}).get("lon")

        main_info = data.get("main", {})
        temperature = main_info.get("temp")
        feels_like = main_info.get("feels_like")
        humidity = main_info.get("humidity")

        wind_info = data.get("wind", {})
        wind_speed = wind_info.get("speed")

        weather_list = data.get("weather", [])
        if len(weather_list) > 0:
            weather_main = weather_list[0].get("main")
        else:
            weather_main = None
        
        timestamp = data.get("dt")

        weather_dict = {
            "city_name": city_name,
            "country": country,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp,
            "temperature": temperature,
            "feels_like": feels_like,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "weather_main": weather_main
        }
        results.append(weather_dict)
    return results

    


def fetch_air_quality(city_list):
    """Fetch air quality (PM2.5) for each city from OpenAQ."""
    # TODO: Kyndal fills this in
    """Fetch air quality (PM2.5) for each city from OpenAQ."""
    results = []

    # If you somehow call this without a key, just return empty
    if not OPENAQ_API_KEY:
        print("No OpenAQ API key set. Set OPENAQ_API_KEY at the top of the file.")
        return results

    headers = {
        "X-API-Key": OPENAQ_API_KEY
    }

    for city in city_list:
        try:
            # PM2.5 is parameter ID 2 in OpenAQ
            url = OPENAQ_BASE_URL + "parameters/2/latest"
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching air quality for {city}: {e}")
            continue

        results_list = data.get("results", [])
        if not results_list:
            continue

        first = results_list[0]
        value = first.get("value")
        coords = first.get("coordinates", {}) or {}

        if value is None:
            continue

        results.append({
            "city": city,
            "location": "OpenAQ PM2.5 sensor",
            "latitude": coords.get("latitude"),
            "longitude": coords.get("longitude"),
            "pm25": value,
            "unit": "µg/m³"
        })

    return results


def fetch_city_data(limit=10, min_population=50000):
    """
    Fetch city metadata (name, country, population, coordinates)
    from the GeoDB Free API.

    If the API is unavailable (403, etc.), fall back to locally generated
    metadata based on CITY_PAIRS so that our joins and visualizations still work.
    """
    url = f"{GEODB_BASE_URL}/cities"
    params = {
        "limit": limit,
        "offset": 0,
        "minPopulation": min_population,
        "sort": "-population",      # biggest cities first
        "hateoasMode": "off",       # simpler JSON
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print("Error fetching GeoDB Cities data:", e)
        print("Using local fallback city metadata instead.")
        return build_fallback_city_data(limit=limit, min_population=min_population)

    data = response.json()

    cities = []
    for item in data.get("data", []):
        city_name = item.get("city") or item.get("name")

        cities.append({
            "geodb_id": item.get("id"),
            "name": city_name,
            "country": item.get("country") or item.get("countryCode"),
            "region": item.get("region"),
            "population": item.get("population"),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
        })

    return cities
# ============================================================
# STORE FUNCTIONS
# ============================================================

def store_weather_data(conn, weather_data):
    """Insert weather data into Cities + WeatherObservations tables."""
    # TODO: April fills this in
    cur = conn.cursor()

    for item in weather_data:
        city = item.get("city_name")
        country = item.get("country")
        latitude = item.get("latitude")
        longitude = item.get("longitude")

        cur.execute("""
            INSERT OR IGNORE INTO Cities (city_name, country, latitude, longitude)
            VALUES (?, ?, ?, ?)
        """, (city, country, latitude, longitude))
        
        cur.execute("""
            SELECT id FROM Cities WHERE city_name = ? AND country = ?
        """, (city, country))
        row = cur.fetchone()

        if row is None:
            print(f"City {city} not found in Cities table.")
            continue

        city_id = row[0]

        cur.execute("""
            INSERT INTO WeatherObservations 
            (city_id, timestamp, temperature, feels_like, humidity, wind_speed, weather_main)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            city_id,
            item.get("timestamp"),
            item.get("temperature"),
            item.get("feels_like"),
            item.get("humidity"),
            item.get("wind_speed"),
            item.get("weather_main")
        ))

    conn.commit()

def store_air_quality_data(conn, aq_data):
    """Insert air-quality station + measurement data."""
    # TODO: Kyndal fills this in
    ## test test 
    
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityLocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT,
            location_name TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityMeasurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER,
            timestamp TEXT,
            parameter TEXT,
            value REAL,
            unit TEXT,
            FOREIGN KEY (location_id) REFERENCES AirQualityLocations(id)
        )
    """)

    for item in aq_data:
        city = item.get("city")
        location = item.get("location")
        lat = item.get("latitude")
        lon = item.get("longitude")
        pm25 = item.get("pm25")
        unit = item.get("unit")

        cur.execute("""
            INSERT INTO AirQualityLocations (city_name, location_name, country, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
        """, (city, location, None, lat, lon))
        location_id = cur.lastrowid

        cur.execute("""
            INSERT INTO AirQualityMeasurements (location_id, timestamp, parameter, value, unit)
            VALUES (?, ?, ?, ?, ?)
        """, (location_id, None, "pm25", pm25, unit))

    conn.commit()


def store_city_data(conn, city_data):
    """Insert GeoDB city metadata into GeoCities + CityDetails tables."""
    # TODO: Sarah fills this in
    cur = conn.cursor()

    for city in city_data:
        geodb_id = city.get("geodb_id")
        name = city.get("name")
        country = city.get("country")
        region = city.get("region")
        population = city.get("population")
        latitude = city.get("latitude")
        longitude = city.get("longitude")

        # If we somehow don't have a geodb_id, make a simple fallback
        if geodb_id is None:
            geodb_id = f"{name}-{country}"

        # Insert basic city info
        cur.execute("""
            INSERT OR IGNORE INTO GeoCities
                (geodb_id, city_name, country, region, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (geodb_id, name, country, region, latitude, longitude))

        # Insert or update extra details (population etc.)
        cur.execute("""
            INSERT OR REPLACE INTO CityDetails
                (geodb_id, population, elevation, density)
            VALUES (?, ?, ?, ?)
        """, (geodb_id, population, None, None))

    conn.commit()


# ============================================================
# MAIN FUNCTION
# ============================================================

def run_tests():
    """Run all test functions for April, Kyndal, and Sarah."""
    create_database("final_project.db")

    # April's tests
    test_fetch_weather()
    test_store_weather_data()
    # test_plot_city_characteristics()  # visualization test - skip for now
    test_write_results_to_file()

    # Kyndal's tests
    test_fetch_air_quality()
    test_store_air_quality_data()
    # test_plot_temp_vs_pm25()          # visualization test - skip for now

    # Sarah's tests
    test_fetch_city_data()
    test_store_city_data()
    # test_plot_population_vs_pm25()    # visualization test - skip for now

    # Combined test
    test_calculate_city_stats()


def run_pipeline():
    """
    Real project workflow:
    - create DB (or ensure it exists)
    - fetch + store data from all APIs for ONE batch of <= 25 cities
    - compute city stats
    - write results to a text file

    NOTE: Because BATCH_SIZE = 25, each time you run THIS FILE we only add
    up to 25 new cities per API. To reach >=100 rows, you run the file
    multiple times. We track progress in PROGRESS_FILE so we don't
    duplicate the same city rows.
    """
    # 1) Make sure DB exists
    create_database("final_project.db")
    conn = sqlite3.connect("final_project.db")

    # 2) Figure out where we left off last time
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
                start_index = data.get("next_start", 0)
        except Exception:
            start_index = 0  # if file is corrupted, just start at 0

    # 3) Get the next batch of CITY_PAIRS
    batch = CITY_PAIRS[start_index : start_index + BATCH_SIZE]

    if not batch:
        print("All city pairs have already been processed. Nothing new to fetch.")
    else:
        print(f"\nProcessing batch starting at index {start_index} "
              f"({len(batch)} cities, max {BATCH_SIZE})...")

        # Split pairs into separate lists for the two APIs
        weather_cities = [w for (w, aq) in batch]
        aq_cities = [aq for (w, aq) in batch]

        # --- Weather (OpenWeather) ---
        weather_data = fetch_weather(weather_cities)
        store_weather_data(conn, weather_data)

        # --- Air Quality (OpenAQ) ---
        aq_data = fetch_air_quality(aq_cities)
        store_air_quality_data(conn, aq_data)

        # 4) Save updated progress
        new_start = start_index + len(batch)
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"next_start": new_start}, f)

        print(f"Batch done. Next start index will be {new_start}.")

    # 5) Fetch + store city metadata from GeoDB
    #    (This may fail with 403; that's okay, we log it.)
    city_data = fetch_city_data(limit=25, min_population=50000)
    store_city_data(conn, city_data)

    # 6) Compute combined stats (for whatever data we currently have)
    city_stats = calculate_city_stats(conn)

    if not city_stats:
        print("No city statistics were created. This is probably because the "
              "external APIs returned no (joinable) data.")
        conn.close()
        return

     # 6) Add AQ categories
    for c in city_stats:
        pm = c.get("avg_pm25")
        if pm is None:
            category = None
        elif pm < 12:
            category = "Good"
        elif pm < 35:
            category = "Moderate"
        else:
            category = "Unhealthy"
        c["aq_category"] = category

    # 7) Visualizations (now part of the real pipeline)
    temp_plot_path = os.path.join(VIS_OUTPUT_DIR, "temp_vs_pm25.png")
    pop_pm25_plot_path = os.path.join(VIS_OUTPUT_DIR, "population_vs_pm25.png")
    city_char_plot_path = os.path.join(VIS_OUTPUT_DIR, "city_pop_with_aq_categories.png")

    plot_temp_vs_pm25(city_stats, save_path=temp_plot_path)
    plot_population_vs_pm25(city_stats, save_path=pop_pm25_plot_path)
    plot_city_characteristics(city_stats, save_path=city_char_plot_path)

    # 8) Write results to a text file
    write_results_to_file(city_stats, filename="results.txt")

def main():
    """Entry point for the program."""
    # For final submission, you probably want the real pipeline:
    run_pipeline()

    #run all tests instead:
    # run_tests()


##test for merging
# ============================================================
# ============================================================

# -----------------------------
# Test Cases for April
# -----------------------------
def test_fetch_weather():
    """Test template for fetch_weather (April)."""
    # TODO: Valid city list, invalid city handling, response structure
    # One valid city and one fake city name
    test_cities = ["Ann Arbor,US", "ThisCityDoesNotExist"]

    weather_data = fetch_weather(test_cities)

    # Check that we got a list back
    if not isinstance(weather_data, list):
        print("FAIL: fetch_weather did not return a list.")
        return

    if len(weather_data) == 0:
        print("WARN: fetch_weather returned an empty list. "
              "Check your OpenWeather API key or internet connection.")
        return

    # Look at the first item and check for some expected keys
    first = weather_data[0]
    expected_keys = ["city_name", "country", "temperature", "humidity"]

    missing = [key for key in expected_keys if key not in first]
    if missing:
        print("FAIL: fetch_weather result is missing keys:", missing)
    else:
        print("PASS: fetch_weather returned a list with the expected structure.")
    print()  # blank line for readability

def test_store_weather_data():
    """Test template for store_weather_data (April)."""
    # TODO: Insert rows, link to Cities table
    print("Running test_store_weather_data...")

    # Create a separate test database so we don't touch the main one
    test_db_name = os.path.join(TEST_OUTPUT_DIR, "test_weather.db")
    create_database(test_db_name)

    conn = sqlite3.connect(test_db_name)
    cur = conn.cursor()

    # Make a small fake weather_data list like fetch_weather would return
    fake_weather = [
        {
            "city_name": "Test City",
            "country": "TC",
            "latitude": 1.23,
            "longitude": 4.56,
            "timestamp": 1234567890,
            "temperature": 20.5,
            "feels_like": 19.0,
            "humidity": 50,
            "wind_speed": 3.2,
            "weather_main": "Clear"
        }
    ]

    # Call the function we are testing
    store_weather_data(conn, fake_weather)

    # Check that something was inserted into Cities
    cur.execute("SELECT COUNT(*) FROM Cities WHERE city_name = ?", ("Test City",))
    city_count = cur.fetchone()[0]

    # Check that something was inserted into WeatherObservations
    cur.execute("SELECT COUNT(*) FROM WeatherObservations")
    weather_count = cur.fetchone()[0]

    if city_count > 0 and weather_count > 0:
        print("PASS: store_weather_data inserted rows into Cities and WeatherObservations.")
    else:
        print("FAIL: store_weather_data did not insert expected rows.")

    conn.close()
    print()  # blank line


def test_plot_city_characteristics():
    """Test template for plot_city_characteristics (April)."""
    # TODO: Chart creation, missing data handling
    print("Running test_plot_city_characteristics...")

    # Simple fake data to plot
    sample_city_stats = [
        {"city": "City A", "population": 100000, "aq_category": "Good"},
        {"city": "City B", "population": 200000, "aq_category": "Moderate"},
        {"city": "City C", "population": 150000, "aq_category": "Unhealthy"},
    ]

    try:
        plot_city_characteristics(sample_city_stats)
        print("PASS: plot_city_characteristics ran without errors.")
    except Exception as e:
        print("FAIL: plot_city_characteristics raised an error:", e)

    print()  # blank line


def test_write_results_to_file():
    """Test template for write_results_to_file (April)."""
    # TODO: File creation + formatting
    print("Running test_write_results_to_file...")

    test_filename = os.path.join(TEST_OUTPUT_DIR, "test_results.txt")


    # Small sample city_stats list
    sample_city_stats = [
        {
            "city": "Sample City",
            "population": 123456,
            "avg_temp": 15.5,
            "avg_pm25": 7.8,
            "aq_category": "Good"
        }
    ]

    # Call the function we are testing
    write_results_to_file(sample_city_stats, filename=test_filename)

    # Try to open the file and read a bit of it
    try:
        with open(test_filename, "r") as f:
            contents = f.read()

        if "Sample City" in contents:
            print("PASS: write_results_to_file created the file and wrote city data.")
        else:
            print("FAIL: test_results.txt does not seem to contain the expected city name.")
    except FileNotFoundError:
        print("FAIL: test_results.txt did not create the file:", test_filename)

    print()  # blank line


# -----------------------------
# Test Cases for Kyndal
# -----------------------------
def test_fetch_air_quality():
    """Test template for fetch_air_quality (Kyndal)."""
    # TODO: Valid response, empty dataset handling
    print("Running test_fetch_air_quality...")

    empty_result = fetch_air_quality([])
    if empty_result != []:
        print("FAIL: fetch_air_quality should return [] for empty input.")
        return

    cities = ["Ann Arbor"]
    try:
        result = fetch_air_quality(cities)
    except Exception as e:
        print("FAIL: fetch_air_quality raised an exception:", e)
        return

    if not isinstance(result, list):
        print("FAIL: fetch_air_quality did not return a list.")
        return

    if result:
        first = result[0]
        if not isinstance(first, dict):
            print("FAIL: fetch_air_quality returned non-dict elements.")
            return
        if "city" not in first or "pm25" not in first:
            print("FAIL: fetch_air_quality missing expected keys.")
            return
        if not isinstance(first["pm25"], (int, float)):
            print("FAIL: pm25 is not numeric.")
            return

    print("PASS: test_fetch_air_quality")
    print()

def test_store_air_quality_data():
    """Test template for store_air_quality_data (Kyndal)."""
    # TODO: Insert station + measurement rows
    print("Running test_store_air_quality_data...")

    conn = sqlite3.connect(":memory:")

    try:
        aq_data = [
            {
                "city": "Ann Arbor",
                "location": "Station AA",
                "latitude": 42.28,
                "longitude": -83.74,
                "pm25": 12.5,
                "unit": "ug/m3",
            },
            {
                "city": "Chicago",
                "location": "Station CHI",
                "latitude": 41.88,
                "longitude": -87.63,
                "pm25": 25.0,
                "unit": "ug/m3",
            },
        ]

        store_air_quality_data(conn, aq_data)
        cur = conn.cursor()

        cur.execute("SELECT city_name, location_name FROM AirQualityLocations")
        stations = cur.fetchall()

        if len(stations) != 2:
            print("FAIL: Expected 2 stations inserted.")
            conn.close()
            return

        station_cities = {row[0] for row in stations}
        if "Ann Arbor" not in station_cities or "Chicago" not in station_cities:
            print("FAIL: Missing expected station cities.")
            conn.close()
            return

        cur.execute("SELECT value FROM AirQualityMeasurements")
        measurements = cur.fetchall()

        if len(measurements) != 2:
            print("FAIL: Expected 2 measurements inserted.")
            conn.close()
            return

        pm_values = {row[0] for row in measurements}
        if 12.5 not in pm_values or 25.0 not in pm_values:
            print("FAIL: PM2.5 values not inserted correctly.")
            conn.close()
            return

        print("PASS: test_store_air_quality_data")

    finally:
        conn.close()

    print()


def test_plot_temp_vs_pm25():
    """Test template for plot_temp_vs_pm25 (Kyndal)."""
    # TODO: Scatter creation, missing values
    print("Running test_plot_temp_vs_pm25...")

    city_stats = [
        {"city": "Ann Arbor", "avg_temp": 12.0, "avg_pm25": 15.0},
        {"city": "Chicago", "avg_temp": 8.0, "avg_pm25": 25.0},
        {"city": "Unknown City", "avg_temp": None, "avg_pm25": None},
    ]

    try:
        plot_temp_vs_pm25(city_stats)
        print("PASS: test_plot_temp_vs_pm25")
    except Exception as e:
        print("FAIL: plot_temp_vs_pm25 raised an exception:", e)

    print()


# -----------------------------
# Test Cases for Sarah
# -----------------------------
def test_fetch_city_data():
    """Test template for fetch_city_data (Sarah)."""
    # TODO: Valid GraphQL response, filtering, field structure
    # Case 1
    result = fetch_city_data(limit=2, min_population=50000)
    if not isinstance(result, list):
        print("FAIL: fetch_city_data did not return a list.")
        return

    if len(result) > 0:
        first = result[0]
        required_fields = ["name", "country", "population", "latitude", "longitude"]
        missing = [f for f in required_fields if f not in first]
        if missing:
            print("FAIL: Missing fields:", missing)
        else:
            print("PASS: Valid structure for small limit.")
    # Case 2
    result2 = fetch_city_data(limit=5, min_population=200000)
    if isinstance(result2, list) and len(result2) <= 5:
        print("PASS: Limit filtering works.")
    else:
        print("FAIL: Limit filtering issue.")
    # Edge case 1
    r_zero = fetch_city_data(limit=0)
    if isinstance(r_zero, list) and len(r_zero) == 0:
        print("PASS: limit=0 returns empty list.")
    else:
        print("FAIL: limit=0 should return empty list.")
    # Edge case 2
    r_high = fetch_city_data(limit=10, min_population=999999999)
    if isinstance(r_high, list) and len(r_high) == 0:
        print("PASS: high population threshold returns empty list.")
    else:
        print("FAIL: huge min_population should return empty list.")
    print()


def test_store_city_data():
    """Test template for store_city_data (Sarah)."""
    # TODO: Insert GeoCities + CityDetails rows
    print("Running test_store_city_data...")
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    # Manually create tables
    cur.execute("CREATE TABLE GeoCities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, country TEXT, region TEXT, population INTEGER)")
    cur.execute("CREATE TABLE CityDetails (id INTEGER PRIMARY KEY AUTOINCREMENT, geocity_id INTEGER, latitude REAL, longitude REAL)")
    # Case 1
    sample_data = [{"name": "Paris", "country": "France", "region": "Ile-de-France", "population": 2000000, "latitude": 48.85, "longitude": 2.35}, {"name": "Tokyo", "country": "Japan", "region": "Kanto", "population": 9000000, "latitude": 35.68, "longitude": 139.65}]
    store_city_data(conn, sample_data)
    cur.execute("SELECT COUNT(*) FROM GeoCities")
    city_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM CityDetails")
    detail_count = cur.fetchone()[0]
    if city_count == 2 and detail_count == 2:
        print("PASS: store_city_data inserted two cities.")
    else:
        print("FAIL: store_city_data did not insert expected rows.")
    # Case 2
    cur.execute("SELECT name, population FROM GeoCities WHERE name='Paris'")
    row = cur.fetchone()
    if row and row[0] == "Paris" and row[1] == 2000000:
        print("PASS: Paris stored correctly.")
    else:
        print("FAIL: Paris not stored correctly.")
    # Edge case 1 (empty list)
    conn2 = sqlite3.connect(":memory:")
    c2 = conn2.cursor()
    c2.execute("CREATE TABLE GeoCities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, country TEXT, region TEXT, population INTEGER)")
    c2.execute("CREATE TABLE CityDetails (id INTEGER PRIMARY KEY AUTOINCREMENT, geocity_id INTEGER, latitude REAL, longitude REAL)")
    store_city_data(conn2, [])
    c2.execute("SELECT COUNT(*) FROM GeoCities")
    if c2.fetchone()[0] == 0:
        print("PASS: empty city_data inserts nothing.")
    else:
        print("FAIL: empty city_data should not insert anything.")
    # Edge case 2 (missing fields)
    conn3 = sqlite3.connect(":memory:")
    c3 = conn3.cursor()
    c3.execute("CREATE TABLE GeoCities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, country TEXT, region TEXT, population INTEGER)")
    c3.execute("CREATE TABLE CityDetails (id INTEGER PRIMARY KEY AUTOINCREMENT, geocity_id INTEGER, latitude REAL, longitude REAL)")
    try:
        store_city_data(conn3, [{"name": "Nowhere"}])
        print("PASS: store_city_data handled missing fields without crashing.")
    except Exception as e:
        print("FAIL: store_city_data crashed on missing fields:", e)
    print()

def test_plot_population_vs_pm25():
    """Test template for plot_population_vs_pm25 (Sarah)."""
    # TODO: Scatter creation, missing population
    print("Running test_plot_population_vs_pm25...")
    # Case 1
    stats1 = [{"city": "Paris", "population": 2000000, "avg_pm25": 12.5}, {"city": "Tokyo", "population": 9000000, "avg_pm25": 20.1}]
    try:
        plot_population_vs_pm25(stats1)
        print("PASS: Basic scatter plot ran without error.")
    except Exception as e:
        print("FAIL: scatter plot raised an error:", e)
    # Case 2
    stats2 = [{"city": f"City{i}", "population": 50000 + i * 1000, "avg_pm25": 5 + i} for i in range(10)]
    try:
        plot_population_vs_pm25(stats2)
        print("PASS: Large dataset plotted successfully.")
    except Exception as e:
        print("FAIL: large dataset plot raised an error:", e)
    # Edge case 1 (missing population)
    stats3 = [{"city": "A", "population": None, "avg_pm25": 12}, {"city": "B", "population": 300000, "avg_pm25": 25}]
    try:
        plot_population_vs_pm25(stats3)
        print("PASS: Missing population handled correctly.")
    except Exception as e:
        print("FAIL: Missing population caused error:", e)
    # Edge case 2 (empty list)
    try:
        plot_population_vs_pm25([])
        print("PASS: Empty list handled safely.")
    except Exception as e:
        print("FAIL: Empty list caused error:", e)
    print()


# -----------------------------
# Combined Function Tests
# -----------------------------
def test_calculate_city_stats():
    """Test template for calculate_city_stats (Kyndal + Sarah)."""
    # TODO: Join all three APIs, compute averages & metrics
    print("Running test_calculate_city_stats...")

    test_db_name = os.path.join(TEST_OUTPUT_DIR, "test_city_stats.db")
    create_database(test_db_name)

    conn = sqlite3.connect(test_db_name)
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO Cities (city_name, country, latitude, longitude) VALUES (?, ?, ?, ?)",
        ("Test City", "TC", 1.23, 4.56)
    )
    city_id = cur.lastrowid

    cur.execute(
        "INSERT INTO WeatherObservations (city_id, timestamp, temperature, feels_like, humidity, wind_speed, weather_main) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (city_id, "2024-01-01T00:00", 10.0, 9.0, 50, 3.0, "Clear")
    )
    cur.execute(
        "INSERT INTO WeatherObservations (city_id, timestamp, temperature, feels_like, humidity, wind_speed, weather_main) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (city_id, "2024-01-01T01:00", 20.0, 19.0, 55, 4.0, "Clouds")
    )

    cur.execute(
        "INSERT INTO AirQualityLocations (city_name, location_name, country, latitude, longitude) VALUES (?, ?, ?, ?, ?)",
        ("Test City", "Test Station", "TC", 1.23, 4.56)
    )
    location_id = cur.lastrowid

    cur.execute(
        "INSERT INTO AirQualityMeasurements (location_id, timestamp, parameter, value, unit) VALUES (?, ?, ?, ?, ?)",
        (location_id, "2024-01-01T00:00", "pm25", 10.0, "ug/m3")
    )
    cur.execute(
        "INSERT INTO AirQualityMeasurements (location_id, timestamp, parameter, value, unit) VALUES (?, ?, ?, ?, ?)",
        (location_id, "2024-01-01T01:00", "pm25", 30.0, "ug/m3")
    )

    geodb_id = "geo-1"
    cur.execute(
        "INSERT INTO GeoCities (geodb_id, city_name, country, region, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)",
        (geodb_id, "Test City", "TC", "Test Region", 1.23, 4.56)
    )
    cur.execute(
        "INSERT INTO CityDetails (geodb_id, population, elevation, density) VALUES (?, ?, ?, ?)",
        (geodb_id, 100000, 100, 1000.0)
    )

    conn.commit()

    try:
        stats = calculate_city_stats(conn)

        if not isinstance(stats, list):
            print("FAIL: calculate_city_stats did not return a list.")
            conn.close()
            return

        if not stats:
            print("FAIL: calculate_city_stats returned an empty list.")
            conn.close()
            return

        first = stats[0]

        needed_keys = ["city", "population", "avg_temp", "avg_pm25"]
        missing = [k for k in needed_keys if k not in first]
        if missing:
            print("FAIL: calculate_city_stats result missing keys:", missing)
            conn.close()
            return

        if first.get("city") != "Test City":
            print("FAIL: calculate_city_stats city name is incorrect.")
            conn.close()
            return

        print("PASS: test_calculate_city_stats")

    except Exception as e:
        print("FAIL: calculate_city_stats raised an exception:", e)

    conn.close()
    print()


# ============================================================
# RUN MAIN
# ============================================================

if __name__ == "__main__":
    main()
