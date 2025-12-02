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
            "city": city_name,
            "country": country,
            "latitude": latitude,
            "longitude": longitude,
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
    cur = conn.cursor()

    for item in weather_data:
        city = item.get("city")
        country = item.get("country")
        latitude = item.get("latitude")
        longitude = item.get("longitude")

        cur.execute("""INSERT OR IGNORE INTO Cities (name, country, latitude, longitude) VALUES (?, ?, ?, ?)""",
                    (city, country, latitude, longitude))
        
        cur.execute("""SELECT id FROM Cities WHERE name = ? AND country = ?""", (city, country))
        row = cur.fetchone()

        if row is None:
            print(f"City {city} not found in Cities table.")
            continue

        city_id = row[0]

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
    cur = conn.cursor()
    # Create the GeoCities table to store the city info
    cur.execute("""CREATE TABLE IF NOT EXISTS GeoCities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, country TEXT, region TEXT, population INTEGER)""")
    # Create the CityDetails table to store extra details in GeoCities
    cur.execut("""CREATE TABLE IF NOT EXISTS CityDetails (id INTEGER PRIMARY KEY AUTOINCREMENT, geocity_id INTEGER, latitude REAL, longitude REAL, FOREIGN KEY (geocity_id) REFERENCES GeoCities(id))""")
    # Loop through each city dictionary returned from fetch_city_data
    for city in city_data:
        city_name = city.get("name")
        country = city.get("country")
        region = city.get("region")
        population = city.get("population")
        latitude = city.get("latitude")
        longitude = city.get("longitude")
        # Insert row into GeoCities
        cur.execute("""INSERT OR IGNORE INTO GeoCities (name, country, region, population) VALUES (?, ?, ?, ?)""", (city_name, country, region, population))
        # ID of city in GeoCities
        cur.execute("""SELECT id FROM GeoCities WHERE name = ? AND country = ?""", (city_name, country))
        row = cur.fetchone()
        # If can't find row then skip city
        if row is None:
            continue
        geocity_id = row[0]
        # Insert row into CityDetails
        cur.execute("""INSERT INTO CityDetails (geocity_id, latitude, longitude) VALUES (?, ?, ?)""", (geocity_id, latitude, longitude))
        # Save all inserts to database file
        conn.commit()

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
    # Make empty lists to store the values to plot
    populations = []
    pm25_values = []
    labels = []
    # Go through each city's stats from the database
    for city in city_stats:
        pop = city.get("population")
        pm25 = city.get("avg_pm25")
        # Skip cities that are missing population or PM2.5
        if pop is None or pm25 is None:
            continue
        populations.append(pop)
        pm25_values.append(pm25)
        labels.append(city.get("city"))
    # If nothing valid to plot print a message
    if not populations or not pm25_values:
        print("Not enough city data to plot Population vs PM2.5.")
        return
    # Make the scatter plot
    plt.figure()
    plt.scatter(populations, pm25_values)
    # Add labels nex to each dot for each city
    for x, y, label in zip(populations, pm25_values, labels):
        plt.text(x, y, label, fontsize=7)
    # Label the axes and title
    plt.xlabel("Population")
    plt.ylabel("Average PM2.5")
    plt.title("City Population vs PM2.5 Levels")
    # Adjust layout so everything fits
    plt.tight_layout()
    plt.show()


def plot_city_characteristics(city_stats):
    """Bar chart of city characteristics (e.g., population or elevation) with air-quality categories."""
    # TODO: April fills this in
    city_labels = []
    populations = []

    for city in city_stats:
        city_name = city.get("city")
        population = city.get("population")    
        aq_category = city.get("aq_category")

        if population is None:
            continue 

        if aq_category is not None:
            label = f"{city_name} ({aq_category})"
        else:
            label = city_name

        city_labels.append(label)
        populations.append(population)

    if not populations:
        print("No valid population data to plot.")
        return
    
    plt.figure()
    x_positions = range(len(city_labels))
    plt.bar(x_positions, populations)
    plt.xticks(x_positions, city_labels, rotation=45, ha='right')
    plt.xlabel("Cities (with AQ Category)")
    plt.ylabel("Population")
    plt.title("City Populations with Air Quality Categories")
    plt.tight_layout()
    plt.show()




# ============================================================
# WRITE RESULTS TO FILE
# ============================================================

def write_results_to_file(city_stats, filename="results.txt"):
    """Write final calculated statistics to a text file."""
    # TODO: April fills this in
    try:
        with open(filename, "w") as f:
            f.write(f"City Statistics Results\n")
            f.write(f"{'-'*40}\n")

            for city in city_stats:
                name = city.get("city")
                population = city.get("population")
                avg_temp = city.get("avg_temperature")
                avg_pm25 = city.get("avg_pm25")
                aq_category = city.get("aq_category")

                f.write(f"City: {name}\n")
                f.write(f"Population: {population}\n")
                f.write(f"Average Temperature: {avg_temp}\n")
                f.write(f"Average PM2.5: {avg_pm25}\n")
                f.write(f"Air Quality Category: {aq_category}\n")
                f.write(f"{'-'*40}\n")

            print(f"Results successfully written to {filename}")
    except Exception as e:
        print(f"Error writing results to file: {e}")


# ============================================================
# MAIN FUNCTION
# ============================================================

def main():
    """Run all steps of the project in order."""
    create_database("final_project.db")

    # TODO: Put the workflow here
    # TEMP: run April's tests
    test_fetch_weather()
    test_store_weather_data()
    test_plot_city_characteristics()
    test_write_results_to_file()



# ============================================================
# TEST CASE TEMPLATES (do NOT fill in yet)
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
    test_db_name = "test_weather.db"
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

    test_filename = "test_results.txt"

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

    test_db_name = "test_city_stats.db"
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
