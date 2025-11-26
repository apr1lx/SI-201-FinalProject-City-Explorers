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
    pass


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
    pass


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
    pass


# ============================================================
# VISUALIZATIONS
# ============================================================

def plot_temp_vs_pm25(city_stats):
    """Scatter plot of avg temperature vs avg PM2.5."""
    # TODO: Kyndal fills this in
    pass


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
