# ============================================================
# Final Project Starter File
# API Keys and Base URLs for: OpenWeatherMap, OpenAQ, GeoDB Cities
# ============================================================

# -----------------------------
# API KEYS (fill these in)
# -----------------------------
OPENWEATHER_API_KEY = "YOUR_OPENWEATHER_API_KEY_HERE"
OPENAQ_API_KEY = None   # OpenAQ does not require an API key
GEODB_API_KEY = "YOUR_GEODB_API_KEY_HERE"   # from RapidAPI

# -----------------------------
# BASE URLS
# -----------------------------
# OpenWeatherMap (Weather)
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/"

# OpenAQ (Air Quality)
OPENAQ_BASE_URL = "https://api.openaq.org/v2/"

# GeoDB Cities (City population + metadata)
GEODB_BASE_URL = "https://wft-geo-db.p.rapidapi.com/v1/geo/"

# ============================================================
# IMPORTS
# ============================================================
import requests
import sqlite3
import json

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


def fetch_city_data():
    """Fetch city metadata (population, coordinates, etc.) from GeoDB Cities."""
    # TODO: Sarah fills this in
    pass


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
    pass


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
    # TODO: Put the workflow here
    pass


if __name__ == "__main__":
    main()
