# ============================================================
# create_database.py
# Creates all SQLite tables for the Final Project
# Weather (OpenWeatherMap) + Air Quality (OpenAQ) + City Info (GeoDB)
# ============================================================

import sqlite3

def create_database(db_name="final_project.db"):
    """
    Creates a SQLite database with all required tables.
    If the database already exists, this ensures tables exist
    without overwriting data.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # ------------------------------------------
    # TABLE 1: Cities (core city info)
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            UNIQUE (city_name, country)
        );
    """)

    # ------------------------------------------
    # TABLE 2: WeatherObservations (OpenWeatherMap)
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherObservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            timestamp TEXT,
            temperature REAL,
            feels_like REAL,
            humidity REAL,
            wind_speed REAL,
            weather_main TEXT,
            FOREIGN KEY (city_id) REFERENCES Cities(id)
        );
    """)

    # ------------------------------------------
    # NEW TABLE: Pollutants (to de-duplicate parameter + unit)
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Pollutants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parameter TEXT UNIQUE,  -- e.g. "pm25"
            unit TEXT               -- e.g. "µg/m³"
        );
    """)

    # ------------------------------------------
    # TABLE 3: AirQualityLocations (station metadata)
    # now references Cities by city_id instead of duplicating city_name
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityLocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            location_name TEXT,
            latitude REAL,
            longitude REAL,
            FOREIGN KEY (city_id) REFERENCES Cities(id)
        );
    """)

    # ------------------------------------------
    # TABLE 4: AirQualityMeasurements (OpenAQ)
    # now uses pollutant_id instead of repeating parameter + unit
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS AirQualityMeasurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER,
            pollutant_id INTEGER,
            timestamp TEXT,
            value REAL,
            FOREIGN KEY (location_id) REFERENCES AirQualityLocations(id),
            FOREIGN KEY (pollutant_id) REFERENCES Pollutants(id)
        );
    """)

    # ------------------------------------------
    # TABLE 5: GeoCities (GeoDB Cities metadata)
    # (kept similar to your original design)
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS GeoCities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            geodb_id TEXT,
            city_name TEXT,
            country TEXT,
            region TEXT,
            latitude REAL,
            longitude REAL
        );
    """)

    # ------------------------------------------
    # TABLE 6: CityDetails (population, elevation, extras)
    # ------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS CityDetails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            geodb_id TEXT,
            population INTEGER,
            elevation INTEGER,
            density REAL,
            FOREIGN KEY (geodb_id) REFERENCES GeoCities(geodb_id)
        );
    """)

    conn.commit()
    conn.close()
    print("Database created successfully!")

# ---------------------------------------------------------
# Run file directly to create the DB
# ---------------------------------------------------------
if __name__ == "__main__":
    create_database()
