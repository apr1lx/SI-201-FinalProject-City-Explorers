import sqlite3
import matplotlib.pyplot as plt

def calculate_city_stats(conn):
    cur = conn.cursor()

    query = """
        SELECT
            c.city_name AS city,
            AVG(w.temperature) AS avg_temp,
            AVG(aqm.value) AS avg_pm25,
            cd.population AS population
        FROM Cities AS c
        JOIN WeatherObservations AS w
            ON w.city_id = c.id
        JOIN AirQualityLocations AS aql
            ON aql.city_name = c.city_name
        JOIN AirQualityMeasurements AS aqm
            ON aqm.location_id = aql.id
           AND aqm.parameter = 'pm25'
        LEFT JOIN GeoCities AS gc
            ON gc.city_name = c.city_name
        LEFT JOIN CityDetails AS cd
            ON cd.geodb_id = gc.geodb_id
        GROUP BY c.city_name
        ORDER BY c.city_name
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


def plot_temp_vs_pm25(city_stats):
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

    if not temps or not pm25_values:
        print("No data available to plot temperature vs PM2.5.")
        return

    plt.figure()
    plt.scatter(temps, pm25_values)
    for x, y, label in zip(temps, pm25_values, labels):
        plt.text(x, y, label, fontsize=8)

    plt.xlabel("Average Temperature (°C)")
    plt.ylabel("Average PM2.5")
    plt.title("Average Temperature vs Average PM2.5 by City")
    plt.tight_layout()
    plt.show()


def plot_population_vs_pm25(city_stats):
    populations = []
    pm25_values = []
    labels = []

    for city in city_stats:
        pop = city.get("population")
        pm25 = city.get("avg_pm25")
        if pop is None or pm25 is None:
            continue
        populations.append(pop)
        pm25_values.append(pm25)
        labels.append(city.get("city"))

    if not populations or not pm25_values:
        print("Not enough city data to plot Population vs PM2.5.")
        return

    plt.figure()
    plt.scatter(populations, pm25_values)
    for x, y, label in zip(populations, pm25_values, labels):
        plt.text(x, y, label, fontsize=7)

    plt.xlabel("Population")
    plt.ylabel("Average PM2.5")
    plt.title("City Population vs PM2.5 Levels")
    plt.tight_layout()
    plt.show()


def plot_city_characteristics(city_stats):
    city_labels = []
    populations = []

    for city in city_stats:
        city_name = city.get("city")
        population = city.get("population")
        aq_category = city.get("aq_category")

        if population is None:
            continue

        label = f"{city_name} ({aq_category})" if aq_category else city_name
        city_labels.append(label)
        populations.append(population)

    if not populations:
        print("No valid population data to plot.")
        return

    plt.figure()
    x_positions = range(len(city_labels))
    plt.bar(x_positions, populations)
    plt.xticks(x_positions, city_labels, rotation=45, ha="right")
    plt.xlabel("Cities (with AQ Category)")
    plt.ylabel("Population")
    plt.title("City Populations with Air Quality Categories")
    plt.tight_layout()
    plt.show()


def write_results_to_file(city_stats, filename="results.txt"):
    """Write final calculated statistics to a text file."""
    try:
        with open(filename, "w") as f:
            f.write("City Statistics Results\n")
            f.write("-" * 40 + "\n")

            for city in city_stats:
                name = city.get("city")
                population = city.get("population")
                avg_temp = city.get("avg_temp")
                avg_pm25 = city.get("avg_pm25")
                aq_category = city.get("aq_category")

                # Make population human-friendly
                if population is None:
                    population_str = "Unknown (no population data)"
                else:
                    # format like 1,234,567
                    population_str = f"{population:,}"

                f.write(f"City: {name}\n")
                f.write(f"Population: {population_str}\n")
                f.write(f"Average Temperature: {avg_temp}\n")
                f.write(f"Average PM2.5: {avg_pm25}\n")
                f.write(f"Air Quality Category: {aq_category}\n")
                f.write("-" * 40 + "\n")

            print(f"Results successfully written to {filename}")
    except Exception as e:
        print(f"Error writing results to file: {e}")