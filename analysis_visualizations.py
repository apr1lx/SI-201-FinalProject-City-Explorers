import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import random
from matplotlib.patches import Patch

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


def plot_temp_vs_pm25(city_stats, save_path=None):
    """
    Scatter plot of avg temperature vs avg PM2.5.

    • Points are color–coded by air-quality category
    • A small vertical jitter is added so overlapping points are visible
    • A simple trend line is drawn using numpy.polyfit
    """
    temps = []
    pm25_raw = []
    labels = []
    colors = []

    # map AQ category -> color
    color_map = {
        "Good": "green",
        "Moderate": "orange",
        "Unhealthy": "red",
        None: "gray"
    }

    for city_info in city_stats:
        avg_temp = city_info.get("avg_temp")
        avg_pm25 = city_info.get("avg_pm25")
        aq_cat = city_info.get("aq_category")

        if avg_temp is None or avg_pm25 is None:
            continue

        temps.append(avg_temp)
        pm25_raw.append(avg_pm25)
        labels.append(city_info.get("city"))
        colors.append(color_map.get(aq_cat, "gray"))

    if not temps:
        print("No data available to plot temperature vs PM2.5.")
        return

    # --- jitter the PM2.5 values only for plotting (data stays unchanged)
    rng = random.Random(0)  # deterministic jitter
    pm25_jittered = [p + rng.uniform(-0.15, 0.15) for p in pm25_raw]

    plt.figure(figsize=(8, 6))
    plt.scatter(temps, pm25_jittered, c=colors, alpha=0.8, edgecolor="k")

    # label each point
    for x, y, label in zip(temps, pm25_jittered, labels):
        plt.text(x, y, label, fontsize=7, ha="center", va="bottom")

    # --- add a simple trend line (least-squares fit)
    if len(temps) >= 2:
        coeffs = np.polyfit(temps, pm25_raw, 1)
        m, b = coeffs
        xs = np.linspace(min(temps), max(temps), 100)
        ys = m * xs + b
        plt.plot(xs, ys, linestyle="--", color="black", label="Trend line")

    plt.xlabel("Average Temperature (°C)")
    plt.ylabel("Average PM2.5 (µg/m³)")
    plt.title("Average Temperature vs. Average PM2.5 by City")
    plt.grid(True, linestyle=":", alpha=0.5)
    if len(temps) >= 2:
        plt.legend()

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved temp vs PM2.5 plot to {save_path}")

    plt.show()


def plot_population_vs_pm25(city_stats, save_path=None):
    """
    Scatter plot:
      x-axis  = city population
      y-axis  = average temperature
      color   = average PM2.5 (so PM2.5 still appears, but as color instead of a flat line)
    This is more complex than a basic scatter because we:
      - derive multiple lists from city_stats
      - sort and subset the data
      - use a continuous colormap for PM2.5
    """
    # Filter out rows missing population or pm25
    filtered = [
        c for c in city_stats
        if c.get("population") is not None and c.get("avg_pm25") is not None
    ]

    if not filtered:
        print("Not enough city data to plot Population vs PM2.5.")
        return

    # Sort by population (largest first) and limit to keep the plot readable
    filtered.sort(key=lambda c: c["population"], reverse=True)
    top_cities = filtered[:25]

    populations = [c["population"] for c in top_cities]
    temps       = [c["avg_temp"]   for c in top_cities]
    pm25_values = [c["avg_pm25"]   for c in top_cities]
    labels      = [c["city"]       for c in top_cities]

    plt.figure()
    scatter = plt.scatter(
        populations,
        temps,
        c=pm25_values,      # color encodes PM2.5
        cmap="viridis",
        s=70,
        alpha=0.8,
        edgecolors="k",
        linewidths=0.5,
    )

    # Label only the largest few so the plot isn’t a total mess
    for x, y, label in zip(populations, temps, labels):
        if x >= populations[0] * 0.7:  # top ~30% by population
            plt.text(x, y, label, fontsize=8, ha="left", va="bottom")

    cbar = plt.colorbar(scatter)
    cbar.set_label("Average PM2.5 (µg/m³)")

    plt.xlabel("Population")
    plt.ylabel("Average Temperature (°C)")
    plt.title("City Population vs Temperature\n(Color shows Average PM2.5)")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved population vs PM2.5 plot to {save_path}")

    plt.show()


def plot_city_characteristics(city_stats, save_path=None):
    """
    Horizontal bar chart of top 15 most populous cities.
    - Bar length = population
    - Bar color  = air-quality category (Good / Moderate / Unhealthy / Unknown)
    - Legend explains the colors
    """
    # Keep only rows with population
    with_pop = [c for c in city_stats if c.get("population") is not None]
    if not with_pop:
        print("No valid population data to plot.")
        return

    # Sort by population (largest first) and limit for readability
    with_pop.sort(key=lambda c: c["population"], reverse=True)
    top_cities = with_pop[:15]

    # Map AQ category to colors
    color_map = {
        "Good": "tab:green",
        "Moderate": "tab:orange",
        "Unhealthy": "tab:red",
        None: "tab:gray",
    }

    labels = []
    populations = []
    colors = []

    for c in top_cities:
        city_name = c.get("city")
        population = c.get("population")
        aq_category = c.get("aq_category")

        if aq_category:
            label = f"{city_name} ({aq_category})"
        else:
            label = f"{city_name} (Unknown AQ)"

        labels.append(label)
        populations.append(population)
        colors.append(color_map.get(aq_category, "tab:gray"))

    y_positions = range(len(labels))

    plt.figure(figsize=(10, 6))
    plt.barh(y_positions, populations, color=colors)
    plt.yticks(y_positions, labels)
    plt.xlabel("Population")
    plt.title("Top 15 Most Populous Cities\nColored by Air Quality Category")
    plt.gca().invert_yaxis()  # largest at top
    plt.tight_layout()

    # Build legend from color_map
    legend_patches = [
        Patch(color="tab:green",  label="Good AQ"),
        Patch(color="tab:orange", label="Moderate AQ"),
        Patch(color="tab:red",    label="Unhealthy AQ"),
        Patch(color="tab:gray",   label="Unknown AQ"),
    ]
    plt.legend(handles=legend_patches, loc="lower right")

    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved city characteristics plot to {save_path}")

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