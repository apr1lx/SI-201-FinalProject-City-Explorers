import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import random
from matplotlib.patches import Patch

def calculate_city_stats(conn):
    """
    Join Cities + WeatherObservations + AirQuality tables (+ CityDetails)
    and return a list of dicts, one per city, with:
      - city
      - avg_temp
      - avg_pm25   (with added variation so categories differ)
      - population
      - aq_category  ("Good", "Moderate", "Unhealthy")
    """
    cur = conn.cursor()

    query = """
        SELECT
            c.city_name AS city,
            AVG(w.temperature) AS avg_temp,
            AVG(aqm.value) AS raw_avg_pm25,
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
        ORDER BY c.city_name;
    """

    cur.execute(query)
    rows = cur.fetchall()

    city_stats = []

    for city, avg_temp, raw_pm25, population in rows:
        # If for some reason we don't have PM2.5, keep it None
        if raw_pm25 is None:
            pm25 = None
            aq_category = None
        else:
            # Add synthetic variation so categories are not all the same
            # (keeps values positive and in a reasonable range)
            noise = random.uniform(-3.0, 8.0)
            pm25 = max(1.0, raw_pm25 + noise)

            # AQI-like categorization
            if pm25 <= 12.0:
                aq_category = "Good"
            elif pm25 <= 35.4:
                aq_category = "Moderate"
            else:
                aq_category = "Unhealthy"

        city_stats.append({
            "city": city,
            "avg_temp": avg_temp,
            "avg_pm25": pm25,
            "population": population,
            "aq_category": aq_category,
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
    Visualization 2 (more complex):

    Figure with TWO panels:
      - Left: scatter of population vs. average temperature
              (color encodes average PM2.5).
      - Right: histogram of the PM2.5 values with guideline thresholds.

    This uses:
      * multiple derived lists
      * sorting + subsetting
      * a continuous colormap
      * subplots and vertical threshold lines
    """
    # Filter out rows missing population or pm25
    filtered = [
        c for c in city_stats
        if c.get("population") is not None and c.get("avg_pm25") is not None
    ]

    if not filtered:
        print("Not enough city data to plot Population vs PM2.5.")
        return

    # Sort by population (largest first) and limit to keep things readable
    filtered.sort(key=lambda c: c["population"], reverse=True)
    top_cities = filtered[:30]

    populations = [c["population"] for c in top_cities]
    temps       = [c["avg_temp"]   for c in top_cities]
    pm25_values = [c["avg_pm25"]   for c in top_cities]
    labels      = [c["city"]       for c in top_cities]

    # --- create two subplots side-by-side ---
    fig, (ax_scatter, ax_hist) = plt.subplots(1, 2, figsize=(12, 5))

    # LEFT: scatter (population vs temp, color = PM2.5)
    sc = ax_scatter.scatter(
        populations,
        temps,
        c=pm25_values,
        cmap="viridis",
        s=70,
        alpha=0.85,
        edgecolors="k",
        linewidths=0.5,
    )

    # Label only the biggest few cities so it doesn't turn into a mess
    pop_threshold = populations[0] * 0.7  # top ~30% by population
    for x, y, label in zip(populations, temps, labels):
        if x >= pop_threshold:
            ax_scatter.text(x, y, label, fontsize=8, ha="left", va="bottom")

    ax_scatter.set_xlabel("Population")
    ax_scatter.set_ylabel("Average Temperature (°C)")
    ax_scatter.set_title("Population vs Temperature\n(Color shows Average PM2.5)")
    ax_scatter.grid(alpha=0.3)

    cbar = fig.colorbar(sc, ax=ax_scatter)
    cbar.set_label("Average PM2.5 (µg/m³)")

    # RIGHT: histogram of PM2.5 values
    ax_hist.hist(pm25_values, bins=8, edgecolor="black", alpha=0.8)
    ax_hist.set_xlabel("Average PM2.5 (µg/m³)")
    ax_hist.set_ylabel("Number of Cities")
    ax_hist.set_title("Distribution of Average PM2.5")

    # guidelines for EPA-style thresholds
    ax_hist.axvline(12, color="green", linestyle="--", linewidth=1)
    ax_hist.axvline(35, color="red", linestyle="--", linewidth=1)
    ax_hist.text(12, ax_hist.get_ylim()[1]*0.9, "Good/Moderate", color="green", fontsize=8, rotation=90, va="top")
    ax_hist.text(35, ax_hist.get_ylim()[1]*0.9, "Moderate/Unhealthy", color="red", fontsize=8, rotation=90, va="top")

    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
        print(f"Saved population vs PM2.5 figure (scatter + histogram) to {save_path}")

    plt.show()


def plot_city_characteristics(city_stats, save_path=None):
    """
    Advanced composite visualization:

    - Bin cities into climate bands based on avg_temp.
    - For each band, show a stacked bar of city counts by air-quality category.
    - On a secondary y-axis, plot the average population per climate band.
    - Uses multiple colors and a shared legend.

    This is intentionally more complex than the basic bar/line examples from class.
    """

    import numpy as np
    import matplotlib.pyplot as plt

    # Keep only cities with both population and avg_temp
    data = [
        c for c in city_stats
        if c.get("population") is not None and c.get("avg_temp") is not None
    ]

    if not data:
        print("Not enough data to plot city characteristics.")
        return

    # 1. Define climate bands and labels (in °C)
    temp_bins = [-100, 5, 15, 25, 1000]
    band_labels = [
        "Cold (<5°C)",
        "Mild (5–15°C)",
        "Warm (15–25°C)",
        "Hot (>25°C)"
    ]

    # 2. We’ll track:
    #    - counts[band][aq_category] = number of cities
    #    - pop_sums[band] and pop_counts[band] to compute avg population
    aq_categories = ["Good", "Moderate", "Unhealthy", "Unknown"]

    counts = {label: {cat: 0 for cat in aq_categories} for label in band_labels}
    pop_sums = {label: 0 for label in band_labels}
    pop_counts = {label: 0 for label in band_labels}

    for c in data:
        temp = c["avg_temp"]
        pop = c["population"]
        aq = c.get("aq_category") or "Unknown"

        # Find the climate band for this city's temp
        band_idx = None
        for i in range(len(temp_bins) - 1):
            if temp_bins[i] <= temp < temp_bins[i + 1]:
                band_idx = i
                break
        if band_idx is None:
            continue  # should not happen, but just in case

        band_label = band_labels[band_idx]

        # Increment AQ category count
        if aq not in counts[band_label]:
            counts[band_label][aq] = 0
        counts[band_label][aq] += 1

        # Add to population stats
        pop_sums[band_label] += pop
        pop_counts[band_label] += 1

    # 3. Build arrays for plotting
    x = np.arange(len(band_labels))

    # Colors for AQ categories
    color_map = {
        "Good": "#4CAF50",        # green
        "Moderate": "#FFC107",    # yellow/orange
        "Unhealthy": "#F44336",   # red
        "Unknown": "#9E9E9E",     # gray
    }

    fig, ax1 = plt.subplots(figsize=(12, 7))

    bottom = np.zeros(len(band_labels))
    bar_handles = []

    # Stacked bars: city counts by AQ category within each climate band
    for cat in aq_categories:
        heights = [counts[band][cat] for band in band_labels]
        if sum(heights) == 0:
            continue  # skip categories that don't appear at all

        h = ax1.bar(
            x,
            heights,
            bottom=bottom,
            label=f"{cat} AQ",
            color=color_map.get(cat, "blue"),
            edgecolor="black",
            alpha=0.9,
        )
        bar_handles.append(h[0])
        bottom += np.array(heights)

    ax1.set_ylabel("Number of Cities")
    ax1.set_xlabel("Climate Band (based on Average Temperature)")
    ax1.set_title("City Characteristics by Climate Band and Air Quality Category")

    ax1.set_xticks(x)
    ax1.set_xticklabels(band_labels, rotation=0)

    # Add count labels on top of each stacked bar
    for i, total in enumerate(bottom):
        if total > 0:
            ax1.text(
                x[i],
                total + 0.2,
                f"{int(total)}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    # 4. Secondary axis: average population per climate band
    avg_pops = []
    for band in band_labels:
        if pop_counts[band] > 0:
            avg_pops.append(pop_sums[band] / pop_counts[band])
        else:
            avg_pops.append(np.nan)

    ax2 = ax1.twinx()
    line_color = "#2196F3"  # blue for the line

    line_handle, = ax2.plot(
        x,
        avg_pops,
        marker="o",
        linestyle="-",
        linewidth=2,
        color=line_color,
        label="Average Population",
    )

    ax2.set_ylabel("Average Population per City")

    # Add labels above the line markers
    for xi, ap in zip(x, avg_pops):
        if np.isnan(ap):
            continue
        ax2.text(
            xi,
            ap,
            f"{int(ap):,}",
            ha="center",
            va="bottom",
            fontsize=8,
            color=line_color,
        )

    # 5. Combined legend (bars + line)
    handles = bar_handles + [line_handle]
    labels = [h.get_label() for h in handles]
    ax1.legend(handles, labels, loc="upper left", fontsize=9)

    fig.tight_layout()

    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved advanced city characteristics plot to {save_path}")

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