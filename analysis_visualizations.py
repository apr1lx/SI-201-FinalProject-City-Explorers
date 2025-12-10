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
      - avg_pm25
      - population
      - aq_category ("Good", "Moderate", "Unhealthy")
    """
    cur = conn.cursor()

    query = """
        SELECT
            c.id                AS city_id,
            c.city_name         AS city,
            AVG(w.temperature)  AS avg_temp,
            AVG(aqm.value)      AS raw_avg_pm25,
            cd.population       AS population
        FROM Cities AS c
        JOIN WeatherObservations AS w
            ON w.city_id = c.id
        JOIN AirQualityLocations AS aql
            ON aql.city_id = c.id
        JOIN AirQualityMeasurements AS aqm
            ON aqm.location_id = aql.id
           AND aqm.parameter = 'pm25'
        LEFT JOIN GeoCities AS gc
            ON gc.city_name = c.city_name
        LEFT JOIN CityDetails AS cd
            ON cd.geodb_id = gc.geodb_id
        GROUP BY c.id, c.city_name
        ORDER BY c.city_name;
    """

    cur.execute(query)
    rows = cur.fetchall()

    city_stats = []

    for city_id, city, avg_temp, raw_pm25, population in rows:
        if raw_pm25 is None:
            pm25 = None
            aq_category = None
        else:
            # use REAL pm25; no extra synthetic noise now
            pm25 = raw_pm25

            if pm25 <= 12.0:
                aq_category = "Good"
            elif pm25 <= 35.4:
                aq_category = "Moderate"
            else:
                aq_category = "Unhealthy"

        city_stats.append({
            "city_id": city_id,
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

        if avg_temp is None or avg_pm25 is None or avg_pm25 <= 0:
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

    plt.figure(figsize=(11, 7))
    plt.scatter(temps, pm25_jittered, c=colors, alpha=0.65, edgecolor="k", s=70)

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

    This version:
      * clips extreme PM2.5 values so the colormap has more variety
      * uses a vibrant colormap
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
    pm25_values = np.array([c["avg_pm25"] for c in top_cities], dtype=float)
    labels      = [c["city"]       for c in top_cities]

    # --- CLIP PM2.5 for coloring & histogram to avoid extreme outliers ---
    # You can tweak 0 and 80 if your range is very different.
    pm25_clipped = np.clip(pm25_values, 0, 80)

    # --- create two subplots side-by-side ---
    fig, (ax_scatter, ax_hist) = plt.subplots(1, 2, figsize=(12, 5))

    # LEFT: scatter (population vs temp, color = clipped PM2.5)
    sc = ax_scatter.scatter(
        populations,
        temps,
        c=pm25_clipped,
        cmap="plasma",      # <--- more vibrant than viridis; change if you like
        s=70,
        alpha=0.85,
        edgecolors="k",
        linewidths=0.5,
        vmin=0,
        vmax=80,           # matches the clipping range
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

    # RIGHT: histogram of *clipped* PM2.5 values
    ax_hist.hist(pm25_clipped, bins=8, edgecolor="black", alpha=0.8)
    ax_hist.set_xlabel("Average PM2.5 (µg/m³)")
    ax_hist.set_ylabel("Number of Cities")
    ax_hist.set_title("Distribution of Average PM2.5")

    # guidelines for EPA-style thresholds
    ax_hist.axvline(12, color="green", linestyle="--", linewidth=1)
    ax_hist.axvline(35, color="red", linestyle="--", linewidth=1)
    ax_hist.text(
        12,
        ax_hist.get_ylim()[1]*0.9,
        "Good/Moderate",
        color="green",
        fontsize=8,
        rotation=90,
        va="top"
    )
    ax_hist.text(
        35,
        ax_hist.get_ylim()[1]*0.9,
        "Moderate/Unhealthy",
        color="red",
        fontsize=8,
        rotation=90,
        va="top"
    )

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

##New visualizations
def plot_pm25_ranked_by_city(city_stats, save_path=None):
    """
    Final Visualization: Ranked Average PM2.5 by City

    - Horizontal bar chart
    - Colored by AQ category
    - Background AQ threshold bands
    - Vertical threshold lines
    - Value labels on bars
    """

    # Filter valid PM2.5 values
    filtered = [c for c in city_stats if c.get("avg_pm25") is not None]
    if not filtered:
        print("Not enough data to plot PM2.5.")
        return

    # Sort highest → lowest
    filtered.sort(key=lambda c: c["avg_pm25"], reverse=True)
    top_cities = filtered[:30]

    cities = [c["city"] for c in top_cities]
    pm25_values = [c["avg_pm25"] for c in top_cities]
    aq_categories = [c.get("aq_category") for c in top_cities]

    # AQ category colors (vivid)
    color_map = {
        "Good": "#2ECC71",       # green
        "Moderate": "#F1C40F",   # yellow
        "Unhealthy": "#E74C3C",  # red
        "Unknown": "#95A5A6",
        None: "#95A5A6",
    }
    bar_colors = [color_map.get(cat, "#95A5A6") for cat in aq_categories]

    # Plot
    fig, ax = plt.subplots(figsize=(12, max(6, 0.35 * len(cities))))

    y_pos = np.arange(len(cities))
    ax.barh(
        y_pos,
        pm25_values,
        color=bar_colors,
        edgecolor="black",
        linewidth=1,
        alpha=0.95,
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(cities)
    ax.invert_yaxis()

    # X-axis limits
    x_max = max(pm25_values) * 1.15
    x_max = max(x_max, 40)
    ax.set_xlim(0, x_max)

    # Background AQ bands
    ax.axvspan(0, 12, color="#2ECC71", alpha=0.12)      # Good
    ax.axvspan(12, 35, color="#F1C40F", alpha=0.12)     # Moderate
    ax.axvspan(35, x_max, color="#E74C3C", alpha=0.10)  # Unhealthy

    # Threshold lines
    ax.axvline(12, color="#27AE60", linestyle="--", linewidth=2)
    ax.axvline(35, color="#C0392B", linestyle="--", linewidth=2)

    # Label PM2.5 values on bars
    for y, val in zip(y_pos, pm25_values):
        ax.text(
            val + 0.4,
            y,
            f"{val:.1f}",
            va="center",
            fontsize=10,
            fontweight="bold",
        )

    # Labels and title
    ax.set_xlabel("Average PM2.5 (µg/m³)")
    ax.set_title("Ranked Average PM2.5 by City", fontsize=14, fontweight="bold")

    ax.grid(axis="x", linestyle=":", alpha=0.3)

    # Legend
    legend_patches = [
        Patch(facecolor=color_map["Good"], edgecolor="black", label="Good"),
        Patch(facecolor=color_map["Moderate"], edgecolor="black", label="Moderate"),
        Patch(facecolor=color_map["Unhealthy"], edgecolor="black", label="Unhealthy"),
        Patch(facecolor=color_map["Unknown"], edgecolor="black", label="Unknown / Missing"),
    ]
    ax.legend(handles=legend_patches, loc="lower right", fontsize=10)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved ranked PM2.5 visualization to {save_path}")

    plt.show()
    
def plot_aq_category_overview(city_stats, save_path=None):
    """
    Advanced Visualization: Air-Quality Category Overview (2×2 grid)

    Subplots:
      (1,1) Bar: number of cities in each AQ category.
      (1,2) Bar: average PM2.5 per category with std-dev error bars.
      (2,1) Boxplots: average temperature by category.
      (2,2) Boxplots: population by category (log scale if data exists).

    Uses multiple derived lists, error bars, and boxplots to match the
    complexity of the other visualizations.
    """
    # Base categories in a fixed order
    all_cats = ["Good", "Moderate", "Unhealthy", "Unknown"]

    # Containers
    counts = {cat: 0 for cat in all_cats}
    temps_by_cat = {cat: [] for cat in all_cats}
    pm25_by_cat = {cat: [] for cat in all_cats}
    pops_by_cat  = {cat: [] for cat in all_cats}

    for c in city_stats:
        aq = c.get("aq_category")
        temp = c.get("avg_temp")
        pm25 = c.get("avg_pm25")
        pop = c.get("population")

        # Normalize AQ category
        if aq not in ["Good", "Moderate", "Unhealthy"]:
            aq = "Unknown"

        counts[aq] += 1

        if temp is not None:
            temps_by_cat[aq].append(temp)
        if pm25 is not None:
            pm25_by_cat[aq].append(pm25)
        if pop is not None:
            pops_by_cat[aq].append(pop)

    # Only keep categories that actually appear at all
    used_cats = [cat for cat in all_cats if counts[cat] > 0]
    if not used_cats:
        print("Not enough data to plot AQ category overview.")
        return

    # Build aligned lists
    cat_indices = np.arange(len(used_cats))
    count_vals = [counts[cat] for cat in used_cats]

    # Color map
    color_map = {
        "Good": "#4CAF50",
        "Moderate": "#FFC107",
        "Unhealthy": "#F44336",
        "Unknown": "#9E9E9E",
    }
    bar_colors = [color_map.get(cat, "#9E9E9E") for cat in used_cats]

    # Compute PM2.5 means + std dev where possible
    pm_means = []
    pm_stds = []
    for cat in used_cats:
        vals = pm25_by_cat[cat]
        if len(vals) == 0:
            pm_means.append(np.nan)
            pm_stds.append(0.0)
        else:
            pm_means.append(float(np.mean(vals)))
            pm_stds.append(float(np.std(vals)))

    # Prepare temperature data for boxplots
    temp_data = [temps_by_cat[cat] for cat in used_cats]

    # Prepare population data for boxplots (may be empty)
    pop_data = [pops_by_cat[cat] for cat in used_cats]
    has_any_pop = any(len(v) > 0 for v in pop_data)

    # --- Create 2×2 grid of subplots ---
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    ax_counts = axes[0, 0]
    ax_pm = axes[0, 1]
    ax_temp = axes[1, 0]
    ax_pop = axes[1, 1]

    # (1,1) Counts bar chart
    bars = ax_counts.bar(
        cat_indices, count_vals, color=bar_colors, edgecolor="black", alpha=0.9
    )
    ax_counts.set_xticks(cat_indices)
    ax_counts.set_xticklabels(used_cats)
    ax_counts.set_ylabel("Number of Cities")
    ax_counts.set_title("Cities per Air Quality Category")
    ax_counts.grid(axis="y", linestyle=":", alpha=0.4)

    # Label counts above bars
    for x, cnt in zip(cat_indices, count_vals):
        ax_counts.text(
            x,
            cnt + 0.1,
            str(cnt),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # (1,2) Average PM2.5 per category with error bars
    # Only plot categories that have at least one PM value
    valid_pm_indices = [i for i, cat in enumerate(used_cats) if len(pm25_by_cat[cat]) > 0]

    if valid_pm_indices:
        x_pm = [cat_indices[i] for i in valid_pm_indices]
        pm_means_plot = [pm_means[i] for i in valid_pm_indices]
        pm_stds_plot = [pm_stds[i] for i in valid_pm_indices]
        pm_colors = [bar_colors[i] for i in valid_pm_indices]

        ax_pm.bar(
            x_pm,
            pm_means_plot,
            yerr=pm_stds_plot,
            capsize=5,
            color=pm_colors,
            edgecolor="black",
            alpha=0.9,
        )

        ax_pm.set_xticks(cat_indices)
        ax_pm.set_xticklabels(used_cats)
        ax_pm.set_ylabel("Average PM2.5 (µg/m³)")
        ax_pm.set_title("Average PM2.5 by Air Quality Category")
        ax_pm.grid(axis="y", linestyle=":", alpha=0.4)

        # Threshold lines for context
        ax_pm.axhline(12, color="green", linestyle="--", linewidth=1)
        ax_pm.axhline(35, color="red", linestyle="--", linewidth=1)
        ymax = ax_pm.get_ylim()[1]
        ax_pm.text(
            cat_indices[-1] + 0.2,
            12,
            "Good threshold (12)",
            color="green",
            va="bottom",
            fontsize=8,
        )
        ax_pm.text(
            cat_indices[-1] + 0.2,
            35,
            "Unhealthy threshold (35)",
            color="red",
            va="bottom",
            fontsize=8,
        )
    else:
        ax_pm.text(
            0.5,
            0.5,
            "No PM2.5 data to summarize.",
            ha="center",
            va="center",
            transform=ax_pm.transAxes,
        )
        ax_pm.axis("off")

    # (2,1) Temperature boxplots by category
    has_any_temp = any(len(v) > 0 for v in temp_data)
    if has_any_temp:
        bp_temp = ax_temp.boxplot(
            temp_data,
            labels=used_cats,
            vert=True,
            patch_artist=True,
            showmeans=True,
        )
        for patch, cat in zip(bp_temp["boxes"], used_cats):
            patch.set_facecolor(color_map.get(cat, "#9E9E9E"))
            patch.set_alpha(0.7)
            patch.set_edgecolor("black")

        ax_temp.set_ylabel("Average Temperature (°C)")
        ax_temp.set_xlabel("Air Quality Category")
        ax_temp.set_title("Temperature Distribution by Air Quality Category")
        ax_temp.grid(axis="y", linestyle=":", alpha=0.4)
    else:
        ax_temp.text(
            0.5,
            0.5,
            "No temperature data for boxplots.",
            ha="center",
            va="center",
            transform=ax_temp.transAxes,
        )
        ax_temp.axis("off")

    # (2,2) Population boxplots by category (log scale)
    if has_any_pop:
        # Replace empty lists with [np.nan] to avoid errors, but they won't really show
        pop_data_clean = [
            vals if len(vals) > 0 else [np.nan]
            for vals in pop_data
        ]

        bp_pop = ax_pop.boxplot(
            pop_data_clean,
            labels=used_cats,
            vert=True,
            patch_artist=True,
            showmeans=True,
        )
        for patch, cat in zip(bp_pop["boxes"], used_cats):
            patch.set_facecolor(color_map.get(cat, "#9E9E9E"))
            patch.set_alpha(0.7)
            patch.set_edgecolor("black")

        ax_pop.set_yscale("log")
        ax_pop.set_ylabel("Population (log scale)")
        ax_pop.set_xlabel("Air Quality Category")
        ax_pop.set_title("Population Distribution by Air Quality Category")
        ax_pop.grid(axis="y", linestyle=":", alpha=0.4)
    else:
        ax_pop.text(
            0.5,
            0.5,
            "No population data for boxplots.",
            ha="center",
            va="center",
            transform=ax_pop.transAxes,
        )
        ax_pop.axis("off")

    # Shared super-title
    fig.suptitle("Air Quality Category Overview (Counts, PM2.5, Temperature, Population)", fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
        print(f"Saved advanced AQ category overview figure to {save_path}")

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