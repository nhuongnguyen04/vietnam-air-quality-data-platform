"""AQI utility functions for Streamlit dashboard."""


def get_aqi_category(aqi: float) -> str:
    """Return AQI category label for a given AQI value."""
    if aqi is None:
        return "Unknown"
    if aqi <= 50:
        return "Good"
    elif aqi <= 100:
        return "Moderate"
    elif aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    elif aqi <= 200:
        return "Unhealthy"
    elif aqi <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


def get_aqi_color(aqi: float) -> str:
    """Return hex color for AQI category."""
    category = get_aqi_category(aqi)
    colors = {
        "Good": "#00E400",
        "Moderate": "#FFFF00",
        "Unhealthy for Sensitive Groups": "#FF7E00",
        "Unhealthy": "#FF0000",
        "Very Unhealthy": "#8F3F97",
        "Hazardous": "#7E0023",
        "Unknown": "#808080",
    }
    return colors.get(category, "#808080")


def get_aqi_color_name(aqi: float) -> str:
    """Return CSS color name for AQI category."""
    category = get_aqi_category(aqi)
    names = {
        "Good": "green",
        "Moderate": "yellow",
        "Unhealthy for Sensitive Groups": "orange",
        "Unhealthy": "red",
        "Very Unhealthy": "purple",
        "Hazardous": "maroon",
        "Unknown": "gray",
    }
    return names.get(category, "gray")


# === AQI Breakpoints ===
AQI_BREAKPOINTS = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]

# Unified AQI color map cho Plotly color_discrete_map
AQI_CATEGORY_COLORS = {
    "Good": "#00E400",
    "Moderate": "#FFFF00",
    "Unhealthy for Sensitive Groups": "#FF7E00",
    "Unhealthy": "#FF0000",
    "Very Unhealthy": "#8F3F97",
    "Hazardous": "#7E0023",
}

EPA_COLORS = AQI_CATEGORY_COLORS
VN_AQI_COLORS = AQI_CATEGORY_COLORS

VN_AQI_COLORBAR_TICKVALS = [25, 75, 125, 175, 250, 400]
VN_AQI_COLORBAR_TICKTEXT = ["0-50", "51-100", "101-150", "151-200", "201-300", "301-500"]

# EPA sequential scale cho color_continuous_scale (dùng cho numeric AQI)
EPA_SEQUENTIAL_SCALE = [
    [0.0, "#00E400"],      # Good
    [0.17, "#FFFF00"],     # Moderate
    [0.33, "#FF7E00"],     # USG
    [0.5, "#FF0000"],      # Unhealthy
    [0.67, "#8F3F97"],     # Very Unhealthy
    [1.0, "#7E0023"],      # Hazardous
]


def _build_step_scale(colors, max_value: int = 500):
    """Build a Plotly colorscale with hard AQI category boundaries."""
    scale = []
    for index, (lo, hi, category) in enumerate(AQI_BREAKPOINTS):
        color = colors[category]
        start = max(lo - 1, 0) / max_value if index else 0.0
        end = min(hi, max_value) / max_value
        scale.append([start, color])
        scale.append([end, color])
    return scale


def get_vn_aqi_step_scale():
    """Return fixed VN_AQI color bands for Plotly numeric AQI charts."""
    return _build_step_scale(VN_AQI_COLORS)


def get_aqi_color_scale(standard: str = "VN_AQI"):
    """Return the appropriate AQI color scale for the selected standard."""
    if standard == "VN_AQI":
        return get_vn_aqi_step_scale()
    return get_epa_continuous_scale()


def get_aqi_color_range(standard: str = "VN_AQI"):
    """Return fixed AQI numeric range for color axes."""
    if standard == "VN_AQI":
        return [0, 500]
    return [0, 300]


def get_aqi_discrete_colors(standard: str = "VN_AQI"):
    """Return category colors keyed by canonical AQI category labels."""
    if standard == "VN_AQI":
        return VN_AQI_COLORS
    return EPA_COLORS


def get_aqi_colorbar_config(standard: str = "VN_AQI", title: str = "AQI"):
    """Return a Plotly colorbar config aligned to the selected AQI standard."""
    config = {"title": {"text": title}}
    if standard == "VN_AQI":
        config.update(
            {
                "tickmode": "array",
                "tickvals": VN_AQI_COLORBAR_TICKVALS,
                "ticktext": VN_AQI_COLORBAR_TICKTEXT,
            }
        )
    return config


def get_epa_continuous_scale():
    """Return EPA-themed sequential color scale for Plotly heatmaps.

    Returns:
        List of [position, hex_color] pairs for color_continuous_scale.
    """
    return [
        [0.00, "#00E400"],   # Good
        [0.15, "#9ACC00"],   # Good → Moderate transition
        [0.25, "#FFFF00"],   # Moderate
        [0.35, "#FF9900"],   # Moderate → USG transition
        [0.45, "#FF7E00"],   # Unhealthy for Sensitive Groups
        [0.55, "#FF6600"],   # USG → Unhealthy transition
        [0.65, "#FF0000"],   # Unhealthy
        [0.80, "#8F3F97"],   # Very Unhealthy
        [1.00, "#7E0023"],   # Hazardous
    ]


def get_epa_color_for_value(aqi: float) -> str:
    """Return EPA hex color for a numeric AQI value.

    Args:
        aqi: AQI numeric value (0-500+)

    Returns:
        EPA hex color string.
    """
    if aqi is None:
        return "#808080"
    for lo, hi, category in AQI_BREAKPOINTS:
        if lo <= aqi <= hi:
            return EPA_COLORS[category]
    return EPA_COLORS["Hazardous"]


def apply_epa_template(fig, height: int = 400):
    """Apply consistent EPA-themed layout to a Plotly figure.

    Args:
        fig: plotly.graph_objects.Figure object
        height: figure height in pixels

    Returns:
        Modified figure with EPA theme applied.
    """
    fig.update_layout(
        font={"family": "sans-serif", "size": 12},
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="rgba(240,242,246,0.4)",
        margin={"l": 20, "r": 20, "t": 30, "b": 20},
        height=height,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(200,200,200,0.3)",
        title_font={"size": 11},
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(200,200,200,0.3)",
        title_font={"size": 11},
    )
    fig.update_traces(marker_line_width=0)
    return fig


def render_empty_chart(message: str, height: int = 250):
    """Return an empty Plotly figure with a descriptive message (theme-aware)."""
    from lib.chart_config import create_empty_state
    return create_empty_state(message, height)



AQI_VN_SQL_EXPR = """
CASE
    WHEN LOWER(parameter) = 'pm25' THEN
        CASE
            WHEN value <= 25 THEN ((50.0 - 0.0) / (25.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 50 THEN ((100.0 - 51.0) / (50.0 - 26.0)) * (value - 26.0) + 51.0
            WHEN value <= 80 THEN ((150.0 - 101.0) / (80.0 - 51.0)) * (value - 51.0) + 101.0
            WHEN value <= 150 THEN ((200.0 - 151.0) / (150.0 - 81.0)) * (value - 81.0) + 151.0
            WHEN value <= 250 THEN ((300.0 - 201.0) / (250.0 - 151.0)) * (value - 151.0) + 201.0
            WHEN value <= 350 THEN ((400.0 - 301.0) / (350.0 - 251.0)) * (value - 251.0) + 301.0
            WHEN value <= 500 THEN ((500.0 - 401.0) / (500.0 - 351.0)) * (value - 351.0) + 401.0
            ELSE NULL
        END
    WHEN LOWER(parameter) = 'pm10' THEN
        CASE
            WHEN value <= 50 THEN ((50.0 - 0.0) / (50.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 150 THEN ((100.0 - 51.0) / (150.0 - 51.0)) * (value - 51.0) + 51.0
            WHEN value <= 250 THEN ((150.0 - 101.0) / (250.0 - 151.0)) * (value - 151.0) + 101.0
            WHEN value <= 350 THEN ((200.0 - 151.0) / (350.0 - 251.0)) * (value - 251.0) + 151.0
            WHEN value <= 420 THEN ((300.0 - 201.0) / (420.0 - 351.0)) * (value - 351.0) + 201.0
            WHEN value <= 500 THEN ((400.0 - 301.0) / (500.0 - 421.0)) * (value - 421.0) + 301.0
            WHEN value <= 600 THEN ((500.0 - 401.0) / (600.0 - 501.0)) * (value - 501.0) + 401.0
            ELSE NULL
        END
    WHEN LOWER(parameter) = 'o3' THEN
        CASE
            WHEN value <= 160 THEN ((50.0 - 0.0) / (160.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 200 THEN ((100.0 - 51.0) / (200.0 - 161.0)) * (value - 161.0) + 51.0
            WHEN value <= 240 THEN ((150.0 - 101.0) / (240.0 - 201.0)) * (value - 201.0) + 101.0
            WHEN value <= 280 THEN ((200.0 - 151.0) / (280.0 - 241.0)) * (value - 241.0) + 151.0
            WHEN value <= 400 THEN ((300.0 - 201.0) / (400.0 - 281.0)) * (value - 281.0) + 201.0
            WHEN value <= 500 THEN ((400.0 - 301.0) / (500.0 - 401.0)) * (value - 401.0) + 301.0
            WHEN value <= 600 THEN ((500.0 - 401.0) / (600.0 - 501.0)) * (value - 501.0) + 401.0
            ELSE NULL
        END
    WHEN LOWER(parameter) = 'so2' THEN
        CASE
            WHEN value <= 125 THEN ((50.0 - 0.0) / (125.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 350 THEN ((100.0 - 51.0) / (350.0 - 126.0)) * (value - 126.0) + 51.0
            WHEN value <= 550 THEN ((150.0 - 101.0) / (550.0 - 351.0)) * (value - 351.0) + 101.0
            WHEN value <= 800 THEN ((200.0 - 151.0) / (800.0 - 551.0)) * (value - 551.0) + 151.0
            WHEN value <= 1600 THEN ((300.0 - 201.0) / (1600.0 - 801.0)) * (value - 801.0) + 201.0
            WHEN value <= 2100 THEN ((400.0 - 301.0) / (2100.0 - 1601.0)) * (value - 1601.0) + 301.0
            WHEN value <= 2630 THEN ((500.0 - 401.0) / (2630.0 - 2101.0)) * (value - 2101.0) + 401.0
            ELSE NULL
        END
    WHEN LOWER(parameter) = 'no2' THEN
        CASE
            WHEN value <= 40 THEN ((50.0 - 0.0) / (40.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 80 THEN ((100.0 - 51.0) / (80.0 - 41.0)) * (value - 41.0) + 51.0
            WHEN value <= 180 THEN ((150.0 - 101.0) / (180.0 - 81.0)) * (value - 81.0) + 101.0
            WHEN value <= 280 THEN ((200.0 - 151.0) / (280.0 - 181.0)) * (value - 181.0) + 151.0
            WHEN value <= 565 THEN ((300.0 - 201.0) / (565.0 - 281.0)) * (value - 281.0) + 201.0
            WHEN value <= 750 THEN ((400.0 - 301.0) / (750.0 - 566.0)) * (value - 566.0) + 301.0
            WHEN value <= 940 THEN ((500.0 - 401.0) / (940.0 - 751.0)) * (value - 751.0) + 401.0
            ELSE NULL
        END
    WHEN LOWER(parameter) = 'co' THEN
        CASE
            WHEN value <= 10000 THEN ((50.0 - 0.0) / (10000.0 - 0.0)) * (value - 0.0) + 0.0
            WHEN value <= 30000 THEN ((100.0 - 51.0) / (30000.0 - 10001.0)) * (value - 10001.0) + 51.0
            WHEN value <= 45000 THEN ((150.0 - 101.0) / (45000.0 - 30001.0)) * (value - 30001.0) + 101.0
            WHEN value <= 60000 THEN ((200.0 - 151.0) / (60000.0 - 45001.0)) * (value - 45001.0) + 151.0
            WHEN value <= 90000 THEN ((300.0 - 201.0) / (90000.0 - 60001.0)) * (value - 60001.0) + 201.0
            WHEN value <= 120000 THEN ((400.0 - 301.0) / (120000.0 - 90001.0)) * (value - 90001.0) + 301.0
            WHEN value <= 150000 THEN ((500.0 - 401.0) / (150000.0 - 120001.0)) * (value - 120001.0) + 401.0
            ELSE NULL
        END
    ELSE NULL
END
"""
