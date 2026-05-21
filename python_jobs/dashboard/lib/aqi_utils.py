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

# EPA color map cho Plotly color_discrete_map (dùng category name làm key)
EPA_COLORS = {
    "Good": "#00E400",
    "Moderate": "#FFFF00",
    "Unhealthy for Sensitive Groups": "#FF7E00",
    "Unhealthy": "#FF0000",
    "Very Unhealthy": "#8F3F97",
    "Hazardous": "#7E0023",
}

VN_AQI_COLORS = {
    "Good": "#00E400",
    "Moderate": "#FFFF00",
    "Unhealthy for Sensitive Groups": "#FF7E00",
    "Unhealthy": "#FF0000",
    "Very Unhealthy": "#8F3F97",
    "Hazardous": "#7E0023",
}

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
    import plotly.graph_objects as go
    import streamlit as st

    theme = st.session_state.get("theme", "light")
    if theme == "dark":
        text_color = "#94a3b8"   # slate-400
        bg_color = "rgba(15,23,42,0.6)"
    else:
        text_color = "#64748b"   # slate-500
        bg_color = "rgba(255,255,255,0.7)"

    fig = go.Figure()
    fig.update_layout(
        xaxis={"visible": False, "showgrid": False},
        yaxis={"visible": False, "showgrid": False},
        annotations=[{
            "text": message,
            "x": 0.5, "y": 0.5,
            "showarrow": False,
            "font": {"size": 14, "color": text_color},
            "xref": "paper", "yref": "paper",
        }],
        height=height,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        transition={"duration": 300, "easing": "cubic-in-out"},
    )
    return fig
