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


# === EPA AQI Breakpoints (Phase 07) ===
EPA_BREAKPOINTS = [
    (0, 50, "Good", "#00E400"),
    (51, 100, "Moderate", "#FFFF00"),
    (101, 150, "Unhealthy for Sensitive Groups", "#FF7E00"),
    (151, 200, "Unhealthy", "#FF0000"),
    (201, 300, "Very Unhealthy", "#8F3F97"),
    (301, 500, "Hazardous", "#7E0023"),
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

# EPA sequential scale cho color_continuous_scale (dùng cho numeric AQI)
EPA_SEQUENTIAL_SCALE = [
    [0.0, "#00E400"],      # Good
    [0.17, "#FFFF00"],     # Moderate
    [0.33, "#FF7E00"],     # USG
    [0.5, "#FF0000"],      # Unhealthy
    [0.67, "#8F3F97"],     # Very Unhealthy
    [1.0, "#7E0023"],      # Hazardous
]


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
    for lo, hi, _, color in EPA_BREAKPOINTS:
        if lo <= aqi <= hi:
            return color
    return "#7E0023"  # Hazardous fallback


def apply_epa_template(fig, height: int = 400):
    """Apply consistent EPA-themed layout to a Plotly figure.

    Args:
        fig: plotly.graph_objects.Figure object
        height: figure height in pixels

    Returns:
        Modified figure with EPA theme applied.
    """
    fig.update_layout(
        font=dict(family="sans-serif", size=12),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="rgba(240,242,246,0.4)",
        margin=dict(l=20, r=20, t=30, b=20),
        height=height,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(200,200,200,0.3)",
        title_font=dict(size=11),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(200,200,200,0.3)",
        title_font=dict(size=11),
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
        xaxis=dict(visible=False, showgrid=False),
        yaxis=dict(visible=False, showgrid=False),
        annotations=[dict(
            text=message,
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=14, color=text_color),
            xref="paper", yref="paper",
        )],
        height=height,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        margin=dict(l=10, r=10, t=30, b=10),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    return fig
