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
