"""Centralized Plotly configuration, color palettes, and layout utilities for dashboard."""
import plotly.graph_objects as go
import streamlit as st
import pandas as pd

# ── Color Palettes ─────────────────────────────────────────────────────────────
SOURCE_PALETTE = {
    "observed": "#0891B2",      # Cyan-600
    "modeled": "#F59E0B",       # Amber-500
    "mixed": "#64748B",         # Slate-500
    "📡 Quan trắc mặt đất": "#0891B2",
    "🛰️ Mô hình vệ tinh": "#F59E0B",
    "📡 Mặt đất": "#0891B2",
    "🛰️ Vệ tinh": "#F59E0B",
}

RISK_PALETTE = {
    "risk_low": "#10B981",      # Emerald-500
    "risk_moderate": "#F59E0B", # Amber-500
    "risk_high": "#EF4444",     # Red-500
    "risk_critical": "#7F1D1D",  # Maroon-900
    "Thấp": "#10B981",
    "Trung bình": "#F59E0B",
    "Rủi ro cao": "#EF4444",
    "Nguy kịch": "#7F1D1D",
    "LOW": "#10B981",
    "MODERATE": "#F59E0B",
    "HIGH RISK": "#EF4444",
    "CRITICAL": "#7F1D1D",
}

AQI_PALETTE = {
    "Good": "#10B981",                        # Emerald-500 (premium green)
    "Moderate": "#EAB308",                    # Yellow-500 (readable gold)
    "Unhealthy for Sensitive Groups": "#F97316", # Orange-500
    "Unhealthy": "#EF4444",                   # Red-500
    "Very Unhealthy": "#8B5CF6",              # Purple-500
    "Hazardous": "#7F1D1D",                   # Red-900
    "Unknown": "#94A3B8",                     # Slate-400
}

# ── Preset Layout Configurations ─────────────────────────────────────────────
def get_plotly_layout(height=400, animate=False, compact=False):
    """Return a polished Plotly layout consistent with premium glassmorphism."""
    theme = st.session_state.get("theme", "light")
    text_color = "#E2E8F0" if theme == "dark" else "#0F172A"
    grid_color = "rgba(255, 255, 255, 0.05)" if theme == "dark" else "rgba(15, 23, 42, 0.05)"
    
    margin = {"l": 15, "r": 15, "t": 40, "b": 15} if compact else {"l": 30, "r": 20, "t": 50, "b": 30}
    
    layout = {
        "height": height,
        "paper_bgcolor": 'rgba(0,0,0,0)',
        "plot_bgcolor": 'rgba(0,0,0,0)',
        "font": {
            "family": "'Inter', 'Roboto', sans-serif",
            "color": text_color,
            "size": 11 if compact else 12
        },
        "margin": margin,
        "xaxis": {
            "gridcolor": grid_color,
            "zeroline": False,
            "showline": False,
            "tickfont": {"size": 10 if compact else 11}
        },
        "yaxis": {
            "gridcolor": grid_color,
            "zeroline": False,
            "showline": False,
            "tickfont": {"size": 10 if compact else 11}
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.05,
            "xanchor": "center",
            "x": 0.5,
            "title_text": None,
            "font": {"size": 10 if compact else 11}
        }
    }

    if animate:
        layout["transition"] = {"duration": 500, "easing": "cubic-in-out"}

    return layout

LAYOUT_COMPACT = get_plotly_layout(height=280, compact=True)
LAYOUT_WIDE = get_plotly_layout(height=400)
LAYOUT_HEATMAP = {
    **get_plotly_layout(height=450),
    "margin": {"l": 80, "r": 20, "t": 40, "b": 60}
}

# ── Dynamic Formatting Helpers ────────────────────────────────────────────────
def format_hover_template(value_name: str, val_format: str = ".1f") -> str:
    """Return a standard premium hover template for Plotly charts."""
    return (
        "<b>%{hovertext}</b><br>" +
        f"{value_name}: " + "<b>%{y:" + val_format + "}</b><br>" +
        "<extra></extra>"
    )

def create_empty_state(message: str, height: int = 250) -> go.Figure:
    """Return an empty Plotly figure with a beautifully styled state (theme-aware)."""
    theme = st.session_state.get("theme", "light")
    text_color = "#94A3B8" if theme == "dark" else "#64748B"
    bg_color = "rgba(15, 23, 42, 0.4)" if theme == "dark" else "rgba(241, 245, 249, 0.6)"
    border_color = "rgba(255, 255, 255, 0.05)" if theme == "dark" else "rgba(226, 232, 240, 0.8)"
    
    fig = go.Figure()
    fig.update_layout(
        xaxis={"visible": False, "showgrid": False},
        yaxis={"visible": False, "showgrid": False},
        annotations=[{
            "text": f"ℹ️  {message}",
            "x": 0.5, "y": 0.5,
            "showarrow": False,
            "font": {
                "family": "'Inter', sans-serif",
                "size": 13, 
                "color": text_color
            },
            "xref": "paper", "yref": "paper",
        }],
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 10, "r": 10, "t": 10, "b": 10},
    )
    return fig

def format_vietnamese_date(d) -> str:
    """Helper to format python dates into standard Vietnamese format (dd/mm/yyyy)."""
    if pd.isna(d) or d is None:
        return "N/A"
    if hasattr(d, "strftime"):
        return d.strftime("%d/%m/%Y")
    return str(d)
