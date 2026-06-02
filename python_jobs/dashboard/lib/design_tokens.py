import streamlit as st

def get_theme_tokens(theme: str = None) -> dict:
    """Centralized design system tokens for Streamlit dashboard.
    Enforces unified colors, spacings, borders, and shadows across light/dark themes.
    """
    theme = theme or st.session_state.get("theme", "light")
    if theme == "dark":
        return {
            "bg": "rgba(30, 41, 59, 0.45)",  # slate-800 glass
            "card_bg": "rgba(15, 23, 42, 0.7)",  # slate-950 glass
            "border": "rgba(255, 255, 255, 0.08)",
            "text": "#f8fafc",                  # slate-50
            "text_secondary": "#cbd5e1",        # slate-300
            "label": "#94a3b8",                 # slate-400
            "accent": "#0891b2",                # cyan-600
            "accent_light": "rgba(8, 145, 178, 0.15)",
            "shadow": "0 10px 25px -5px rgba(0, 0, 0, 0.3)",
            "glass_blur": "blur(12px)",
        }
    else:
        return {
            "bg": "rgba(255, 255, 255, 0.85)",
            "card_bg": "#ffffff",
            "border": "rgba(226, 232, 240, 0.8)",
            "text": "#0f172a",                  # slate-900
            "text_secondary": "#475569",        # slate-600
            "label": "#64748b",                 # slate-500
            "accent": "#0891b2",                # cyan-600
            "accent_light": "rgba(8, 145, 178, 0.05)",
            "shadow": "0 10px 25px -5px rgba(0, 0, 0, 0.05)",
            "glass_blur": "blur(8px)",
        }
