import streamlit as st
from lib.i18n import t

def inject_style():
    """Inject custom CSS for premium UI look and feel."""
    theme = st.session_state.get("theme", "light")

    if theme == "dark":
        bg_color = "#020617"
        text_color = "#cbd5e1"
        card_bg = "rgba(15,23,42,0.6)"
        border_color = "rgba(255,255,255,0.08)"
        sidebar_bg = "#0f172a"
        shadow = "0 8px 32px 0 rgba(0,0,0,0.4)"
        glass_blur = "blur(16px)"
    else:
        bg_color = "#f8fafb"
        text_color = "#0f172a"
        card_bg = "rgba(255,255,255,0.7)"
        border_color = "rgba(226,232,240,0.8)"
        sidebar_bg = "#ffffff"
        shadow = "0 8px 32px 0 rgba(31,38,135,0.07)"
        glass_blur = "blur(12px)"

    st.markdown(f"""
    <style>
        /* ── Base Styles ─────────────────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

        html, body, [data-testid="stAppViewContainer"], .stMarkdown {{
            font-family: 'Inter', sans-serif !important;
            background-color: {bg_color};
            color: {text_color};
            transition: background-color 0.3s ease, color 0.3s ease;
        }}

        header[data-testid="stHeader"] {{ visibility: hidden; height: 0; }}
        footer {{ visibility: hidden; }}
        .block-container {{ padding-top: 1rem !important; padding-bottom: 5rem !important; }}

        /* ── Sidebar Polish ──────────────────────────────── */
        [data-testid="stSidebar"] {{
            background-color: {sidebar_bg} !important;
            border-right: 1px solid {border_color};
            min-width: 250px !important;
        }}
        [data-testid="stSidebarNav"] {{
            padding-top: 0 !important;
        }}
        [data-testid="stSidebarNav"] span {{
            color: {text_color} !important;
            font-weight: 600;
            font-size: 0.95rem;
        }}
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {{
            font-size: 1.4rem !important;
            font-weight: 700 !important;
            margin-bottom: 0 !important;
            padding-top: 1.5rem !important;
        }}

        /* ── Top Bar Columns Alignment ──────────────────── */
        [data-testid="stHorizontalBlock"] {{
            align-items: center;
        }}

        /* ── Glass Card & Charts ─────────────────────────── */
        .glass-card, .stPlotlyChart {{
            background: {card_bg};
            backdrop-filter: {glass_blur};
            -webkit-backdrop-filter: {glass_blur};
            border: 1px solid {border_color};
            border-radius: 16px;
            padding: 1.2rem;
            margin-bottom: 1.5rem;
            box-shadow: {shadow};
            animation: fadeIn 0.4s ease-out;
        }}

        .glass-card:hover {{
            box-shadow: 0 12px 40px rgba(31,38,135,0.12);
            transition: box-shadow 0.3s ease;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(8px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        /* ── Metric Card ─────────────────────────────────── */
        .metric-card {{
            display: flex;
            align-items: center;
            gap: 1rem;
            padding: 0.5rem 0;
        }}
        .metric-icon {{
            background: {bg_color};
            border: 1px solid {border_color};
            border-radius: 12px;
            width: 48px;
            height: 48px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }}
        .metric-icon svg {{
            fill: {text_color};
            width: 26px;
            height: 26px;
        }}
        .metric-value {{
            font-size: 1.8rem;
            font-weight: 700;
            line-height: 1.1;
        }}
        .metric-label {{
            font-size: 0.9rem;
            opacity: 0.7;
            margin-bottom: 2px;
        }}

        /* ── Buttons ────────────────────────────────────── */
        .stButton button {{
            border-radius: 10px !important;
            font-weight: 600 !important;
            transition: all 0.2s ease;
        }}
        .stButton > button:hover {{
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}
    </style>
    """, unsafe_allow_html=True)

def render_top_bar():
    """Render a persistent top bar for theme and language toggles on one row, aligned right."""
    theme = st.session_state.get("theme", "light")
    lang = st.session_state.get("lang", "vi")

    # Use columns to align right and keep on one row
    c1, spacer, c2, c3 = st.columns([0.6, 0.2, 0.1, 0.1])

    with c2:
        if st.button("🌙" if theme == "light" else "☀️", key="theme_toggle"):
            st.session_state.theme = "dark" if theme == "light" else "light"
            st.rerun()

    with c3:
        if st.button("EN" if lang == "vi" else "VI", key="lang_toggle"):
            st.session_state.lang = "en" if lang == "vi" else "vi"
            st.rerun()

def render_metric_card(label, value, delta=None, delta_color="normal", icon=None):
    """Render a custom premium metric card with SVG icons."""
    icons = {
        "traffic": '<path d="M20 7h-9l1.45-3.45a1 1 0 0 0-1.82-.72L8.43 7H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2zM4 9h16v10h-6v-2h-4v2H4V9z"/>',
        "insights": '<path d="m16 6 2.29 2.29-4.88 4.88-4-4L2 16.59 3.41 18l6-6 4 4 6.3-6.29L22 12V6h-6z"/>',
        "error": '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/>',
        "star": '<path d="M12 17.27 18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21 12 17.27z"/>',
        "biotech": '<path d="M7 19c-1.1 0-2 .9-2 2h14c0-1.1-.9-2-2-2H7zM6.9 9.88l-2.02.32c-.39.06-.67.43-.61.82l.13.82c.06.39.43.67.82.61l2.02-.32c.39-.06.67-.43.61-.82l-.13-.82c-.06-.39-.43-.67-.82-.61zm.55 3.51-.32 2.02c-.06.39.21.76.61.82l.82.13c.39.06.76-.21.82-.61l.32-2.02c.06-.39-.21-.76-.61-.82l-.82-.13c-.39-.06-.76.21-.82.61zm11.65-3.51.13.82c.06.39.43.67.82.61l2.02-.32c.39-.06.67-.43.61-.82l-.13-.82c-.06-.39-.43-.67-.82-.61l-2.02.32c-.39.06-.67.43-.61.82zm-.55 3.51.82.13c.39.06.76-.21.82-.61l.32-2.02c.06-.39-.21-.76-.61-.82l-.82-.13c-.39-.06-.76.21-.82.61l-.32 2.02c-.06.39.21.76.61.82zm-7.97-1.4c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2z"/><path d="M12 3C7.58 3 4 6.58 4 11c0 3.17 1.86 5.92 4.57 7.21l1.41-3.66C8.78 13.9 8 12.55 8 11c0-2.21 1.79-4 4-4s4 1.79 4 4c0 1.55-.78 2.9-1.98 3.55l1.41 3.66C18.14 16.92 20 14.17 20 11c0-4.42-3.58-8-8-8z"/>',
        "location": '<path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"/>',
        "health": '<path d="M19 3H5c-1.1 0-1.99.9-1.99 2L3 19c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-1 11h-4v4h-4v-4H6v-4h4V6h4v4h4v4z"/>',
        "device_thermostat": '<path d="M15 13V5c0-1.66-1.34-3-3-3S9 3.34 9 5v8c-1.21.91-2 2.37-2 4 0 2.76 2.24 5 5 5s5-2.24 5-5c0-1.63-.79-3.09-2-4zm-4-2V5c0-.55.45-1 1-1s1 .45 1 1v1h-1v1h1v2h-1v1h1v1h-2z"/>',
        "humidity_percentage": '<path d="M12 2c-5.33 4.55-8 8.48-8 11.8 0 4.98 3.8 8.2 8 8.2s8-3.22 8-8.2c0-3.32-2.67-7.25-8-11.8zm2.42 13.84c-.39.11-.79-.12-.9-.51-.11-.39.12-.79.51-.9.51-.14.8-.62.8-1.2 0-.66-.54-1.2-1.2-1.2s-1.2.54-1.2 1.2c0 .35-.14.67-.37.9-.23.23-.55.37-.9.37-.35 0-.67-.14-.9-.37-.23-.23-.37-.55-.37-.9 0-1.99 1.61-3.6 3.6-3.6s3.6 1.61 3.6 3.6c0 1.25-.66 2.32-1.77 2.61z"/>',
        "air": '<path d="M13.96 12.29l.75-2.12c.11-.3.40-.51.72-.51h3.57v2h-2.92l-.71 2h3.63v2h-4.28c-.32 0-.61-.21-.72-.51l-.75-2.12c-.22-.62-.22-1.24 0-1.86zm-5.92 0l.75-2.12c.11-.3.40-.51.72-.51h3.57v2H10.16l-.71 2h3.63v2H9.00a.75.75 0 01-.72-.51l-.75-2.12c-.22-.62-.22-1.24 0-1.86zm-7.04 0l.75-2.12c.11-.3.40-.51.72-.51h3.57v2H3.12l-.71 2h3.63v2H2a.75.75 0 01-.72-.51L.53 14.15c-.22-.62-.22-1.24 0-1.86z"/>'
    }

    icon_svg = f'<svg viewBox="0 0 24 24">{icons.get(icon, "")}</svg>' if icon in icons else ""

    st.markdown(f"""
        <div class="glass-card">
            <div class="metric-card">
                <div class="metric-icon">{icon_svg}</div>
                <div>
                    <div class="metric-label">{label}</div>
                    <div class="metric-value">{value}</div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def get_plotly_layout(height=400, animate=False):
    """Return a polished Plotly layout consistent with the glassmorphism theme."""
    theme = st.session_state.get("theme", "light")
    text_color = "#cbd5e1" if theme == "dark" else "#0f172a"
    grid_color = "rgba(255, 255, 255, 0.05)" if theme == "dark" else "rgba(0, 0, 0, 0.05)"

    layout = dict(
        height=height,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, sans-serif', color=text_color),
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(gridcolor=grid_color, zeroline=False),
        yaxis=dict(gridcolor=grid_color, zeroline=False),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            title_text=None
        )
    )

    if animate:
        layout["transition"] = dict(duration=500, easing="cubic-in-out")

    return layout