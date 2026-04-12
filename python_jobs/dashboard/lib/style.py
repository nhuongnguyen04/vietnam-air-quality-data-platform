import streamlit as st
from lib.i18n import t

def inject_style():
    """Inject custom CSS for premium UI look and feel."""
    theme = st.session_state.get("theme", "light")
    
    if theme == "dark":
        bg_color = "#0e1117"
        text_color = "#f0f2f6"
        card_bg = "rgba(17, 25, 40, 0.75)"
        border_color = "rgba(255, 255, 255, 0.1)"
        sidebar_bg = "#111b21"
        shadow = "0 8px 32px 0 rgba(0, 0, 0, 0.37)"
    else:
        bg_color = "#f8f9fb"
        text_color = "#1e293b"
        card_bg = "rgba(255, 255, 255, 0.8)"
        border_color = "rgba(226, 232, 240, 0.8)"
        sidebar_bg = "#ffffff"
        shadow = "0 8px 32px 0 rgba(31, 38, 135, 0.07)"

    st.markdown(f"""
    <style>
        /* ── Base Styles ─────────────────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap');
        
        html, body, [data-testid="stAppViewContainer"], .stMarkdown {{
            font-family: 'Montserrat', sans-serif !important;
            background-color: {bg_color};
            color: {text_color};
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
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid {border_color};
            border-radius: 16px;
            padding: 1.2rem;
            margin-bottom: 1.5rem;
            box-shadow: {shadow};
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
        "health": '<path d="M19 3H5c-1.1 0-1.99.9-1.99 2L3 19c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-1 11h-4v4h-4v-4H6v-4h4V6h4v4h4v4z"/>'
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

def get_plotly_layout(height=400):
    """Return a polished Plotly layout consistent with the glassmorphism theme."""
    theme = st.session_state.get("theme", "light")
    text_color = "#f0f2f6" if theme == "dark" else "#1e293b"
    grid_color = "rgba(255, 255, 255, 0.05)" if theme == "dark" else "rgba(0, 0, 0, 0.05)"
    
    return dict(
        height=height,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Montserrat', color=text_color),
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(gridcolor=grid_color, zeroline=False),
        yaxis=dict(gridcolor=grid_color, zeroline=False),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
