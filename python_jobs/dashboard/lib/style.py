import streamlit as st

def inject_style():
    """Inject custom CSS for premium UI look and feel."""
    theme = st.session_state.get("theme", "light")

    if theme == "dark":
        bg_color = "#020617"             # slate-950
        text_color = "#cbd5e1"           # slate-300
        card_bg = "rgba(15, 23, 42, 0.65)" # slate-900 with glass
        border_color = "rgba(255, 255, 255, 0.08)"
        sidebar_bg = "#0f172a"           # slate-900
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -2px rgba(0, 0, 0, 0.2)"
        glass_blur = "blur(12px)"
        accent_color = "#0891b2"         # cyan-600
        accent_glow = "rgba(8, 145, 178, 0.25)"
        divider_color = "rgba(255, 255, 255, 0.1)"
    else:
        bg_color = "#f8fafc"             # slate-50
        text_color = "#0f172a"           # slate-900
        card_bg = "rgba(255, 255, 255, 0.85)"
        border_color = "rgba(226, 232, 240, 0.8)"
        sidebar_bg = "#ffffff"
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.05)"
        glass_blur = "blur(8px)"
        accent_color = "#0891b2"
        accent_glow = "rgba(8, 145, 178, 0.15)"
        divider_color = "rgba(15, 23, 42, 0.08)"

    st.markdown(f"""
    <style>
        /* ── Base Styles & Typography ─────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0');

        html, body, [data-testid="stAppViewContainer"], .stMarkdown {{
            font-family: 'Inter', sans-serif !important;
            background-color: {bg_color};
            color: {text_color};
            transition: background-color 0.3s ease, color 0.3s ease;
        }}

        /* Restore Material Icons Font Family to prevent raw text ligatures */
        span[data-testid="stIconMaterial"], [class*="material-symbols"], [class*="material-icons"] {{
            font-family: 'Material Symbols Outlined', 'Material Symbols Sharp', 'Material Symbols Rounded', 'Material Icons', 'Material Icons Round' !important;
        }}



        h1, h2, h3, .page-hero-title {{
            font-family: 'Outfit', sans-serif !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em;
        }}

        header[data-testid="stHeader"] {{ visibility: hidden; height: 0; }}
        footer {{ visibility: hidden; }}
        .block-container {{ padding-top: 0.25rem !important; padding-bottom: 0.35rem !important; }}

        /* ── Custom Scrollbars ────────────────────────────── */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        ::-webkit-scrollbar-track {{
            background: rgba(0, 0, 0, 0.02);
        }}
        ::-webkit-scrollbar-thumb {{
            background: rgba(148, 163, 184, 0.3);
            border-radius: 10px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(148, 163, 184, 0.5);
        }}

        /* ── Sidebar Polish ──────────────────────────────── */
        [data-testid="stSidebar"] {{
            background-color: {sidebar_bg} !important;
            border-right: 1px solid {border_color};
            min-width: 260px !important;
        }}
        [data-testid="stSidebar"] > div:first-child [data-testid="stVerticalBlockBorderWrapper"] > div {{
            display: flex;
            flex-direction: column;
        }}
        [data-testid="stSidebarNav"] {{
            padding-top: 0 !important;
            order: 2;
        }}
        .sidebar-brand-container {{
            order: 1;
            margin-top: 1rem;
        }}
        .sidebar-filters-container {{
            order: 3;
        }}
        [data-testid="stSidebarNav"] span {{
            color: {text_color} !important;
            font-family: 'Outfit', sans-serif;
            font-weight: 600;
            font-size: 0.95rem;
        }}
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.5rem !important;
            font-weight: 800 !important;
            margin-bottom: 0.5rem !important;
            padding-top: 1.5rem !important;
            background: linear-gradient(135deg, {accent_color}, #06b6d4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        /* ── Page Hero Header ────────────────────────────── */
        .page-hero {{
            background: linear-gradient(135deg, {card_bg}, rgba(8, 145, 178, 0.04));
            border: 1px solid {border_color};
            border-left: 5px solid {accent_color};
            border-radius: 12px;
            padding: 0.5rem 0.85rem;
            margin-bottom: 0.5rem;
            box-shadow: {shadow};
            animation: slideDown 0.4s cubic-bezier(0.16, 1, 0.3, 1) forwards;
        }}
        .page-hero-content {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}
        .page-hero-icon {{
            font-size: 1.5rem;
            background: {bg_color};
            border: 1px solid {border_color};
            padding: 0.3rem;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .page-hero-title {{
            margin: 0 !important;
            font-size: 1.3rem !important;
            line-height: 1.2;
            color: {text_color};
        }}
        .page-hero-subtitle {{
            margin: 0.15rem 0 0 0 !important;
            font-size: 0.8rem;
            opacity: 0.75;
            line-height: 1.3;
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
            border-radius: 12px;
            padding: 0.75rem;
            margin-bottom: 0.65rem;
            box-shadow: {shadow};
            animation: fadeIn 0.45s cubic-bezier(0.16, 1, 0.3, 1) forwards;
            overflow: hidden;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}

        .glass-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 16px 40px {accent_glow};
            border-color: rgba(8, 145, 178, 0.3);
        }}

        /* ── Metric Card ─────────────────────────────────── */
        .metric-card {{
            display: flex;
            align-items: center;
            gap: 1rem;
            padding: 0.1rem 0;
        }}
        .metric-icon {{
            background: {bg_color};
            border: 1px solid {border_color};
            border-radius: 12px;
            width: 46px;
            height: 46px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
            transition: transform 0.2s ease;
        }}
        .metric-card:hover .metric-icon {{
            transform: scale(1.05);
            border-color: {accent_color};
        }}
        .metric-icon svg {{
            fill: {accent_color};
            width: 22px;
            height: 22px;
        }}
        .metric-value {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.75rem;
            font-weight: 800;
            line-height: 1.1;
            color: {text_color};
        }}
        .metric-label {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            font-weight: 600;
            opacity: 0.7;
            margin-bottom: 2px;
            white-space: normal;
            line-height: 1.3;
        }}

        /* ── City Metric (Special Dual Value) ────────────── */
        .city-card {{
            padding: 1rem;
        }}
        .city-name {{
            font-family: 'Outfit', sans-serif;
            font-size: 1rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: {text_color};
        }}
        .city-main-val {{
            font-family: 'Outfit', sans-serif;
            font-size: 2rem;
            font-weight: 800;
            line-height: 1;
            color: {accent_color};
        }}
        .city-sub-row {{
            display: flex;
            align-items: center;
            gap: 6px;
            margin-top: 6px;
            font-size: 0.8rem;
        }}
        .city-sub-label {{
            opacity: 0.6;
        }}

        /* ── Section Divider ─────────────────────────────── */
        .section-divider {{
            height: 1px;
            background: {divider_color};
            margin: 0.5rem 0;
            border-radius: 1px;
        }}

        /* ── Loading Skeleton ────────────────────────────── */
        .loading-skeleton {{
            background: {card_bg};
            border: 1px solid {border_color};
            border-radius: 16px;
            position: relative;
            overflow: hidden;
            margin-bottom: 1.5rem;
        }}
        .skeleton-pulse {{
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, 
                rgba(148, 163, 184, 0.0) 0%, 
                rgba(148, 163, 184, 0.08) 50%, 
                rgba(148, 163, 184, 0.0) 100%
            );
            animation: pulse 1.5s infinite;
        }}

        /* ── Animations ──────────────────────────────────── */
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(12px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        @keyframes slideDown {{
            from {{ opacity: 0; transform: translateY(-16px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        @keyframes pulse {{
            0% {{ transform: translateX(-100%); }}
            100% {{ transform: translateX(100%); }}
        }}

        /* ── Streamlit Native Element Styling ─────────────── */
        .stButton button {{
            border-radius: 10px !important;
            font-family: 'Outfit', sans-serif !important;
            font-weight: 600 !important;
            transition: all 0.2s ease !important;
            border: 1px solid {border_color} !important;
            background-color: {card_bg} !important;
        }}
        .stButton > button:hover {{
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(8, 145, 178, 0.15) !important;
            border-color: {accent_color} !important;
            color: {accent_color} !important;
        }}

        /* Expander customization */
        .st-emotion-cache-p2wz29 {{
            background-color: {card_bg} !important;
            border: 1px solid {border_color} !important;
            border-radius: 12px !important;
        }}

        /* Responsive Breakpoints */
        @media (max-width: 768px) {{
            .page-hero-content {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.75rem;
            }}
            .page-hero-icon {{
                font-size: 1.75rem;
            }}
            .page-hero-title {{
                font-size: 1.4rem !important;
            }}
            .metric-card {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
        }}
    </style>
    """, unsafe_allow_html=True)

def render_top_bar():
    """Render a persistent top bar for theme and language toggles on one row, aligned right."""
    theme = st.session_state.get("theme", "light")
    lang = st.session_state.get("lang", "vi").upper()

    # Use a single row of columns for more reliable alignment
    c_spacer, c_globe, c_lang, c_theme = st.columns(
        [0.82, 0.03, 0.1, 0.05],
        vertical_alignment="center",
        gap="small"
    )

    with c_globe:
        st.markdown("<span style='font-size:1.1rem; opacity:0.8;'>🌐</span>", unsafe_allow_html=True)

    with c_lang:
        new_lang = st.segmented_control(
            label="Language",
            options=["EN", "VI"],
            default=lang,
            key="lang_segmented",
            label_visibility="collapsed"
        )
        if new_lang and new_lang.lower() != st.session_state.lang:
            st.session_state.lang = new_lang.lower()
            st.rerun()

    with c_theme:
        mode_icon = "🌙" if theme == "light" else "☀️"
        if st.button(mode_icon, key="theme_toggle"):
            st.session_state.theme = "dark" if theme == "light" else "light"
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
        "schedule": '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 5h-2v6l5 3 .9-1.64-3.9-2.31V7z"/>',
        "upload": '<path d="M5 20h14v-2H5v2zm7-18-5.5 5.5 1.41 1.41L11 5.83V16h2V5.83l3.09 3.08 1.41-1.41L12 2z"/>',
        "device_thermostat": '<path d="M15 13V5c0-1.66-1.34-3-3-3S9 3.34 9 5v8c-1.21.91-2 2.37-2 4 0 2.76 2.24 5 5 5s5-2.24 5-5c0-1.63-.79-3.09-2-4zm-4-2V5c0-.55.45-1 1-1s1 .45 1 1v1h-1v1h1v2h-1v1h1v1h-2z"/>',
        "humidity_percentage": '<path d="M12 2c-5.33 4.55-8 8.48-8 11.8 0 4.98 3.8 8.2 8 8.2s8-3.22 8-8.2c0-3.32-2.67-7.25-8-11.8zm2.42 13.84c-.39.11-.79-.12-.9-.51-.11-.39.12-.79.51-.9.51-.14.8-.62.8-1.2 0-.66-.54-1.2-1.2-1.2s-1.2.54-1.2 1.2c0 .35-.14.67-.37.9-.23.23-.55.37-.9.37-.35 0-.67-.14-.9-.37-.23-.23-.37-.55-.37-.9 0-1.99 1.61-3.6 3.6-3.6s3.6 1.61 3.6 3.6c0 1.25-.66 2.32-1.77 2.61z"/>',
        "air": '<path d="M13.96 12.29l.75-2.12c.11-.3.40-.51.72-.51h3.57v2h-2.92l-.71 2h3.63v2h-4.28c-.32 0-.61-.21-.72-.51l-.75-2.12c-.22-.62-.22-1.24 0-1.86zm-5.92 0l.75-2.12c.11-.3.40-.51.72-.51h3.57v2H10.16l-.71 2h3.63v2H9.00a.75.75 0 01-.72-.51l-.75-2.12c-.22-.62-.22-1.24 0-1.86zm-7.04 0l.75-2.12c.11-.3.40-.51.72-.51h3.57v2H3.12l-.71 2h3.63v2H2a.75.75 0 01-.72-.51L.53 14.15c-.22-.62-.22-1.24 0-1.86z"/>'
    }

    icon_svg = f'<svg viewBox="0 0 24 24">{icons.get(icon, "")}</svg>' if icon in icons else ""

    st.markdown(f"""
        <div class="glass-card" style="min-height: 84px; display: flex; align-items: center; width: 100%;">
            <div class="metric-card" style="width: 100%;">
                <div class="metric-icon">{icon_svg}</div>
                <div>
                    <div class="metric-label" style="min-height: 36px; max-height: 36px; display: flex; align-items: center; overflow: hidden;">{label}</div>
                    <div class="metric-value">{value}</div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def render_city_metric(city_name, avg_aqi, hotspot_aqi, label_avg="Avg", label_hotspot="Hotspot"):
    """Render a compact city-specific AQI metric card with dual values."""
    theme = st.session_state.get("theme", "light")
    badge_bg = "rgba(239, 68, 68, 0.15)"
    badge_color = "#f87171" if theme == "dark" else "#ef4444"

    st.markdown(f"""
        <div class="glass-card city-card">
            <div class="city-name">{city_name}</div>
            <div class="city-sub-label">{label_avg}</div>
            <div class="city-main-val">{avg_aqi}</div>
            <div class="city-sub-row">
                <span style="background: {badge_bg}; color: {badge_color}; padding: 2px 6px; border-radius: 6px; font-weight: 600;">
                    ↑ {hotspot_aqi}
                </span>
                <span class="city-sub-label">{label_hotspot}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

def get_plotly_layout(height=400, animate=False):
    """Return a polished Plotly layout consistent with the glassmorphism theme."""
    # Delegate to the centralized chart_config for DRY compliance
    from lib.chart_config import get_plotly_layout as _get_layout
    return _get_layout(height=height, animate=animate)
