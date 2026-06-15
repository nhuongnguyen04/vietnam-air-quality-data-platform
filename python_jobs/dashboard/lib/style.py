import streamlit as st

def inject_style():
    """Inject custom CSS for premium UI look and feel."""
    theme = st.session_state.get("theme", "light")

    if theme == "dark":
        bg_color = "#020617"             # slate-950
        text_color = "#cbd5e1"           # slate-300
        card_bg = "rgba(15, 23, 42, 0.7)"  # slate-900 solid-like glass per design proposal
        border_color = "rgba(255, 255, 255, 0.08)"
        sidebar_bg = "#0F172A"           # slate-900 matching design system dark mode sidebar
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -2px rgba(0, 0, 0, 0.2)"
        glass_blur = "blur(12px)"
        accent_color = "#0891b2"         # cyan-600
        accent_glow = "rgba(8, 145, 178, 0.25)"
        divider_color = "rgba(255, 255, 255, 0.1)"
        
        # Sidebar specific variables
        nav_item_hover_bg = "rgba(255, 255, 255, 0.04)"
        nav_item_active_bg = "rgba(8, 145, 178, 0.08)" # Changed to cyan transparent highlight
        nav_text_color = "rgba(255, 255, 255, 0.7)"
        nav_text_active_color = "#ffffff"
        nav_icon_color = "rgba(255, 255, 255, 0.6)"
        nav_icon_active_color = "#ffffff"
        nav_header_color = "rgba(255, 255, 255, 0.4)"
        badge_bg = "#ffffff"
        badge_text = "#ef4444"
    else:
        bg_color = "#FAFBFC"             # light background per design proposal
        text_color = "#0f172a"           # slate-900
        card_bg = "#FFFFFF"              # solid white per design proposal
        border_color = "#E2E8F0"         # border per design proposal
        sidebar_bg = "#ffffff"
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.05)"
        glass_blur = "blur(8px)"
        accent_color = "#0891b2"
        accent_glow = "rgba(8, 145, 178, 0.15)"
        divider_color = "rgba(15, 23, 42, 0.08)"
        
        # Sidebar specific variables
        nav_item_hover_bg = "rgba(15, 23, 42, 0.03)"
        nav_item_active_bg = "rgba(8, 145, 178, 0.08)" # Changed to cyan transparent highlight
        nav_text_color = "rgba(15, 23, 42, 0.7)"
        nav_text_active_color = "#0f172a"
        nav_icon_color = "rgba(15, 23, 42, 0.6)"
        nav_icon_active_color = "#0f172a"
        nav_header_color = "rgba(15, 23, 42, 0.45)"
        badge_bg = "#fef2f2"
        badge_text = "#dc2626"

    st.markdown(f"""
    <style>
        /* ── Base Styles & Typography ─────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800;900&family=Plus+Jakarta+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0');

        html, body, [data-testid="stAppViewContainer"], .stMarkdown {{
            font-family: 'Plus Jakarta Sans', sans-serif !important;
            background-color: {bg_color};
            color: {text_color};
            transition: background-color 0.3s ease, color 0.3s ease;
        }}

        code, kbd, pre, samp, [data-testid="stCodeBlock"] code, .jetbrains-mono {{
            font-family: 'JetBrains Mono', monospace !important;
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

        /* Make header transparent and let clicks pass through, but keep collapse button clickable */
        header[data-testid="stHeader"] {{
            background-color: transparent !important;
            background: transparent !important;
            border-bottom: none !important;
            z-index: 999 !important;
            pointer-events: none !important;
        }}
        
        [data-testid="collapsedControl"] {{
            pointer-events: auto !important;
            visibility: visible !important;
        }}
        
        /* Hide Deploy and MainMenu default buttons to keep clean layout */
        header[data-testid="stHeader"] [data-testid="stDeployButton"],
        header[data-testid="stHeader"] [data-testid="stMainMenu"] {{
            display: none !important;
        }}
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
        [data-testid="stSidebarContent"] {{
            padding-top: 0.5rem !important;
            padding-bottom: 3.5rem !important;
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
            margin-top: 0.5rem;
        }}
        .sidebar-filters-container {{
            order: 3;
        }}
        
        /* Navigation links styling */
        a[data-testid="stSidebarNavLink"] {{
            padding: 5px 12px !important;
            margin: 1px 0 !important;
            border-radius: 4px !important;
            transition: all 0.2s ease !important;
            display: flex !important;
            align-items: center !important;
            text-decoration: none !important;
            background-color: transparent !important;
        }}
        
        a[data-testid="stSidebarNavLink"]:hover {{
            background-color: {nav_item_hover_bg} !important;
        }}
        
        a[data-testid="stSidebarNavLink"][aria-current="page"] {{
            background-color: {nav_item_active_bg} !important;
            border-right: 3px solid {accent_color} !important; /* Cyan active indicator per design proposal */
            border-radius: 4px 0 0 4px !important;
        }}
        
        /* Normal text color for links */
        a[data-testid="stSidebarNavLink"] div[data-testid="stMarkdownContainer"] p {{
            color: {nav_text_color} !important;
            font-family: 'Inter', sans-serif !important;
            font-weight: 500 !important;
            font-size: 0.86rem !important;
            margin: 0 !important;
        }}
        
        /* Active text color for link */
        a[data-testid="stSidebarNavLink"][aria-current="page"] div[data-testid="stMarkdownContainer"] p {{
            color: {nav_text_active_color} !important;
            font-weight: 700 !important;
        }}
        
        /* Normal icon styling */
        a[data-testid="stSidebarNavLink"] [data-testid="stIconMaterial"] {{
            color: {nav_icon_color} !important;
            font-size: 1.1rem !important;
            margin-right: 8px !important;
        }}
        
        /* Active icon styling */
        a[data-testid="stSidebarNavLink"][aria-current="page"] [data-testid="stIconMaterial"] {{
            color: {nav_icon_active_color} !important;
        }}
        
        /* Section headers in sidebar navigation */
        header[data-testid="stNavSectionHeader"] {{
            padding-top: 0.6rem !important;
            padding-bottom: 0.2rem !important;
            margin-left: 12px !important;
        }}
        
        /* Header text styling */
        header[data-testid="stNavSectionHeader"] span[class*="e1lpckdq8"] p {{
            font-family: 'Outfit', sans-serif !important;
            font-size: 0.72rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
            color: {nav_header_color} !important;
            font-weight: 700 !important;
            margin: 0 !important;
        }}
        
        /* Hide the native expand/collapse icons in the headers */
        header[data-testid="stNavSectionHeader"] div[class*="e1lpckdq7"] {{
            display: none !important;
        }}
        
        /* Badge for Cảnh báo (Alerts) next to the text */
        span[label="Cảnh báo"] div[data-testid="stMarkdownContainer"] p::after,
        span[label="Alerts"] div[data-testid="stMarkdownContainer"] p::after {{
            content: "3" !important;
            background-color: {badge_bg} !important;
            color: {badge_text} !important;
            font-size: 0.75rem !important;
            font-weight: 700 !important;
            padding: 1px 6px !important;
            border-radius: 4px !important;
            margin-left: 12px !important;
            display: inline-block !important;
            border: 1px solid rgba(239, 68, 68, 0.15) !important;
            line-height: 1.2 !important;
            vertical-align: middle !important;
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
        /* Note: Removed global align-items: center; to respect native Streamlit vertical_alignment settings and allow standard top alignment */


        /* ── Glass Card & Charts ─────────────────────────── */
        .glass-card {{
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
            transition: transform 0.2s ease, border-color 0.2s ease;
        }}

        .stPlotlyChart {{
            background: {card_bg};
            backdrop-filter: {glass_blur};
            -webkit-backdrop-filter: {glass_blur};
            border: 1px solid {border_color};
            border-radius: 12px;
            padding: 6px;
            margin-bottom: 0.65rem;
            box-shadow: {shadow};
            animation: fadeIn 0.45s cubic-bezier(0.16, 1, 0.3, 1) forwards;
            overflow: hidden;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }}

        .glass-card:hover, .stPlotlyChart:hover {{
            transform: translateY(-2px);
            border-color: {accent_color} !important;
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
            background-color: rgba(148, 163, 184, 0.12);
            animation: pulse 1.5s infinite ease-in-out;
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
            0% {{ opacity: 0.4; }}
            50% {{ opacity: 0.8; }}
            100% {{ opacity: 0.4; }}
        }}

        @keyframes growProgressBar {{
            from {{ width: 0%; }}
        }}

        .progress-bar-fill {{
            animation: growProgressBar 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards;
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
