import streamlit as st
from lib.i18n import t

# ── SVG ICON DATABASE ────────────────────────────────────────────────────────
SVG_ICONS = {
    "insights": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="m354-287 126-76 126 77-33-144 111-96-146-13-58-136-58 135-146 13 111 97-33 143ZM233-120l65-281L80-590l288-25 112-265 112 265 288 25-218 189 65 281-247-149-247 149Z"/></svg>',
    "biotech": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M160-160v-80h80v-40q0-83 58.5-141.5T440-480v-40h-80v-80h80v-40q0-83 58.5-141.5T640-840h160v80h-160q-50 0-85 35t-35 85v40h80v80h-80v40q0 83-58.5 141.5T440-240v40h80v80H160Z"/></svg>',
    "error": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-280q17 0 28.5-11.5T520-320q0-17-11.5-28.5T480-360q-17 0-28.5 11.5T440-320q0 17 11.5 28.5T480-280Zm-40-160h80v-240h-80v240Zm40 360q-83 0-156-31.5T197-197q-54-54-85.5-127T80-480q0-83 31.5-156T197-763q54-54 127-85.5T480-880q83 0 156 31.5T763-763q54 54 85.5 127T880-480q0 83-31.5 156T763-197q-54 54-127 85.5T480-80Z"/></svg>',
    "location_on": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-480q33 0 56.5-23.5T560-560q0-33-23.5-56.5T480-640q-33 0-56.5 23.5T400-560q0 33 23.5 56.5T480-480Zm0 400Q306-224 223-345.5T140-571q0-150 100.5-249.5T480-920q139 0 239.5 99.5T820-571q0 125-83 246.5T480-80Z"/></svg>',
    "traffic": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M440-80v-167q-85-15-142.5-73.5T240-460v-300q0-25 17.5-42.5T300-820h140v-60h80v60h140q25 0 42.5 17.5T720-760v300q0 81-57.5 139.5T520-247v167h-80ZM480-500q25 0 42.5-17.5T540-560q0-25-17.5-42.5T480-620q-25 0-42.5 17.5T420-560q0 25 17.5 42.5T480-500Z"/></svg>',
    "health": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M440-280h80v-160h160v-80H520v-160h-80v160H280v80h160v160ZM480-80q-83 0-156-31.5T197-197q-54-54-85.5-127T80-480q0-83 31.5-156T197-763q54-54 127-85.5T480-880q83 0 156 31.5T763-763q54 54 85.5 127T880-480q0 83-31.5 156T763-197q-54 54-127 85.5T480-80Z"/></svg>',
    "status": '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="m354-287 126-76 126 77-33-144 111-96-146-13-58-136-58 135-146 13 111 97-33 143ZM233-120l65-281L80-590l288-25 112-265 112 265 288 25-218 189 65 281-247-149-247 149Z"/></svg>'
}

def inject_style():
    """Inject custom CSS for premium look and feel."""
    theme = st.session_state.get("theme", "light")
    
    if theme == "dark":
        bg_color = "#0E1117"
        secondary_bg = "#1A1C24"
        text_color = "#E0E0E0"
        card_bg = "rgba(255, 255, 255, 0.05)"
        border_color = "rgba(255, 255, 255, 0.1)"
        shadow = "0 8px 32px 0 rgba(0, 0, 0, 0.37)"
    else:
        bg_color = "#F0F2F6"
        secondary_bg = "#FFFFFF"
        text_color = "#262730"
        card_bg = "rgba(255, 255, 255, 0.7)"
        border_color = "rgba(255, 255, 255, 0.3)"
        shadow = "0 8px 32px 0 rgba(31, 38, 135, 0.15)"

    css = f"""
    <style>
        /* Hide Streamlit Header & Footer */
        header[data-testid="stHeader"] {{
            visibility: hidden;
            height: 0;
        }}
        footer {{
            visibility: hidden;
        }}
        
        /* Base Theme */
        .stApp {{
            background-color: {bg_color};
            color: {text_color};
            padding-top: 0 !important;
        }}

        /* Adjust Main Container Padding */
        .block-container {{
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }}
        
        /* Glassmorphism Card */
        .glass-card {{
            background: {card_bg};
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            border-radius: 12px;
            border: 1px solid {border_color};
            box-shadow: {shadow};
            padding: 18px;
            margin-bottom: 20px;
        }}
        
        /* Sidebar Styling Refinement */
        [data-testid="stSidebar"] {{
            background-color: {secondary_bg} !important;
            border-right: 1px solid {border_color};
        }}
        
        [data-testid="stSidebar"] * {{
            color: {text_color} !important;
        }}
        
        [data-testid="stSidebarNav"] span {{
            color: {text_color} !important;
        }}

        /* Custom Font - Montserrat */
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap');
        
        html, body, [class*="css"], .stText, .stMarkdown {{
            font-family: 'Montserrat', sans-serif !important;
        }}

        /* Plotly Modebar Hidden */
        .js-plotly-plot .plotly .modebar {{
            display: none !important;
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def render_top_bar():
    """Render a persistent top bar for theme and language toggles."""
    lang = st.session_state.get("lang", "vi")
    
    # Using columns for native top alignment
    tc1, tc2, tc3 = st.columns([0.84, 0.08, 0.08])
    
    with tc2:
        current_theme = st.session_state.get("theme", "light")
        new_theme = "dark" if current_theme == "light" else "light"
        theme_icon = "🌙" if current_theme == "light" else "☀️"
        st.button(f"{theme_icon}", key="theme_toggle_top", 
                  on_click=lambda: st.session_state.update({"theme": new_theme}),
                  help=t("theme_light" if new_theme=="light" else "theme_dark", lang))

    with tc3:
        new_lang = "en" if lang == "vi" else "vi"
        btn_label = "EN" if lang == "vi" else "VI"
        st.button(btn_label, key="lang_toggle_top", 
                  on_click=lambda: st.session_state.update({"lang": new_lang}),
                  help=t("lang_en" if new_lang=="en" else "lang_vi", lang))

def render_metric_card(label, value, delta=None, delta_color="normal", icon=None):
    """Render a custom premium metric card with SVG icons."""
    theme = st.session_state.get("theme", "light")
    text_color = "#E0E0E0" if theme == "dark" else "#262730"
    
    delta_html = ""
    if delta:
        color = "#09ab3b" if delta.startswith("+") else "#ff4b4b"
        delta_html = f'<div style="font-size: 0.8rem; font-weight: 600; color: {color};">{delta}</div>'

    icon_svg = SVG_ICONS.get(icon, "")
    icon_html = f'<div style="font-size: 2rem; margin-right: 15px; opacity: 0.7; color: {text_color}; display: flex; align-items: center;">{icon_svg}</div>' if icon_svg else ""
    
    card_html = f"""<div class="glass-card"><div style="display: flex; align-items: center;">{icon_html}<div><div class="metric-label">{label}</div><div class="metric-value font-semibold" style="color: {text_color};">{value}</div>{delta_html}</div></div></div>"""
    st.markdown(card_html, unsafe_allow_html=True)

def get_plotly_layout(height=400):
    theme = st.session_state.get("theme", "light")
    is_dark = theme == "dark"
    text_color = "#E0E0E0" if is_dark else "#262730"
    grid_color = "rgba(255,255,255,0.1)" if is_dark else "rgba(0,0,0,0.05)"
    
    return dict(
        font=dict(family="Montserrat, sans-serif", size=12, color=text_color),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=20),
        height=height,
        hovermode="x unified",
        xaxis=dict(showgrid=True, gridcolor=grid_color, linecolor=grid_color, tickfont=dict(color=text_color)),
        yaxis=dict(showgrid=True, gridcolor=grid_color, linecolor=grid_color, tickfont=dict(color=text_color)),
        legend=dict(font=dict(color=text_color), bgcolor="rgba(0,0,0,0)")
    )
