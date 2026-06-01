"""Helper functions and design system components for Streamlit dashboard sub-pages."""
import streamlit as st
import functools
from lib.i18n import t

def init_page(page_key: str, default_title: str = "", icon: str = "📊") -> str:
    """Initialize a dashboard sub-page.

    Retrieves current language, and renders a premium page hero or standard title.

    Args:
        page_key: The translation key suffix, e.g., 'overview' will look up 'overview_title'
        default_title: Fallback title if translation key doesn't exist
        icon: Emoji/icon representing the page

    Returns:
        lang: Current active language ('vi' or 'en')
    """
    lang = st.session_state.get("lang", "vi")

    # Retrieve title from i18n
    title_text = t(f"{page_key}_title", lang)
    if not title_text or title_text == f"{page_key}_title":
        title_text = t(page_key, lang)
    if not title_text or title_text == page_key:
        title_text = default_title

    # Retrieve caption from i18n
    caption_text = t(f"{page_key}_caption", lang)
    if not caption_text or caption_text == f"{page_key}_caption":
        caption_text = t(f"{page_key}_intro", lang)
    if not caption_text or caption_text == f"{page_key}_intro":
        caption_text = t(f"{page_key}_description", lang)

    if caption_text in [f"{page_key}_caption", f"{page_key}_intro", f"{page_key}_description"]:
        caption_text = ""

    render_page_hero(title_text, caption_text, icon)

    return lang

def clean_html(html_str: str) -> str:
    """Helper to strip newlines and indentation from HTML strings.
    This prevents the Markdown parser from misinterpreting indented HTML lines as code blocks.
    """
    return " ".join(line.strip() for line in html_str.split("\n") if line.strip())

def render_unified_brand_header(page_key: str | None = None):
    """Render a persistent top bar for theme, standard and language toggles on one row, aligned right.
    This is shared across all dashboard pages to keep a cohesive, gorgeous premium layout.
    """
    theme = st.session_state.get("theme", "light")
    lang = st.session_state.get("lang", "vi")
    active_standard = st.session_state.get("standard", "VN_AQI")

    from lib.data_service import get_current_aqi_status
    
    # 3. Render Brand Header Bar (GreenAir VN | Live | VI/EN | WHO/VN)
    st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
    c_brand, c_actions = st.columns([0.45, 0.55], vertical_alignment="center")
    
    with c_brand:
        brand_html = """
        <div style='display: flex; align-items: center; gap: 8px;'>
            <span style='height: 10px; width: 10px; background-color: #10b981; border-radius: 50%; display: inline-block; box-shadow: 0 0 8px #10b981;'></span>
            <span style='font-family: "Outfit", sans-serif; font-size: 1.35rem; font-weight: 800; letter-spacing: -0.01em; background: linear-gradient(135deg, #10b981, #0891b2); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>Viet Nam Air Quality</span>
        </div>
        """
        st.markdown(clean_html(brand_html), unsafe_allow_html=True)
        
    with c_actions:
        if theme == "dark":
            btn_bg = "rgba(30, 41, 59, 0.45)"
            btn_border = "rgba(255, 255, 255, 0.08)"
            btn_color = "#f8fafc"
            btn_hover_bg = "rgba(8, 145, 178, 0.15)"
            live_bg = "rgba(16, 185, 129, 0.1)"
            live_border = "rgba(16, 185, 129, 0.4)"
        else:
            btn_bg = "#ffffff"
            btn_border = "rgba(226, 232, 240, 0.8)"
            btn_color = "#0f172a"
            btn_hover_bg = "rgba(8, 145, 178, 0.05)"
            live_bg = "rgba(16, 185, 129, 0.04)"
            live_border = "rgba(16, 185, 129, 0.25)"

        st.markdown(clean_html(f"""
        <style>
        /* Đảm bảo các cột hành động có độ rộng hoàn toàn bằng nhau */
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {{
            flex: 1 1 0% !important;
            min-width: 0 !important;
            width: 20% !important;
        }}
        
        /* Đảm bảo tất cả các nút và popover trong các cột hành động chiếm 100% độ rộng cột */
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] button,
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] div[data-testid="stPopover"],
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] div[data-testid="stPopover"] > button {{
            width: 100% !important;
            max-width: 100% !important;
            box-sizing: border-box !important;
        }}

        /* Áp dụng kiểu dáng đồng nhất cho tất cả các nút trong hàng actions */
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] button {{
            height: 36px !important;
            min-height: 36px !important;
            padding: 0px 4px !important;
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
            font-size: 0.82rem !important;
            font-weight: 700 !important;
            border-radius: 8px !important;
            text-align: center !important;
            white-space: nowrap !important;
            border: 1px solid {btn_border} !important;
            background-color: {btn_bg} !important;
            color: {btn_color} !important;
            transition: all 0.2s ease !important;
        }}
        
        /* Hiệu ứng hover cho các nút thường */
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] button:hover {{
            border-color: #0891b2 !important;
            color: #0891b2 !important;
            background-color: {btn_hover_bg} !important;
            transform: translateY(-1px) !important;
        }}

        /* Nút Live ở cột thứ nhất có style riêng biệt nổi bật */
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-of-type(1) button {{
            color: #10b981 !important;
            border-color: {live_border} !important;
            background-color: {live_bg} !important;
        }}
        div[data-testid="column"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-of-type(1) button:hover {{
            background-color: rgba(16, 185, 129, 0.12) !important;
            border-color: #10b981 !important;
        }}

        /* Căn phải cho segmented control của Nguồn */
        .source-selector-anchor ~ div[data-testid="stHorizontalBlock"] div[data-testid="stSegmentedControl"] {{
            display: flex !important;
            justify-content: flex-end !important;
            width: 100% !important;
        }}
        .source-selector-anchor ~ div[data-testid="stHorizontalBlock"] div[data-testid="stSegmentedControl"] > div {{
            display: flex !important;
            justify-content: flex-end !important;
            width: auto !important;
        }}
        </style>
        """), unsafe_allow_html=True)
        
        # Define columns based on active page
        if page_key == "ask_data":
            # For Ask Data, we only render Live and Language buttons to match mockup layout
            col_live, col_lang = st.columns([0.5, 0.5], gap="small", vertical_alignment="center")
        else:
            col_live, col_lang, col_std, col_theme, col_bell = st.columns([0.2, 0.2, 0.2, 0.2, 0.2], gap="small", vertical_alignment="center")
        
        with col_live:
            st.button("Live", key="live_btn_header_sh", use_container_width=True)
            
        with col_lang:
            lang_label = lang.upper()
            if st.button(lang_label, key="lang_toggle_header_sh", use_container_width=True):
                st.session_state.lang = "en" if lang == "vi" else "vi"
                st.rerun()
                
        if page_key != "ask_data":
            with col_std:
                if st.button(active_standard, key="std_toggle_header_sh", use_container_width=True):
                    st.session_state.standard = "WHO 2021" if active_standard == "VN_AQI" else "VN_AQI"
                    st.rerun()
                    
            with col_theme:
                theme_icon = "🌙" if theme == "light" else "☀️"
                if st.button(theme_icon, key="theme_toggle_header_sh", use_container_width=True):
                    st.session_state.theme = "dark" if theme == "light" else "light"
                    st.rerun()
                    
            with col_bell:
                with st.popover("🔔", use_container_width=True, key="bell_popover_header_sh"):
                    st.markdown("#### 🔔 Thông báo & Cảnh báo" if lang == "vi" else "#### 🔔 Notifications & Alerts")
                    try:
                        current_df = get_current_aqi_status()
                        if not current_df.empty:
                            alert_provinces = current_df[current_df["current_aqi"] > 150]
                            if not alert_provinces.empty:
                                st.warning(f"Có {len(alert_provinces)} tỉnh thành đang vượt ngưỡng AQI 150!" if lang == "vi" else f"There are {len(alert_provinces)} provinces exceeding AQI 150!")
                                for _, row in alert_provinces.iterrows():
                                    st.write(f"• **{row.province}**: AQI {int(row.current_aqi)} ({row.main_pollutant.upper()})")
                            else:
                                st.success("Không có cảnh báo ô nhiễm vượt ngưỡng." if lang == "vi" else "No air pollution alerts.")
                        else:
                            st.info("Hệ thống đang hoạt động bình thường." if lang == "vi" else "System is operating normally.")
                    except Exception:
                        st.info("Hệ thống đang hoạt động bình thường." if lang == "vi" else "System is operating normally.")

    # Render Ask Data second row (VN_AQI switcher + disclaimer warning) above the title
    if page_key == "ask_data":
        render_section_divider()
        col_std_btn, col_cap = st.columns([0.18, 0.82], vertical_alignment="center")
        with col_std_btn:
            if st.button(f"🔗 {active_standard} ˅", key="std_toggle_ask_data_header", use_container_width=True):
                st.session_state.standard = "WHO 2021" if active_standard == "VN_AQI" else "VN_AQI"
                st.rerun()
        with col_cap:
            st.markdown(f"<span style='opacity: 0.65; font-size: 0.88rem; margin-left: 0.5rem;'>· Analytics tables only · {t('ask_data_caption_warning', lang)}</span>", unsafe_allow_html=True)
        render_section_divider()

def render_page_hero(title: str, caption: str, icon: str = "📊"):
    """Render a styled premium page header with glassmorphism and subtle gradient."""
    caption_html = f"<p class='page-hero-subtitle'>{caption}</p>" if caption else ""
    hero_html = f'<div class="page-hero" style="border: none; background: transparent; box-shadow: none; padding: 0; margin: 0; margin-bottom: 0.5rem;"><div class="page-hero-content"><span class="page-hero-icon">{icon}</span><div><h1 class="page-hero-title">{title}</h1>{caption_html}</div></div></div>'
    st.markdown(hero_html, unsafe_allow_html=True)

def render_section_divider():
    """Render a premium subtle section divider instead of st.markdown('---')."""
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

def render_loading_skeleton(height: int = 200):
    """Render a placeholder glassmorphism skeleton during loading."""
    skeleton_html = f'<div class="loading-skeleton" style="height: {height}px;"><div class="skeleton-pulse"></div></div>'
    st.markdown(skeleton_html, unsafe_allow_html=True)

def render_info_banner(message: str, type: str = "info"):
    """Render a premium info/warning/success banner using custom styled container."""
    colors = {
        "info": ("rgba(8, 145, 178, 0.08)", "#0891b2", "ℹ️"),
        "warning": ("rgba(245, 158, 11, 0.08)", "#d97706", "⚠️"),
        "success": ("rgba(16, 185, 129, 0.08)", "#059669", "✅"),
        "error": ("rgba(239, 68, 68, 0.08)", "#dc2626", "🚨"),
    }
    bg, border_color, icon = colors.get(type, colors["info"])
    banner_html = f"""
    <div style="background: {bg}; border: 1px solid {border_color}40; border-left: 4px solid {border_color}; padding: 0.5rem 0.85rem; border-radius: 8px; margin-bottom: 0.75rem; display: flex; align-items: center; gap: 0.5rem; width: 100%;">
        <span style="font-size: 1.1rem; color: {border_color};">{icon}</span>
        <span style="font-size: 0.85rem; font-weight: 500; opacity: 0.95;">{message}</span>
    </div>
    """
    st.markdown(banner_html, unsafe_allow_html=True)

def page_wrapper(page_key: str, default_title: str = "", icon: str = "📊"):
    """Decorator to standardise page loading, error boundary, and translation context."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Inject general styling first
            from lib.style import inject_style
            inject_style()
            
            # Render persistent top bar brand
            render_unified_brand_header(page_key)
            
            lang = init_page(page_key, default_title, icon=icon)
            
            try:
                # Custom loading spinner text
                loading_msg = t("loading", lang)
                if loading_msg == "loading":
                    loading_msg = "Đang xử lý dữ liệu..." if lang == "vi" else "Loading analytics..."
                
                with st.spinner(loading_msg):
                    return func(lang, *args, **kwargs)
            except Exception as e:
                st.error(f"⚠️ **Đã xảy ra lỗi:** {e}" if lang == "vi" else f"⚠️ **An error occurred:** {e}")
                st.info(
                    "Vui lòng đảm bảo các mô hình dbt và database ClickHouse đang hoạt động chính xác."
                    if lang == "vi" else
                    "Please ensure clickhouse database and dbt models are running correctly."
                )
                st.exception(e)
        return wrapper
    return decorator

