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

def render_top_bar_inline():
    """Render compact theme and language toggles inline next to page title."""
    theme = st.session_state.get("theme", "light")
    lang = st.session_state.get("lang", "vi").upper()

    # Columns optimized for the right-side action panel
    c_globe, c_lang, c_theme = st.columns(
        [0.15, 0.55, 0.3],
        vertical_alignment="center",
        gap="small"
    )

    with c_globe:
        st.markdown("<div style='text-align: right;'><span style='font-size:1.1rem; opacity:0.8;'>🌐</span></div>", unsafe_allow_html=True)

    with c_lang:
        new_lang = st.segmented_control(
            label="Language",
            options=["EN", "VI"],
            default=lang,
            key="lang_segmented_inline",
            label_visibility="collapsed"
        )
        if new_lang and new_lang.lower() != st.session_state.lang:
            st.session_state.lang = new_lang.lower()
            st.rerun()

    with c_theme:
        mode_icon = "🌙" if theme == "light" else "☀️"
        if st.button(mode_icon, key="theme_toggle_inline", use_container_width=True):
            st.session_state.theme = "dark" if theme == "light" else "light"
            st.rerun()

def render_page_hero(title: str, caption: str, icon: str = "📊"):
    """Render a styled premium page header with glassmorphism and subtle gradient."""
    # Split header into title (72%) and action controls (28%) on a single horizontal row
    col_title, col_actions = st.columns([0.72, 0.28], vertical_alignment="center")
    
    with col_title:
        caption_html = f"<p class='page-hero-subtitle'>{caption}</p>" if caption else ""
        hero_html = f'<div class="page-hero" style="border: none; background: transparent; box-shadow: none; padding: 0; margin: 0;"><div class="page-hero-content"><span class="page-hero-icon">{icon}</span><div><h1 class="page-hero-title">{title}</h1>{caption_html}</div></div></div>'
        st.markdown(hero_html, unsafe_allow_html=True)
        
    with col_actions:
        render_top_bar_inline()

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
