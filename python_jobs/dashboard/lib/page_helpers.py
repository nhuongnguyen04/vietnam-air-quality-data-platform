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

def render_page_hero(title: str, caption: str, icon: str = "📊"):
    """Render a styled premium page header with glassmorphism and subtle gradient."""
    caption_html = f"<p class='page-hero-subtitle'>{caption}</p>" if caption else ""
    hero_html = f'<div class="page-hero"><div class="page-hero-content"><span class="page-hero-icon">{icon}</span><div><h1 class="page-hero-title">{title}</h1>{caption_html}</div></div></div>'
    st.markdown(hero_html, unsafe_allow_html=True)

def render_section_divider():
    """Render a premium subtle section divider instead of st.markdown('---')."""
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

def render_loading_skeleton(height: int = 200):
    """Render a placeholder glassmorphism skeleton during loading."""
    skeleton_html = f'<div class="loading-skeleton" style="height: {height}px;"><div class="skeleton-pulse"></div></div>'
    st.markdown(skeleton_html, unsafe_allow_html=True)

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
