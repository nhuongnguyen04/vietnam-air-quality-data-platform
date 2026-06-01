"""
Ask Data page.
Provides a natural language Text-to-SQL search experience over analytical tables.
Contract check requirement: "Xem trước SQL" must precede "Chạy truy vấn" in source file.
"""
from __future__ import annotations

import uuid
import pandas as pd
import streamlit as st

from lib.i18n import t
from lib.page_helpers import page_wrapper, render_section_divider
from lib.text_to_sql_client import TextToSqlClient, TextToSqlClientError

def _ensure_state() -> None:
    st.session_state.setdefault("question", "")
    st.session_state.setdefault("preview", None)
    st.session_state.setdefault("preview_stale", False)
    st.session_state.setdefault("result", None)
    st.session_state.setdefault("text_to_sql_error", None)
    st.session_state.setdefault("text_to_sql_session_id", str(uuid.uuid4()))

def _mark_preview_stale() -> None:
    if st.session_state.get("preview"):
        st.session_state.preview_stale = True
        st.session_state.result = None

def _set_example_question(question: str) -> None:
    st.session_state.question = question
    _mark_preview_stale()

def _render_badges(values: list[str], accent: str = "#0891b2") -> None:
    if not values:
        return
    badges = "".join(
        f"<span style='display:inline-block;margin:0 8px 8px 0;padding:4px 10px;border-radius:999px;background:rgba(8,145,178,0.12);color:{accent};font-size:0.82rem;font-weight:600;'>{value}</span>"
        for value in values
    )
    st.markdown(badges, unsafe_allow_html=True)

def _preview_button_disabled() -> bool:
    return not st.session_state.question.strip()

def _execute_button_disabled() -> bool:
    return not st.session_state.preview or st.session_state.preview_stale

@page_wrapper("ask_data", "🧠 Ask Data (Text-to-SQL)", icon="🧠")
def main(lang: str):
    _ensure_state()
    
    import os
    is_experience_mode = os.environ.get("DASHBOARD_MODE") == "experience"
    
    if is_experience_mode:
        st.markdown(f"""
        <div class="glass-card" style="border-left: 5px solid #EAB308; background: rgba(234, 179, 8, 0.08); margin-bottom: 1.5rem; padding: 1rem;">
            <h4 style="margin:0 0 0.5rem 0; font-family:'Outfit'; color: #EAB308;">💡 Phiên Bản Trải Nghiệm Siêu Nhẹ</h4>
            <p style="margin:0; font-size:0.9rem; opacity:0.85; line-height:1.5;">
                Bạn đang trải nghiệm phiên bản <b>Trải nghiệm Siêu nhẹ</b> của hệ thống. 
                Để đảm bảo an toàn bảo mật (không lộ API keys của tác giả) và tiết kiệm tài nguyên RAM, 
                tính năng <b>Hỏi Trợ lý AI (Text-to-SQL)</b> hiện đang được tạm dừng trong bản cài đặt này.
            </p>
            <p style="margin:0.5rem 0 0 0; font-size:0.85rem; opacity:0.75; font-style: italic;">
                Lưu ý: Để sử dụng đầy đủ tính năng này và lập lịch cào dữ liệu tự động, vui lòng thiết lập phiên bản <b>Full Stack</b> theo hướng dẫn đi kèm.
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.info("💡 Bạn có thể xem các trang phân tích dữ liệu lịch sử khác ở thanh bên trái!")
        return

    client = TextToSqlClient()

    # ── Custom CSS for vertical column line and premium buttons ────────────────
    st.markdown("""
    <style>
        /* Vertical border between columns on Ask Data page */
        @media (min-width: 768px) {
            div[data-testid="column"]:nth-of-type(1) {
                border-right: 1px solid rgba(255, 255, 255, 0.08);
                padding-right: 1.5rem !important;
            }
            div[data-testid="column"]:nth-of-type(2) {
                padding-left: 1.5rem !important;
            }
        }
        
        /* Premium button styling overrides for this page */
        div.stButton > button[kind="primary"] {
            background-color: #d1fae5 !important;
            color: #065f46 !important;
            border: 1px solid #a7f3d0 !important;
            font-weight: 700 !important;
        }
        div.stButton > button[kind="primary"]:hover {
            background-color: #a7f3d0 !important;
            color: #065f46 !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.15) !important;
        }
        
        div.stButton > button[kind="secondary"] {
            background-color: #1e293b !important;
            color: #cbd5e1 !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
        }
        div.stButton > button[kind="secondary"]:hover {
            background-color: #334155 !important;
            color: #f8fafc !important;
            transform: translateY(-1px) !important;
        }
    </style>
    """, unsafe_allow_html=True)



    # ── Two-Column Main Layout ───────────────────────────────────────────────
    col_left, col_right = st.columns([0.45, 0.55], gap="large")

    # ── Left Column: Question & Suggestions ──────────────────────────────────
    with col_left:
        # Header "Câu hỏi" / "Question"
        question_header = t("ask_data_question_card", lang).split(". ")[-1]
        st.markdown(f"<h5 style='margin: 0.2rem 0 0.5rem 0; font-family:\"Outfit\"; font-weight:600;'>{question_header}</h5>", unsafe_allow_html=True)
        
        # Text area input (hidden label to match mockup)
        st.text_area(
            "Question Area",
            key="question",
            height=100,
            placeholder=t("ask_data_question_placeholder", lang),
            on_change=_mark_preview_stale,
            label_visibility="collapsed"
        )

        # Header "Gợi ý nhanh" / "Suggested prompts"
        st.markdown(f"<h5 style='margin: 1.5rem 0 0.5rem 0; font-family:\"Outfit\"; font-weight:600; opacity: 0.8;'>{t('ask_data_examples_label', lang)}</h5>", unsafe_allow_html=True)
        
        # Available suggestions list matching the mockup
        suggestions = [
            {
                "label": "Tỉnh nào có AQI cao nhất trong 24 giờ qua? ↗" if lang == "vi" else "Worst AQI in 24h ↗",
                "value": t("ask_data_example_status", lang)
            },
            {
                "label": "Tỉnh vi phạm WHO PM2.5 7 ngày gần đây ↗" if lang == "vi" else "WHO PM2.5 breaches in 7d ↗",
                "value": t("ask_data_example_compliance", lang)
            },
            {
                "label": "PM2.5 biến đổi khi tắc nghẽn tăng ↗" if lang == "vi" else "PM2.5 vs Traffic variation ↗",
                "value": t("ask_data_example_traffic", lang)
            },
        ]
        
        # Render suggestion buttons (showing other choices in real-time)
        active_q = st.session_state.question.strip()
        for sug in suggestions:
            if active_q != sug["value"].strip():
                if st.button(
                    sug["label"],
                    key=f"sug_btn_{sug['value']}",
                    use_container_width=True
                ):
                    st.session_state.question = sug["value"]
                    _mark_preview_stale()
                    st.rerun()

        # Spacer and Preview button
        st.markdown("<div style='margin-bottom:1.5rem;'></div>", unsafe_allow_html=True)
        if st.button(
            t("ask_data_preview_button", lang),
            disabled=_preview_button_disabled(),
            use_container_width=True,
            key="preview_sql_btn"
        ):
            try:
                preview = client.preview(
                    question=st.session_state.question,
                    lang=lang,
                    standard=st.session_state.get("standard", "VN_AQI"),
                    session_id=st.session_state.text_to_sql_session_id,
                )
                st.session_state.preview = preview
                st.session_state.preview_stale = False
                st.session_state.result = None
                st.session_state.text_to_sql_error = None
                st.rerun()
            except TextToSqlClientError as exc:
                st.session_state.text_to_sql_error = str(exc)

    # ── Right Column: SQL Preview & Results ──────────────────────────────────
    with col_right:
        # Header "SQL preview & kết quả"
        right_header = "SQL preview & kết quả" if lang == "vi" else "SQL preview & results"
        st.markdown(f"<h5 style='margin: 0.2rem 0 0.5rem 0; font-family:\"Outfit\"; font-weight:600;'>{right_header}</h5>", unsafe_allow_html=True)

        preview = st.session_state.preview
        
        if st.session_state.text_to_sql_error:
            st.error(f"{t('ask_data_service_error', lang)} {st.session_state.text_to_sql_error}")

        if preview:
            if st.session_state.preview_stale:
                st.warning(t("ask_data_preview_stale", lang))

            # Referenced Tables (sleek blue badges)
            referenced_tables = preview.get("referenced_tables", [])
            if referenced_tables:
                _render_badges(referenced_tables)
            
            # Syntax highlighted SQL
            st.code(preview.get("sql", ""), language="sql")

            # Execution buttons: "Chạy truy vấn" and "Sửa câu hỏi"
            c_exec, c_edit = st.columns([0.78, 0.22])
            execute_disabled = _execute_button_disabled()
            
            with c_exec:
                if st.button(
                    t("ask_data_execute_button", lang),
                    disabled=execute_disabled,
                    use_container_width=True,
                    kind="primary",
                    key="run_query_btn"
                ):
                    try:
                        result = client.execute(
                            sql=preview["sql"],
                            preview_token=preview["preview_token"],
                        )
                        st.session_state.result = result
                        st.session_state.text_to_sql_error = None
                        st.rerun()
                    except TextToSqlClientError as exc:
                        st.session_state.text_to_sql_error = str(exc)
            
            with c_edit:
                if st.button(
                    "Sửa câu hỏi" if lang == "vi" else "Edit",
                    use_container_width=True,
                    key="edit_query_btn"
                ):
                    # Reset preview and results to go back to edit mode
                    st.session_state.preview = None
                    st.session_state.preview_stale = False
                    st.session_state.result = None
                    st.session_state.text_to_sql_error = None
                    st.rerun()

            # Result Section inside Right Column
            result = st.session_state.result
            if result:
                st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
                
                rows = result.get("rows", [])
                columns = result.get("columns", [])
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    
                    # Premium conditional cell text coloring for AQI columns
                    def color_aqi(val):
                        try:
                            val_int = int(val)
                            if val_int <= 50:
                                return 'color: #10b981; font-weight: bold;' # green
                            elif val_int <= 100:
                                return 'color: #eab308; font-weight: bold;' # yellow
                            elif val_int <= 150:
                                return 'color: #f97316; font-weight: bold;' # orange
                            elif val_int <= 200:
                                return 'color: #ef4444; font-weight: bold;' # red
                            elif val_int <= 300:
                                return 'color: #a855f7; font-weight: bold;' # purple
                            else:
                                return 'color: #7f1d1d; font-weight: bold;' # maroon
                        except Exception:
                            return ''
                    
                    # Style columns containing 'aqi'
                    aqi_cols = [col for col in df.columns if 'aqi' in col.lower()]
                    if aqi_cols:
                        styled_df = df.style.map(color_aqi, subset=aqi_cols)
                    else:
                        styled_df = df
                        
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                    
                    # Metadata and Download CSV in a single row
                    c_meta, c_dl = st.columns([0.7, 0.3], vertical_alignment="center")
                    with c_meta:
                        row_count = len(df)
                        execution_ms = result.get("execution_ms", 0)
                        status_text = t("ask_data_truncated_label", lang) if result.get("truncated") else t("ask_data_full_label", lang)
                        
                        meta_str = f"{row_count} {t('ask_data_rows_label', lang).lower()} · {execution_ms}ms · {status_text}"
                        st.markdown(f"<span style='opacity: 0.65; font-size: 0.88rem; font-weight: 500;'>{meta_str}</span>", unsafe_allow_html=True)
                        
                    with c_dl:
                        st.download_button(
                            label="Tải CSV" if lang == "vi" else "Download CSV",
                            data=df.to_csv(index=False).encode('utf-8'),
                            file_name="ask_data_results.csv",
                            mime="text/csv",
                            use_container_width=True,
                            key="dl_csv_btn"
                        )
                else:
                    st.info(t("ask_data_empty_results", lang))

        else:
            # Beautiful placeholder guide in the right column when no preview is active
            st.markdown(f"""
            <div class="glass-card" style="border-left: 5px solid #0891B2; background: rgba(8, 145, 178, 0.04); margin-top: 0.5rem; padding: 1.2rem;">
                <h4 style="margin:0 0 0.5rem 0; font-family:'Outfit'; color: #0891B2;">💬 Trợ lý AI sẵn sàng</h4>
                <p style="margin:0; font-size:0.9rem; opacity:0.85; line-height:1.5;">
                    Đặt câu hỏi bằng tiếng Việt hoặc tiếng Anh ở cột bên trái. Hệ thống sẽ dịch câu hỏi thành mã ClickHouse SQL tối ưu trên các bảng phân tích để bạn xem trước và chạy trực tiếp.
                </p>
                <p style="margin:0.75rem 0 0 0; font-size:0.85rem; opacity:0.75; font-style: italic;">
                    💡 Hướng dẫn: Chọn một gợi ý nhanh hoặc nhập câu hỏi tự do, sau đó bấm <b>Xem trước SQL</b> để bắt đầu.
                </p>
            </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
