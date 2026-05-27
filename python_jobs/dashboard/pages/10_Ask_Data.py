"""
Ask Data page.
Provides a natural language Text-to-SQL search experience over analytical tables.
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
    client = TextToSqlClient()

    example_questions = [
        t("ask_data_example_status", lang),
        t("ask_data_example_compliance", lang),
        t("ask_data_example_traffic", lang),
    ]

    # ── Main input card ──────────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="glass-card" style="border-left: 5px solid #0891B2;">
        <h4 style="margin:0 0 0.5rem 0; font-family:'Outfit';">💬 Hỏi Trợ lý AI</h4>
        <p style="margin:0; font-size:0.9rem; opacity:0.85; line-height:1.4;">
            {t("ask_data_intro", lang)}
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.text_area(
        t("ask_data_question_label", lang),
        key="question",
        height=100,
        placeholder=t("ask_data_question_placeholder", lang),
        on_change=_mark_preview_stale,
    )

    st.caption("💡 " + (t("ask_data_examples_label", lang) if lang == "vi" else "Suggested Prompts:"))
    for example_question in example_questions:
        st.button(
            "💡 " + example_question,
            key=f"ask_data_example_{example_question}",
            use_container_width=True,
            on_click=_set_example_question,
            args=(example_question,),
        )

    st.markdown("<div style='margin-bottom:1.5rem;'></div>", unsafe_allow_html=True)

    if st.button(
        t("ask_data_preview_button", lang),
        disabled=_preview_button_disabled(),
        type="primary",
        use_container_width=True,
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

    render_section_divider()

    # ── SQL Preview section ──────────────────────────────────────────────────────
    preview = st.session_state.preview
    if preview:
        st.markdown(f"#### 💻 {t('ask_data_preview_card', lang)}")
        if st.session_state.preview_stale:
            st.warning(t("ask_data_preview_stale", lang))

        st.markdown(f"**{t('ask_data_explanation_label', lang)}** {preview.get('explanation', '')}")
        st.caption(t("ask_data_referenced_tables", lang))
        _render_badges(preview.get("referenced_tables", []))
        
        warnings = preview.get("warnings", [])
        if warnings:
            st.warning("\n".join(warnings))
        
        st.code(preview.get("sql", ""), language="sql")

        execute_disabled = _execute_button_disabled()
        if execute_disabled:
            st.caption(t("ask_data_execute_disabled", lang))

        if st.button(
            t("ask_data_execute_button", lang),
            key="ask_data_execute_action",
            disabled=execute_disabled,
            use_container_width=True,
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

    if st.session_state.text_to_sql_error:
        st.error(f"{t('ask_data_service_error', lang)} {st.session_state.text_to_sql_error}")

    render_section_divider()

    # ── Result section ───────────────────────────────────────────────────────────
    result = st.session_state.result
    if result:
        st.markdown(f"#### 📋 {t('ask_data_results_card', lang)}")
        meta = [
            f"{t('ask_data_rows_label', lang)}: {result.get('row_count', 0)}",
            f"{t('ask_data_runtime_label', lang)}: {result.get('execution_ms', 0)} ms",
            t("ask_data_truncated_label", lang) if result.get("truncated") else t("ask_data_full_label", lang),
        ]
        _render_badges(meta, accent="#0f172a")
        
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        if rows:
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Premium download option
            st.download_button(
                label="📥 Tải xuống CSV" if lang == "vi" else "📥 Download CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name="ask_data_results.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info(t("ask_data_empty_results", lang))
    else:
        st.info(f"**{t('ask_data_empty_title', lang)}**  \n{t('ask_data_empty_body', lang)}")

if __name__ == "__main__":
    main()
