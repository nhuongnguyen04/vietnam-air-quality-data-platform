"""Ask Data page for preview-first natural language analytics."""
from __future__ import annotations

import uuid

import pandas as pd
import streamlit as st

from lib.i18n import t
from lib.text_to_sql_client import TextToSqlClient, TextToSqlClientError


lang = st.session_state.get("lang", "vi")
PREVIEW_LABEL_VI = "Xem trước SQL"
EXECUTE_LABEL_VI = "Chạy truy vấn"


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
        f"<span style='display:inline-block;margin:0 8px 8px 0;padding:4px 10px;border-radius:999px;background:rgba(8,145,178,0.12);color:{accent};font-size:0.85rem;font-weight:600;'>{value}</span>"
        for value in values
    )
    st.markdown(badges, unsafe_allow_html=True)


def _preview_button_disabled() -> bool:
    return not st.session_state.question.strip()


def _execute_button_disabled() -> bool:
    return not st.session_state.preview or st.session_state.preview_stale


_ensure_state()
client = TextToSqlClient()

st.title(f"🧠 {t('ask_data_title', lang)}")
st.caption(t("ask_data_intro", lang))

example_questions = [
    t("ask_data_example_status", lang),
    t("ask_data_example_compliance", lang),
    t("ask_data_example_traffic", lang),
]

with st.container():
    st.subheader(t("ask_data_question_card", lang))
    st.text_area(
        t("ask_data_question_label", lang),
        key="question",
        height=100,
        placeholder=t("ask_data_question_placeholder", lang),
        on_change=_mark_preview_stale,
    )
    st.caption(t("ask_data_examples_label", lang))
    example_columns = st.columns(3)
    for column, example_question in zip(example_columns, example_questions):
        with column:
            st.button(
                example_question,
                key=f"ask_data_example_{example_question}",
                use_container_width=True,
                on_click=_set_example_question,
                args=(example_question,),
            )

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
                standard=st.session_state.get("standard", "TCVN"),
                session_id=st.session_state.text_to_sql_session_id,
            )
            st.session_state.preview = preview
            st.session_state.preview_stale = False
            st.session_state.result = None
            st.session_state.text_to_sql_error = None
        except TextToSqlClientError as exc:
            st.session_state.text_to_sql_error = str(exc)


preview = st.session_state.preview
if preview:
    st.subheader(t("ask_data_preview_card", lang))
    if st.session_state.preview_stale:
        st.warning(t("ask_data_preview_stale", lang))

    st.write(f"**{t('ask_data_explanation_label', lang)}** {preview.get('explanation', '')}")
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
        except TextToSqlClientError as exc:
            st.session_state.text_to_sql_error = str(exc)


if st.session_state.text_to_sql_error:
    st.error(f"{t('ask_data_service_error', lang)} {st.session_state.text_to_sql_error}")


result = st.session_state.result
if result:
    st.subheader(t("ask_data_results_card", lang))
    meta = [
        f"{t('ask_data_rows_label', lang)}: {result.get('row_count', 0)}",
        f"{t('ask_data_runtime_label', lang)}: {result.get('execution_ms', 0)} ms",
        t("ask_data_truncated_label", lang)
        if result.get("truncated")
        else t("ask_data_full_label", lang),
    ]
    _render_badges(meta, accent="#0f172a")
    rows = result.get("rows", [])
    columns = result.get("columns", [])
    if rows:
        st.dataframe(
            pd.DataFrame(rows, columns=columns),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(t("ask_data_empty_results", lang))
else:
    st.info(f"**{t('ask_data_empty_title', lang)}**  \n{t('ask_data_empty_body', lang)}")
