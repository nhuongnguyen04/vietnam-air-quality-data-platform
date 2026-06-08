"""Orchestrator for the 2-Call search-verified LLM pipeline."""
import os
import json
import logging
from datetime import datetime
import streamlit as st

from lib.ai_client import get_ai_client, get_analysis_model

logger = logging.getLogger(__name__)

def _load_prompts() -> dict:
    """Tải cấu hình prompt từ file JSON prompts_config.json."""
    json_path = os.path.join(os.path.dirname(__file__), "prompts_config.json")
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read prompts_config.json: {e}")
        # Return fallback configuration dict in case file read fails
        return {
            "system_prompts": {
                "analysis_template": "Bạn là chuyên gia phân tích. Dữ liệu: {role_description}",
                "verify_template": "Bạn là chuyên gia tổng hợp. Dữ liệu: {role_description}"
            },
            "page_roles": {
                "overview": "Chuyên gia Môi trường vĩ mô"
            }
        }

def _parse_json_robust(text: str) -> dict:
    """Extract and parse JSON from LLM response content robustly."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_idx = text.find("{")
        end_idx = text.rfind("}")
        if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
            try:
                return json.loads(text[start_idx:end_idx+1])
            except json.JSONDecodeError as e:
                logger.error(f"Failed parsing extracted JSON block: {e}")
                raise
        else:
            raise

@st.cache_data(ttl=300, show_spinner=False)
def generate_ai_analysis(_context_hash: str, context_json: str, lang: str = "vi") -> dict:
    """Run direct single-call LLM analysis on the ClickHouse database context."""
    context = json.loads(context_json)
    page_name = context.get("page_name", "overview")
    
    prompts = _load_prompts()
    role = prompts["page_roles"].get(page_name, prompts["page_roles"].get("overview", ""))
    
    client = get_ai_client()
    model = get_analysis_model()
    
    if not client:
        return _fallback_rule_based(context_json, lang)
        
    try:
        system_prompt = prompts["system_prompts"]["analysis_template"].format(role_description=role)
        user_content = (
            f"{system_prompt}\n\n"
            f"--- BẮT ĐẦU DỮ LIỆU CONTEXT ---\n"
            f"Dữ liệu context trang {page_name}:\n{context_json}\n"
            f"--- KẾT THÚC DỮ LIỆU CONTEXT ---\n\n"
            f"YÊU CẦU: Hãy phân tích dữ liệu context trên theo các hướng dẫn ở trên."
        )
        call_response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a professional air quality data analyst."},
                {"role": "user", "content": user_content},
            ],
            max_tokens=1500,
            temperature=0.2,
        )
        final_content = call_response.choices[0].message.content
    except Exception as e:
        logger.error(f"LLM analysis failed: {e}")
        return _fallback_rule_based(context_json, lang)

    return {
        "status": "success",
        "content": final_content,
        "sources": [],
        "model": model,
        "timestamp": datetime.now().isoformat()
    }

def _fallback_rule_based(context_json: str, lang: str) -> dict:
    """Tự động trả về insights tĩnh khi API lỗi hoặc chưa thiết lập key."""
    context = json.loads(context_json)
    page_name = context.get("page_name", "overview")
    
    # Try importing local insights generator if available
    try:
        from lib.data_service.air_quality import generate_insights
        filters = context.get("filters", {})
        # Mock filters to pass standard parameters
        t_filters = {
            "spatial_grain": filters.get("spatial_grain", "Toàn quốc"),
            "scope_val": filters.get("scope_val"),
            "date_range": None, # default historical
            "time_grain": "Ngày",
            "time_unit": "day",
            "pollutant": filters.get("pollutant", "pm25"),
            "standard": filters.get("standard", "VN_AQI"),
        }
        insights_list = generate_insights(t_filters, lang)
        insights_md = ""
        for insight in insights_list:
            icon = insight.get("icon", "📌")
            title = insight.get("title", "")
            msg = insight.get("message", "")
            insights_md += f"### {icon} {title}\n{msg}\n\n"
        api_key_exists = bool(os.environ.get("CKEY_API_KEY"))
        if api_key_exists:
            content_prefix = "⚠️ **Không thể khởi chạy AI phân tích tự động** (Có lỗi xảy ra khi gọi API hoặc quá tải).\n\n"
        else:
            content_prefix = "⚠️ **AI phân tích tự động không khả dụng** (Chưa thiết lập CKEY_API_KEY).\n\n"
        content = content_prefix + insights_md
    except Exception as e:
        logger.warning(f"Failed to run rule-based fallback generator: {e}")
        api_key_exists = bool(os.environ.get("CKEY_API_KEY"))
        if api_key_exists:
            content = (
                "⚠️ **Không thể kết nối đến máy chủ AI** (Có lỗi xảy ra trong quá trình xử lý hoặc CSDL ClickHouse).\n\n"
                "**Nhận định tạm thời từ hệ thống:**\n"
                "- Các trạm đo đang ghi nhận chỉ số hoạt động bình thường.\n"
                "- Vui lòng thử tải lại trang hoặc kiểm tra nhật ký lỗi (logs) của container."
            )
        else:
            content = (
                "⚠️ **AI phân tích tự động không khả dụng** (Chưa thiết lập CKEY_API_KEY).\n\n"
                "**Nhận định từ hệ thống:**\n"
                "- Các trạm đo đang ghi nhận chỉ số hoạt động bình thường.\n"
                "- Vui lòng cấu hình khóa kết nối để nhận các nhận định chuyên sâu hơn."
            )

    return {
        "status": "fallback",
        "content": content,
        "sources": [],
        "model": "rule-based",
        "timestamp": datetime.now().isoformat(),
    }
