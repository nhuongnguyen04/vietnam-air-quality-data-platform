"""Tab renderer for the 3-tab layout pattern (Ground / Satellite / Comparison)."""
import streamlit as st
from lib.data_service import get_source_coverage

def render_coverage_banner(source_name: str, spatial_grain: str, scope_val: str, lang: str):
    """Render standardized coverage warning/info banners for a source."""
    if source_name == "aqiin":
        prov_val = scope_val if spatial_grain in ["Tỉnh", "Phường"] else None
        cov_df = get_source_coverage(prov_val)
        if not cov_df.empty:
            if prov_val:
                row = cov_df.iloc[0]
                cov_pct = row["aqiin_coverage_pct"]
                total_w = row["total_ward_count"]
                aqi_w = row["aqiin_ward_count"]

                if cov_pct < 50:
                    st.warning(
                        f"⚠️ **Chất lượng bao phủ thấp:** Chỉ có **{cov_pct:.1f}%** số phường/xã "
                        f"({int(aqi_w)}/{int(total_w)}) tại **{prov_val}** có trạm quan trắc mặt đất hoạt động. "
                        f"Dữ liệu bản đồ có thể có khoảng trống không gian lớn."
                        if lang == "vi" else
                        f"⚠️ **Low Spatial Coverage:** Only **{cov_pct:.1f}%** of wards "
                        f"({int(aqi_w)}/{int(total_w)}) in **{prov_val}** have active ground monitors. "
                        f"Map visualization may contain significant spatial gaps."
                    )
                else:
                    st.success(
                        f"✅ **Độ bao phủ mặt đất tốt:** **{cov_pct:.1f}%** số phường/xã "
                        f"({int(aqi_w)}/{int(total_w)}) tại **{prov_val}** có trạm quan trắc hoạt động."
                        if lang == "vi" else
                        f"✅ **Good Ground Coverage:** **{cov_pct:.1f}%** of wards "
                        f"({int(aqi_w)}/{int(total_w)}) in **{prov_val}** have active ground monitors."
                    )
            else:
                total_aqiin_wards = cov_df["aqiin_ward_count"].sum()
                total_wards = cov_df["total_ward_count"].sum()
                cov_pct = (total_aqiin_wards * 100.0 / total_wards) if total_wards > 0 else 0

                if cov_pct < 30:
                    st.warning(
                        f"⚠️ **Bao phủ trạm mặt đất hạn chế:** Toàn quốc chỉ có **{cov_pct:.1f}%** số phường/xã "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) có trạm quan trắc mặt đất hoạt động. "
                        f"Khuyến nghị tham khảo thêm tab **🛰 Mô hình vệ tinh** để bổ sung vùng thiếu dữ liệu."
                        if lang == "vi" else
                        f"⚠️ **Limited Ground Monitor Coverage:** Only **{cov_pct:.1f}%** of wards "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) nationwide have active ground monitors. "
                        f"We recommend checking the **🛰 Satellite Model** tab for full spatial coverage."
                    )
                else:
                    st.success(
                        f"✅ **Tình trạng bao phủ mặt đất:** **{cov_pct:.1f}%** số phường/xã toàn quốc "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) có trạm quan trắc hoạt động."
                        if lang == "vi" else
                        f"✅ **Ground Monitor Coverage:** **{cov_pct:.1f}%** of wards "
                        f"({int(total_aqiin_wards)}/{int(total_wards)}) nationwide have active ground monitors."
                    )
    elif source_name == "openweather":
        st.info(
            "🛰️ **Mô hình Vệ tinh & SILAM:** Cung cấp độ bao phủ địa lý đầy đủ (100% xã/phường) dựa trên "
            "dữ liệu lưới ô ~25km từ Viện Khí tượng Phần Lan (FMI). Lưu ý: do là mô hình kết hợp, "
            "chỉ số AQI/nồng độ chất ô nhiễm thường có xu hướng thấp hơn (underestimate) thực tế đo tại mặt đất từ 1.5 đến 2.5 lần."
            if lang == "vi" else
            "🛰️ **Satellite & SILAM Model:** Provides complete geographic coverage (100% of wards) based on "
            "~25km grid resolution from the Finnish Meteorological Institute (FMI). Note: due to the model blending nature, "
            "AQI/concentrations typically tend to be underestimated by 1.5x to 2.5x compared to ground-truth monitors."
        )

def render_3_tabs(
    lang: str,
    ground_label: str,
    sat_label: str,
    comp_label: str,
    render_ground_fn,
    render_sat_fn,
    render_comp_fn,
    sat_info_text_vi: str = None,
    sat_info_text_en: str = None
):
    """Render unified 3-tab layout and execute corresponding content renderers."""
    tab_ground, tab_sat, tab_comp = st.tabs([ground_label, sat_label, comp_label])
    
    with tab_ground:
        render_ground_fn()
        
    with tab_sat:
        if sat_info_text_vi and lang == "vi":
            st.info(sat_info_text_vi)
        elif sat_info_text_en and lang == "en":
            st.info(sat_info_text_en)
        render_sat_fn()
        
    with tab_comp:
        render_comp_fn()
