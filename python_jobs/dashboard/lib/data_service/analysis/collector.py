"""Page-specific context gatherers from ClickHouse tables."""
from datetime import datetime
import pandas as pd
import logging
import streamlit as st
from lib.clickhouse_client import query_df
from lib.data_service.core import get_source_table, get_pollutant_cols

logger = logging.getLogger(__name__)

@st.cache_data(ttl=300, show_spinner=False)
def collect_analysis_context(page_name: str, filters: dict, lang: str = "vi") -> dict:
    """Gather page-specific structured context datasets from ClickHouse."""
    spatial_grain = filters.get("spatial_grain", "Toàn quốc")
    scope_val = filters.get("scope_val")
    date_range = filters.get("date_range")
    time_grain = filters.get("time_grain", "Ngày")
    time_unit = filters.get("time_unit", "day")
    pollutant = filters.get("pollutant", "pm25")
    standard = filters.get("standard", "VN_AQI")
    source_name = filters.get("source_name", "aqiin")
    
    context = {
        "page_name": page_name,
        "filters": {
            "spatial_grain": spatial_grain,
            "scope_val": scope_val,
            "date_range": str(date_range) if date_range else "Lịch sử mặc định",
            "pollutant": pollutant,
            "standard": standard,
            "source_name": source_name,
        },
        "timestamp": datetime.now().isoformat(),
    }
    
    try:
        if page_name == "overview":
            from lib.data_service.air_quality import get_national_summary, generate_insights, get_chart_data, get_aqi_distribution
            from lib.data_service.weather import get_weather_summary_stats, get_weather_ranking_data
            from lib.data_service.traffic import get_traffic_summary_stats, get_traffic_ranking_data
            from lib.data_service.core import get_pollutant_col, escape_value, build_where_clause
            
            table = get_source_table(spatial_grain, time_grain, source_name)
            avg_col, max_col = get_pollutant_cols(pollutant, standard)
            
            # --- 1. OVERVIEW DATA ---
            # 1.1 National summary
            try:
                summary_df = get_national_summary(table, avg_col, max_col, spatial_grain, scope_val, date_range, time_unit, source_name)
                context["aqi_summary"] = summary_df.to_dict(orient="records") if not summary_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching overview national summary: {ex}")
                context["aqi_summary"] = []
                
            # 1.2 Top polluted provinces and worst province details
            try:
                map_df = get_chart_data(table, avg_col, spatial_grain, scope_val, date_range, time_unit, source_name)
                if not map_df.empty:
                    top_df = map_df.sort_values("display_val", ascending=False)
                    context["top_provinces"] = top_df.head(5)[["province", "display_val"]].to_dict(orient="records")
                    worst_idx = map_df["display_val"].idxmax()
                    context["worst_province"] = map_df.loc[worst_idx][["province", "display_val"]].to_dict()
                    context["exceeding_count"] = len(map_df[map_df["display_val"] > 150])
                else:
                    context["top_provinces"] = []
                    context["worst_province"] = {}
                    context["exceeding_count"] = 0
            except Exception as ex:
                logger.error(f"Error compiling top/worst provinces: {ex}")
                context["top_provinces"] = []
                context["worst_province"] = {}
                context["exceeding_count"] = 0
                
            # 1.3 AQI distribution percentages
            try:
                dist_df = get_aqi_distribution(table, avg_col, spatial_grain, scope_val, date_range, time_unit, source_name, lang=lang) if pollutant == "aqi" else pd.DataFrame()
                context["aqi_distribution"] = dist_df.to_dict(orient="records") if not dist_df.empty else []
            except Exception as ex:
                logger.error(f"Error compiling AQI distribution: {ex}")
                context["aqi_distribution"] = []
                
            # 1.4 Dynamic Insights
            try:
                insights_filters = {
                    "spatial_grain": spatial_grain,
                    "scope_val": scope_val,
                    "date_range": date_range,
                    "time_grain": time_grain,
                    "pollutant": pollutant,
                    "standard": standard,
                }
                insights = generate_insights(insights_filters, lang=lang)
                context["dynamic_insights"] = insights
            except Exception as ex:
                logger.error(f"Error generating insights: {ex}")
                context["dynamic_insights"] = []
                
            # --- 2. POLLUTANTS DATA ---
            # 2.1 Compliance stats (WHO/TCVN limits and PM2.5/PM10 ratio)
            try:
                who_lim = 15 if pollutant == "pm25" else 45 if pollutant == "pm10" else 25
                tcvn_lim = 50 if pollutant == "pm25" else 100 if pollutant == "pm10" else 80
                col = get_pollutant_col(pollutant)
                where_clause = build_where_clause(spatial_grain, scope_val, date_range)
                q_comp = f"""
                SELECT 
                    count() as total_days, 
                    sum(if({col} > {who_lim}, 1, 0)) as who_breaches, 
                    sum(if({col} <= {tcvn_lim}, 1, 0)) as tcvn_compliance,
                    avg(pm25_avg) as avg_pm25,
                    avg(pm10_avg) as avg_pm10
                FROM air_quality.dm_air_quality_overview_daily 
                WHERE {where_clause} AND {col} IS NOT NULL
                """
                compliance_df = query_df(q_comp)
                if not compliance_df.empty:
                    comp_row = compliance_df.iloc[0]
                    avg_pm25 = comp_row.avg_pm25 or 0
                    avg_pm10 = comp_row.avg_pm10 or 0
                    ratio = (avg_pm25 / avg_pm10) if avg_pm10 > 0 else 0
                    
                    context["pollutants_compliance"] = {
                        "total_days": int(comp_row.total_days or 0),
                        "who_breaches": int(comp_row.who_breaches or 0),
                        "tcvn_compliance": int(comp_row.tcvn_compliance or 0),
                        "who_breach_pct": float(comp_row.who_breaches / comp_row.total_days * 100) if comp_row.total_days > 0 else 0.0,
                        "tcvn_compliance_pct": float(comp_row.tcvn_compliance / comp_row.total_days * 100) if comp_row.total_days > 0 else 0.0,
                        "pm25_pm10_ratio": float(ratio),
                        "probable_source": "Giao thông" if ratio > 0.6 else "Bụi / XD" if ratio < 0.4 else "Hỗn hợp"
                    }
                else:
                    context["pollutants_compliance"] = {}
            except Exception as ex:
                logger.error(f"Error fetching pollutants compliance statistics: {ex}")
                context["pollutants_compliance"] = {}

            # 2.2 Emission sources mix percentages
            try:
                source_mix_type = "observed" if source_name == "aqiin" else "modeled" if source_name == "openweather" else "observed"
                where_mix = build_where_clause(spatial_grain, scope_val, date_range)
                if where_mix:
                    where_mix += f" AND source_mix = '{source_mix_type}'"
                else:
                    where_mix = f"source_mix = '{source_mix_type}'"
                q_fp = f"""
                SELECT probable_source, count() as count 
                FROM air_quality.dm_pollutant_source_fingerprint 
                WHERE {where_mix}
                GROUP BY probable_source
                """
                sources_df = query_df(q_fp)
                context["emission_sources"] = sources_df.to_dict(orient="records") if not sources_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching emission fingerprint sources: {ex}")
                context["emission_sources"] = []
                
            # --- 3. HISTORICAL TREND DATA ---
            # 3.1 Daily average AQI and Max AQI timeline
            try:
                source_mix_val = "observed" if source_name == "aqiin" else "modeled" if source_name == "openweather" else "observed"
                where_daily = build_where_clause(spatial_grain, scope_val, date_range)
                if where_daily:
                    where_daily += f" AND source_mix = '{source_mix_val}'"
                else:
                    where_daily = f"source_mix = '{source_mix_val}'"
                
                q_daily = f"""
                SELECT
                    toString(date) as date,
                    round(avg({avg_col}), 1) as avg_aqi,
                    round(max({max_col}), 0) as max_aqi
                FROM air_quality.dm_air_quality_overview_daily
                WHERE {where_daily} AND {avg_col} IS NOT NULL
                GROUP BY date
                ORDER BY date ASC
                """
                daily_df = query_df(q_daily)
                context["daily_aqi_timeline"] = daily_df.to_dict(orient="records") if not daily_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching daily AQI timeline: {ex}")
                context["daily_aqi_timeline"] = []

            # 3.2 Month-over-month averages
            try:
                source_mix_val = "observed" if source_name == "aqiin" else "modeled" if source_name == "openweather" else "observed"
                q_monthly = f"""
                SELECT
                    toStartOfMonth(date) as month,
                    avg({avg_col}) as avg_aqi,
                    max({max_col}) as max_aqi
                FROM air_quality.dm_air_quality_overview_daily
                WHERE province != '' AND {avg_col} IS NOT NULL AND source_mix = '{source_mix_val}'
                GROUP BY month
                ORDER BY month DESC
                LIMIT 6
                """
                monthly_df = query_df(q_monthly)
                if not monthly_df.empty:
                    monthly_df["month"] = monthly_df["month"].astype(str)
                    context["monthly_aqi_trends"] = monthly_df.to_dict(orient="records")
                else:
                    context["monthly_aqi_trends"] = []
            except Exception as ex:
                logger.error(f"Error fetching monthly AQI trends: {ex}")
                context["monthly_aqi_trends"] = []
                
            # 3.3 Pollution Spikes (> 150 AQI)
            try:
                source_mix_val = "observed" if source_name == "aqiin" else "modeled" if source_name == "openweather" else "observed"
                where_spikes = build_where_clause(spatial_grain, scope_val, date_range)
                q_spikes = f"""
                SELECT
                    toString(date) as date,
                    province,
                    {avg_col} as avg_aqi,
                    {max_col} as max_aqi
                FROM air_quality.dm_air_quality_overview_daily
                WHERE {where_spikes} AND {max_col} > 150 AND province != '' AND source_mix = '{source_mix_val}'
                ORDER BY date ASC, max_aqi DESC
                """
                spikes_df = query_df(q_spikes)
                context["pollution_spikes"] = spikes_df.to_dict(orient="records") if not spikes_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching pollution spikes: {ex}")
                context["pollution_spikes"] = []

            # --- 4. TRAFFIC IMPACT DATA ---
            # 4.1 Traffic correlation KPIs
            try:
                traffic_df = get_traffic_summary_stats(spatial_grain, scope_val, date_range)
                context["traffic_stats"] = traffic_df.to_dict(orient="records")[0] if not traffic_df.empty else {}
            except Exception as ex:
                logger.error(f"Error fetching traffic stats: {ex}")
                context["traffic_stats"] = {}
                
            # 4.2 Traffic hotspots
            try:
                target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
                hotspots_df = get_traffic_ranking_data(spatial_grain, scope_val, date_range, col=target_poll)
                context["traffic_hotspots"] = hotspots_df.head(6).to_dict(orient="records") if not hotspots_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching traffic hotspots ranking: {ex}")
                context["traffic_hotspots"] = []

            # --- 5. WEATHER IMPACT DATA ---
            # 5.1 Weather correlation KPIs
            try:
                target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
                stagnant_sum_col = f"stagnant_{target_poll}_sum"
                dispersive_sum_col = f"dispersive_{target_poll}_sum"
                weather_df = get_weather_summary_stats(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)
                context["weather_stats"] = weather_df.to_dict(orient="records")[0] if not weather_df.empty else {}
            except Exception as ex:
                logger.error(f"Error fetching weather stats: {ex}")
                context["weather_stats"] = {}
                
            # 5.2 Weather stagnation ranking
            try:
                target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
                stagnant_sum_col = f"stagnant_{target_poll}_sum"
                dispersive_sum_col = f"dispersive_{target_poll}_sum"
                p_col = f"{target_poll}_daily_avg"
                ranking_df = get_weather_ranking_data(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col, p_col=p_col)
                context["weather_stagnation_ranking"] = ranking_df.head(6).to_dict(orient="records") if not ranking_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching weather stagnation ranking: {ex}")
                context["weather_stagnation_ranking"] = []
                
            # 5.3 Weather correlation matrix static values
            context["weather_correlations"] = {
                "pm25_temp": -0.16,
                "pm25_hum": 0.22,
                "pm25_wind": -0.33,
                "temp_hum": -0.92,
                "temp_wind": 0.69,
                "hum_wind": -0.68
            }

            # --- 6. HEALTH RISK DATA ---
            # 6.1 Health risk KPIs and Exposure ranking
            try:
                health_where = "1=1"
                if spatial_grain in ["Vùng", "Region"] and scope_val:
                    health_where = f"region_3 = '{escape_value(scope_val)}'"
                elif spatial_grain in ["Khu vực", "Area"] and scope_val:
                    health_where = f"region_8 = '{escape_value(scope_val)}'"
                elif spatial_grain in ["Tỉnh", "Phường", "Province", "Ward"] and scope_val:
                    health_where = f"province = '{escape_value(scope_val)}'"

                q_health = f"""
                SELECT
                    province,
                    population,
                    time_weighted_pm25,
                    confidence_score,
                    confidence_level,
                    source_mix,
                    total_exposure_index_m,
                    risk_category,
                    exposure_risk_category,
                    pollution_rank,
                    exposure_rank,
                    national_risk_rank
                FROM air_quality.dm_regional_health_risk_ranking
                WHERE {health_where}
                ORDER BY national_risk_rank ASC
                """
                health_df = query_df(q_health)
                if not health_df.empty:
                    top_polluted = health_df.sort_values("time_weighted_pm25", ascending=False).iloc[0]
                    mean_pm25 = health_df.time_weighted_pm25.mean()
                    high_risk_count = len(health_df[health_df.risk_category.isin(['CRITICAL', 'HIGH RISK'])])
                    total_pop = health_df.population.sum()
                    
                    context["health_kpis"] = {
                        "worst_province": top_polluted.province,
                        "worst_pm25": float(top_polluted.time_weighted_pm25),
                        "mean_pm25": float(mean_pm25),
                        "critical_high_risk_provinces": int(high_risk_count),
                        "exposed_population_m": float(total_pop / 1_000_000)
                    }
                    
                    risk_counts = health_df.groupby("risk_category").size().to_dict()
                    total_p = sum(risk_counts.values())
                    context["health_risk_distribution"] = {k: float(v / total_p * 100) for k, v in risk_counts.items()}
                    
                    exposure_rank_df = health_df.sort_values("total_exposure_index_m", ascending=False)
                    context["population_exposure_ranking"] = exposure_rank_df.head(6)[["province", "total_exposure_index_m", "risk_category"]].to_dict(orient="records")
                else:
                    context["health_kpis"] = {}
                    context["health_risk_distribution"] = {}
                    context["population_exposure_ranking"] = []
            except Exception as ex:
                logger.error(f"Error fetching health risk: {ex}")
                context["health_kpis"] = {}
                context["health_risk_distribution"] = {}
                context["population_exposure_ranking"] = []
            
        elif page_name == "pollutants":
            from lib.data_service.core import get_pollutant_col
            who_lim = 15 if pollutant == "pm25" else 45 if pollutant == "pm10" else 25
            tcvn_lim = 50 if pollutant == "pm25" else 100 if pollutant == "pm10" else 80
            table = get_source_table(spatial_grain, time_grain, source_name)
            col = get_pollutant_col(pollutant)
            
            try:
                q = f"""
                SELECT 
                    count() as total_days, 
                    sum(if({col} > {who_lim}, 1, 0)) as who_breaches, 
                    sum(if({col} <= {tcvn_lim}, 1, 0)) as tcvn_compliance 
                FROM air_quality.{table} 
                WHERE {col} IS NOT NULL
                """
                compliance_df = query_df(q)
                context["compliance_stats"] = compliance_df.to_dict(orient="records") if not compliance_df.empty else []
            except Exception as ex:
                logger.error(f"Error running pollutants compliance: {ex}")
                context["compliance_stats"] = []
                
            try:
                q_fp = "SELECT probable_source, count() as count FROM air_quality.dm_pollutant_source_fingerprint GROUP BY probable_source"
                sources_df = query_df(q_fp)
                context["emission_sources"] = sources_df.to_dict(orient="records") if not sources_df.empty else []
            except Exception as ex:
                logger.error(f"Error running fingerprint sources: {ex}")
                context["emission_sources"] = []

        elif page_name == "weather":
            from lib.data_service.weather import get_weather_summary_stats, get_weather_ranking_data
            target_poll = "pm25" if pollutant not in ["pm10"] else pollutant
            stagnant_sum_col = f"stagnant_{target_poll}_sum"
            dispersive_sum_col = f"dispersive_{target_poll}_sum"
            p_col = f"{target_poll}_daily_avg"
            
            try:
                weather_df = get_weather_summary_stats(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col)
                context["weather_summary"] = weather_df.to_dict(orient="records") if not weather_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching weather summary: {ex}")
                context["weather_summary"] = []
                
            try:
                ranking_df = get_weather_ranking_data(spatial_grain, scope_val, date_range, p_stag=stagnant_sum_col, p_disp=dispersive_sum_col, p_col=p_col)
                context["stagnation_ranking"] = ranking_df.to_dict(orient="records") if not ranking_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching stagnation ranking: {ex}")
                context["stagnation_ranking"] = []

        elif page_name == "traffic":
            from lib.data_service.traffic import get_traffic_summary_stats, get_traffic_ranking_data
            try:
                traffic_df = get_traffic_summary_stats(spatial_grain, scope_val, date_range)
                context["traffic_summary"] = traffic_df.to_dict(orient="records") if not traffic_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching traffic summary: {ex}")
                context["traffic_summary"] = []
                
            try:
                hotspots_df = get_traffic_ranking_data(spatial_grain, scope_val, date_range, col=pollutant)
                context["traffic_hotspots"] = hotspots_df.to_dict(orient="records") if not hotspots_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching traffic hotspots: {ex}")
                context["traffic_hotspots"] = []

        elif page_name == "health_risk":
            try:
                q = "SELECT province, population, time_weighted_pm25, risk_category, exposure_risk_category FROM air_quality.dm_regional_health_risk_ranking ORDER BY time_weighted_pm25 DESC LIMIT 10"
                health_df = query_df(q)
                context["health_risks"] = health_df.to_dict(orient="records") if not health_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching health risk ranking: {ex}")
                context["health_risks"] = []

        elif page_name == "source_comparison":
            try:
                q = "SELECT source, reliable_pct, latest_lag_hours, offline_count FROM air_quality.dm_platform_source_health"
                freshness_df = query_df(q)
                context["source_freshness"] = freshness_df.to_dict(orient="records") if not freshness_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching source freshness stats: {ex}")
                context["source_freshness"] = []

        elif page_name == "historical_trend":
            try:
                q = "SELECT toStartOfMonth(date) as month, avg(pm25_daily_aqi) as avg_aqi FROM air_quality.fct_air_quality_summary_daily GROUP BY month ORDER BY month DESC LIMIT 6"
                trends_df = query_df(q)
                # Convert month timestamps to strings
                if not trends_df.empty:
                    trends_df["month"] = trends_df["month"].astype(str)
                    context["monthly_trends"] = trends_df.to_dict(orient="records")
                else:
                    context["monthly_trends"] = []
            except Exception as ex:
                logger.error(f"Error fetching historical monthly trends: {ex}")
                context["monthly_trends"] = []

        elif page_name == "alerts":
            try:
                q = "SELECT compliance_status, count() as days_count FROM air_quality.dm_aqi_compliance_standards GROUP BY compliance_status"
                alerts_df = query_df(q)
                context["compliance_timeline"] = alerts_df.to_dict(orient="records") if not alerts_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching compliance stats: {ex}")
                context["compliance_timeline"] = []

        elif page_name == "status":
            try:
                q = "SELECT latest_lag_hours, reliable_pct, attention_count, offline_count FROM air_quality.dm_platform_health_summary"
                status_df = query_df(q)
                context["system_status"] = status_df.to_dict(orient="records") if not status_df.empty else []
            except Exception as ex:
                logger.error(f"Error fetching platform health status: {ex}")
                context["system_status"] = []

    except Exception as e:
        logger.error(f"Error collecting context for page '{page_name}': {e}")
        context["error"] = f"Không thể lấy dữ liệu đầy đủ từ CSDL: {str(e)}"
        
    return context
