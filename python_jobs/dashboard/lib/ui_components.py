import plotly.express as px
import streamlit as st
from lib.aqi_utils import get_aqi_color_scale, get_aqi_color_range, get_aqi_colorbar_config, get_aqi_discrete_colors
from lib.chart_config import get_plotly_layout, create_empty_state
from lib.i18n import t

def render_map_component(map_df, map_view, spatial_grain, source_name, scope_val, pollutant, standard, theme, val_label, lang, height=480):
    """Renders the Plotly map component for the dashboard."""
    label_col = "ward_name" if (spatial_grain in ["Tỉnh", "Phường"] or source_name == "aqiin") else "province"

    if pollutant == "aqi":
        color_scale = get_aqi_color_scale(standard)
        range_val   = get_aqi_color_range(standard)
    else:
        color_scale = "Viridis" if theme == "light" else "Plasma"
        range_val   = [0, map_df.display_val.max() * 1.1 if not map_df.empty else 100]

    if not map_df.empty:
        # Determine center and zoom level based on spatial scope
        is_national = (spatial_grain == "Toàn quốc" or not scope_val or scope_val == "Toàn quốc" or scope_val == "National")
        
        if is_national:
            # National view: Center on Da Nang area to display the full S-shaped country of Vietnam
            map_lat = 16.2
            map_lon = 108.2
            zoom_level = 4.8
        else:
            # Regional or local view: Center on the mean of the data points
            map_lat = map_df.latitude.mean()
            map_lon = map_df.longitude.mean()
            if spatial_grain in ["Tỉnh", "Phường"]:
                zoom_level = 8.5
            else:
                zoom_level = 5.8

        tooltip_data = {
            "province": True,
            "display_val": ":.1f",
            "latitude": False,
            "longitude": False,
        }
        if "confidence_score" in map_df.columns:
            tooltip_data["confidence_score"] = ":.2f"
        if "source_mix" in map_df.columns:
            tooltip_data["source_mix"] = True

        if map_view == "Heatmap":
            fig_map = px.density_map(
                map_df,
                lat="latitude", lon="longitude",
                z="display_val",
                radius=18,
                hover_name=label_col,
                hover_data=tooltip_data,
                color_continuous_scale=color_scale,
                range_color=range_val,
                zoom=zoom_level,
                center={"lat": map_lat, "lon": map_lon},
            )
        else:
            fig_map = px.scatter_map(
                map_df,
                lat="latitude", lon="longitude",
                color="display_val",
                hover_name=label_col,
                hover_data=tooltip_data,
                color_continuous_scale=color_scale,
                range_color=range_val,
                zoom=zoom_level,
                center={"lat": map_lat, "lon": map_lon},
                size="display_val",
                size_max=24,
                labels={
                    "display_val": val_label,
                    "province":    t("province", lang),
                    "ward_name":   t("location", lang),
                },
            )
        
        map_style = "carto-darkmatter" if theme == "dark" else "carto-positron"
        fig_map.update_layout(
            map_style=map_style,
            mapbox_style=map_style,
            height=height,
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)"
        )
        if pollutant == "aqi":
            fig_map.update_layout(coloraxis_colorbar=get_aqi_colorbar_config(standard, val_label))
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.plotly_chart(
            create_empty_state(t("no_data", lang) if lang == "en" else "Không có dữ liệu cho vùng này."),
            use_container_width=True,
        )

def render_distribution_chart(df_dist, pollutant, standard, val_label, lang, height=220):
    """Renders the distribution pie or histogram chart."""
    if df_dist.empty:
        st.plotly_chart(create_empty_state("No data", height=height), use_container_width=True)
        return

    if pollutant == "aqi":
        aqi_colors = get_aqi_discrete_colors(standard)
        color_map = {
            t("aqi_good", lang):           aqi_colors["Good"],
            t("aqi_moderate", lang):        aqi_colors["Moderate"],
            t("aqi_unhealthy_sg", lang):    aqi_colors["Unhealthy for Sensitive Groups"],
            t("aqi_unhealthy", lang):       aqi_colors["Unhealthy"],
            t("aqi_very_unhealthy", lang):  aqi_colors["Very Unhealthy"],
            t("aqi_hazardous", lang):       aqi_colors["Hazardous"],
        }
        fig = px.pie(
            df_dist, values="count", names="aqi_category",
            color="aqi_category", color_discrete_map=color_map,
        )
    else:
        fig = px.histogram(
            df_dist, x="display_val", marginal="box",
            labels={"display_val": val_label, "count": t("chart_label_count", lang)},
        )
    
    fig.update_layout(get_plotly_layout(height=height, compact=True))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    
    # Specific layout polish for Pie Chart to ensure visual balance and maximize readability
    if pollutant == "aqi":
        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.05,
                xanchor="center",
                x=0.5
            ),
            margin={"l": 10, "r": 10, "t": 15, "b": 35}
        )
        
    st.plotly_chart(fig, use_container_width=True)

def render_ranking_chart(rank_df, bar_y_col, color_scale, range_val, val_label, pollutant, standard, lang, height=220):
    """Renders the horizontal bar chart for top polluted areas."""
    if rank_df.empty:
        st.plotly_chart(create_empty_state("No data", height=height), use_container_width=True)
        return

    df_top = rank_df.sort_values("display_val", ascending=False).head(10)
    df_top = df_top.sort_values("display_val", ascending=True)

    fig = px.bar(
        df_top,
        y=bar_y_col,
        x="display_val",
        orientation="h",
        color="display_val",
        color_continuous_scale=color_scale,
        range_color=range_val,
        labels={"display_val": val_label, "province": t("province", lang)},
    )
    fig.update_layout(get_plotly_layout(height=height, compact=True))
    
    # Hide redundant colorbar and enlarge left margin to prevent province/ward name truncation
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin={"l": 85, "r": 15, "t": 10, "b": 15},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig, use_container_width=True)
