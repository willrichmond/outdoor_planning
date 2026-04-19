import streamlit as st
import polars as pl
from utils.logger import logger
from utils.kayak_utils import get_clean_gauge_data, get_kayaking_levels, get_current_river_levels,get_river_gauge_data,get_kayaking_levels_range,get_color_flow_range
from data.kayak.kayak_static import section_list, gauge_list,river_list

@st.cache_data(ttl=3600)
def load_static_data():
    return pl.DataFrame(section_list), gauge_list, pl.DataFrame(river_list)


@st.cache_data(ttl=3600)
def run_river_flow_apis(gauge_list,section_df,river_df):
    river_gauge_data,gauge_run_details= get_river_gauge_data(gauge_list)
    clean_gauge_data = get_clean_gauge_data(river_gauge_data)
    kayaking_levels_cfs= get_kayaking_levels(
    df_clean=clean_gauge_data,
    value_type="flow_cfs",
)
    kayaking_levels_ft= get_kayaking_levels(
    df_clean=clean_gauge_data,
    value_type="stage_ft",)

    kayaking_levels_range = get_kayaking_levels_range(kayaking_levels_cfs,kayaking_levels_ft,section_df)

    kayaking_levels_current = get_current_river_levels(kayaking_levels_range,river_df)

    return  river_gauge_data,gauge_run_details,clean_gauge_data,kayaking_levels_cfs, kayaking_levels_ft,kayaking_levels_range,kayaking_levels_current

# kayaking_levels_current
# kayaking_levels_range
# gauge_run_details
# clean_gauge_data

# Data
section_df, gauge_list, river_df = load_static_data()
with st.spinner("Fetching river levels..."):
    river_gauge_data,gauge_run_details,clean_gauge_data,kayaking_levels_cfs, kayaking_levels_ft,kayaking_levels_range,kayaking_levels_current = run_river_flow_apis(gauge_list,section_df,river_df)


st.title("🌊 Kayaking")


# Tabs
tab_current, tab_forecast, tab_river_details, tab_gauges = st.tabs(["Current", "Forecast","River Details",'Gauges'])

with tab_current:
    st.subheader("Current River Levels")
    st.dataframe(kayaking_levels_current.style.apply(get_color_flow_range, axis=1),column_config={
        'flow_range': None,
        'flow_range_max': None,
    })

with tab_forecast:
    st.header("Forecast")

    section_options = st.multiselect(
    "Pick Your Rivers!",
    options=section_df['section_name'].to_list(),
    default=['Staircase','The Canyon'],
)

    kayaking_levels_filtered = (
        kayaking_levels_range
        .lazy()
        .select('section_name','mountain_time','river_level','flow_type')
        .filter(pl.col("section_name").is_in(section_options))
        .with_columns(section_name=pl.when(pl.col('flow_type')=='standard').then(pl.col('section_name')).otherwise(pl.col('section_name') + ' (Max)'))
        .collect()
    )

    st.line_chart(kayaking_levels_filtered,
                  x="mountain_time",
                  y="river_level",
                  color="section_name",
                  width="stretch",
                  height=500)

    st.dataframe(kayaking_levels_filtered)

with tab_river_details:

    st.dataframe(kayaking_levels_range)


with tab_gauges:
    st.dataframe(gauge_run_details)
    st.dataframe(clean_gauge_data)