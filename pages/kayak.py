import streamlit as st
import polars as pl
from utils.kayak_utils import get_gauge_data, get_clean_gauge_data, get_kayaking_levels, get_current_river_levels
from data.kayak import section_list, gauge_list,river_list

@st.cache_data
def load_static_data():
    return pl.DataFrame(section_list), pl.DataFrame(gauge_list), pl.DataFrame(river_list)


@st.cache_data(ttl=3600)
def run_apis():
    gauge_data = get_gauge_data(pl.DataFrame(gauge_list))
    clean_gauge_data = get_clean_gauge_data(gauge_data)
    kayaking_levels = get_kayaking_levels(clean_gauge_data, value_type="flow_cfs")
    current_river_levels = get_current_river_levels(kayaking_levels)

    return kayaking_levels, current_river_levels


# Data
with st.spinner("Fetching river levels..."):
    kayaking_levels, current_river_levels = run_apis()

section_details, gauge_details, river_details = load_static_data()

st.title("📊 Kayaking")

# Tabs
tab_current, tab_forecast, tab_river_details = st.tabs(["Current", "Forecast","River Details"])

with tab_current:
    st.subheader("Current River Levels")
    st.dataframe(current_river_levels)
with tab_forecast:
    st.header("Forecast")

    section_options = st.multiselect(
    "Pick Your Rivers!",
    ['main_payette', 'lower_payette', 'lower_payette_climax', 'payette_gutter', 'nf_payette_warm_up', 'sf_payette_grandjean', 'sf_payette_kirkham', 'sf_payette_canyon_low', 'sf_payette_canyon_high', 'sf_payette_staircase', 'deadwood', 'salmon_riggins', 'salmon_mill_wave', 'little_salmon', 'upper_lochsa', 'lower_lochsa', 'boise_ww_park', 'boise_barber_park', 'owyhee_three_forks', 'mf_salmon', 'murtaugh'],
    default=['sf_payette_canyon_low', 'sf_payette_canyon_high', 'sf_payette_staircase',],
)

    kayaking_levels_filtered = (
        kayaking_levels
        .unpivot(
        index=['mountain_time','data_type'],
        on=[x for x in kayaking_levels.columns if x not in ('mountain_time', 'data_type')],
        variable_name="section",
        value_name="flow"
        )
        .filter(pl.col("section").is_in(section_options))
    )

    st.line_chart(kayaking_levels_filtered,
                  x="mountain_time",
                  y="flow", color="section",
                  width="stretch",
                  height=500)

with tab_river_details:
    st.header("River Details")
    st.subheader("Sections")
    st.dataframe(section_details)
    st.subheader("Gauges")
    st.dataframe(gauge_details)
    st.subheader("Rivers")
    st.dataframe(river_details)