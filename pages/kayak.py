import streamlit as st
import polars as pl
from utils.kayak_utils import get_gauge_data, get_clean_gauge_data, get_kayaking_levels, get_current_river_levels
from data.kayak import section_list, gauge_list,river_list

st.title("📊 Page One")
st.write("This is page one.")

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

with st.spinner("Fetching river levels..."):
    kayaking_levels, current_river_levels = run_apis()


section, gauge, river = load_static_data()

st.subheader("Sections")
st.dataframe(section)

st.subheader("Gauges")
st.dataframe(gauge)

st.subheader("Rivers")
st.dataframe(river)

st.subheader("Current River Levels")
st.dataframe(kayaking_levels)
st.dataframe(current_river_levels)