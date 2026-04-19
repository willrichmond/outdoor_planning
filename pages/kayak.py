import streamlit as st
import polars as pl
import pandas as pd
from typing import Any, Dict, List, Tuple
from utils.logger import logger
from utils.kayak_utils import get_clean_gauge_data, get_kayaking_levels, get_current_river_levels,get_river_gauge_data,get_kayaking_levels_range,get_color_flow_range
from data.kayak.kayak_static import section_list, gauge_list,river_list

@st.cache_data(ttl=3600)
def load_static_data()-> Tuple[pl.DataFrame, List[Dict[str, Any]], pl.DataFrame]:
    """
    Load and cache static reference data for river flow processing.

    Converts predefined in-memory data structures into Polars DataFrames where
    appropriate and caches the results to avoid repeated initialization.

    Returns:
        A tuple containing:
            - section_df (pl.DataFrame): Section metadata
            - gauge_list (list of dict): Gauge configuration objects
            - river_df (pl.DataFrame): River metadata

    Notes:
        - Data is cached for 1 hour using Streamlit's `@st.cache_data`.
        - Assumes `section_list`, `gauge_list`, and `river_list` are defined
        in the module scope.
        - `gauge_list` is returned as-is (not converted to Polars) because it
        is used as input to API-processing functions.
        """
    return pl.DataFrame(section_list), gauge_list, pl.DataFrame(river_list)


@st.cache_data(ttl=3600)
def run_river_flow_apis(
gauge_list: List[Dict[str, Any]],
section_df: pl.DataFrame,
river_df: pl.DataFrame,
) -> Tuple[pd.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Execute the full river flow data pipeline and cache results.

    Fetches observed and forecast gauge data, cleans and transforms it into
    section-level flow metrics, derives flow ranges and boat suitability,
    and produces a current river conditions summary for display.

    Args:
        gauge_list: List of gauge configuration dictionaries used to fetch
            observed and forecast data.
        section_df: Polars DataFrame containing section thresholds and metadata.
        river_df: Polars DataFrame containing river metadata.

    Returns:
        A tuple containing:
            - kayaking_levels_current (pd.DataFrame):
                Current river conditions formatted for display (Streamlit).
            - kayaking_levels_range (pl.DataFrame):
                Full section-level dataset with flow ranges and boat flags.
            - gauge_run_details (pl.DataFrame):
                Metadata about each API call (rows returned, errors, etc.).
            - clean_gauge_data (pl.DataFrame):
                Cleaned and normalized raw gauge data used for downstream
                transformations.

    Notes:
        - Results are cached for 1 hour using Streamlit's `@st.cache_data`.
        - Pipeline steps:
            1. Fetch gauge data (`get_river_gauge_data`)
            2. Clean timestamps and normalize (`get_clean_gauge_data`)
            3. Derive section levels (flow + stage)
            4. Compute flow ranges and boat suitability
            5. Generate current river summary for UI
        - Assumes all downstream functions are available in scope.
    """
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

    return  kayaking_levels_current,kayaking_levels_range,gauge_run_details,clean_gauge_data


# Data
section_df, gauge_list, river_df = load_static_data()
with st.spinner("Fetching river levels..."):
    kayaking_levels_current,kayaking_levels_range,gauge_run_details,clean_gauge_data = run_river_flow_apis(gauge_list,section_df,river_df)


# Streamlit Page
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

    st.header("Forecast Data Table")
    st.dataframe(kayaking_levels_filtered)

with tab_river_details:

    st.dataframe(kayaking_levels_range)


with tab_gauges:
    st.header("Gauge API Details")
    st.dataframe(gauge_run_details)

    st.header("Gauge Data Table")
    st.dataframe(clean_gauge_data)