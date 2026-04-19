import streamlit as st
import polars as pl
from utils.logger import logger
from utils.kayak_utils import get_clean_gauge_data, get_kayaking_levels, get_current_river_levels,get_river_gauge_data,get_kayaking_levels_range,get_kayaking_levels_pivot
from data.kayak import section_list, gauge_list,river_list

@st.cache_data(ttl=3600)
def load_static_data():
    return pl.DataFrame(section_list), gauge_list, pl.DataFrame(river_list)


@st.cache_data(ttl=3600)
def run_river_flow_apis(gauge_list,section_df):
    river_gauge_data= get_river_gauge_data(gauge_list)
    clean_gauge_data = get_clean_gauge_data(river_gauge_data)
    kayaking_levels_cfs= get_kayaking_levels(
    df_clean=clean_gauge_data,
    value_type="flow_cfs",
)
    kayaking_levels_ft= get_kayaking_levels(
    df_clean=clean_gauge_data,
    value_type="stage_ft",)

    kayaking_levels_range = get_kayaking_levels_range(kayaking_levels_cfs,kayaking_levels_ft,section_df)

    kayaking_levels_current = get_current_river_levels(kayaking_levels_range)

    return  river_gauge_data,clean_gauge_data,kayaking_levels_cfs, kayaking_levels_ft,kayaking_levels_range,kayaking_levels_current



# Data
section_df, gauge_list, river_df = load_static_data()
with st.spinner("Fetching river levels..."):
    river_gauge_data,clean_gauge_data,kayaking_levels_cfs, kayaking_levels_ft,kayaking_levels_range,kayaking_levels_current = run_river_flow_apis(gauge_list,section_df)

def color_flow_range(row):
    colors = {
        'Too Low': 'background-color: #d4e6f1',
        'Low': 'background-color: #a9cce3',
        'Medium': 'background-color: #82e0aa',
        'High': 'background-color: #f0b27a',
        'Too High': 'background-color: #ec7063',
        None: ''
    }
    color_standard = colors.get(row['flow_range'], '')
    color_max = colors.get(row['flow_range_max'], '')

    return [color_standard if col == 'river_level' else color_max if col == 'river_level_max' else '' for col in row.index]

st.title("📊 Kayaking")


# Tabs
tab_current, tab_forecast, tab_river_details, tab_gauges = st.tabs(["Current", "Forecast","River Details",'Gauges'])

with tab_current:
    st.subheader("Current River Levels")
    st.dataframe(kayaking_levels_current.style.apply(color_flow_range, axis=1),column_config={
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
    st.dataframe(section_df)
    st.dataframe(kayaking_levels_range)

#     kayaking_levels_filtered = (
#         kayaking_levels
#         .unpivot(
#         index=['mountain_time','data_type'],
#         on=[x for x in kayaking_levels.columns if x not in ('mountain_time', 'data_type')],
#         variable_name="section",
#         value_name="flow"
#         )
#         .filter(pl.col("section").is_in(section_options))
#     )

#     st.line_chart(kayaking_levels_filtered,
#                   x="mountain_time",
#                   y="flow", color="section",
#                   width="stretch",
#                   height=500)

with tab_river_details:
    st.dataframe(river_gauge_data)
    st.dataframe(clean_gauge_data)
    st.dataframe(kayaking_levels_cfs)
    st.dataframe(kayaking_levels_ft)


with tab_gauges:
    st.dataframe(pl.DataFrame(gauge_list))