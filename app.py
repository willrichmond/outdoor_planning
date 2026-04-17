import streamlit as st

st.set_page_config(
    page_title="My App",
    page_icon="🏠",
)

st.title("🏠 Home")
st.write("Welcome! Use the sidebar to navigate between pages.")


# To-Dos
# gauges page
# round everything
# sort based on river level
# remove the rounding to round 2
# pandas styling for the river levels (fix rounding, add commas)
# fix the color coding
# clean the buerau of reclamation data
# update the logger statements
# paralelize the api calls (claude can help)
# remove other functions
# split the api calls
# Update the formulas for section_id
# Join the section names
# Create a flow function for level & boat type
# Fix the charts
# Update lat / lon for put ins
# Weather API function
# Add weather to river details
# Create a river page with flow lines
# Toggle by boat type
# https://altair.streamlit.app/Layer_Line_Color_Rule


# example = example.rename(
#     {
#         col: example_titles[int(col.split('_')[-1])]
#         for col in example.columns
#         if col.startswith('section_id')
#     }
# )

# Follow Up PR
# get lat/lon for put ins
# simple noaa api for river details page
# Add weather for skiing tab
# AQI API function


# from concurrent.futures import ThreadPoolExecutor, as_completed

# def get_river_gauge_data(gauge_list):
#     logger.info('Beginning to fetch river gauge data for all gauges...')
#     gauge_data_all = []

#     def process_gauge(gauge_loop):
#         results = []
#         gauge_name = gauge_loop['gauge_name']
#         logger.info(f"Processing gauge: {gauge_name}")

#         # Observed data
#         if gauge_loop['observed_api'] == 'waterdata_usgs':
#             observed_data = get_usgs_observed_flow(gauge_loop)
#             if observed_data is not None:
#                 results.extend(clean_usgs_noaa_data(observed_data))
#         elif gauge_loop['observed_api'] == 'bureau_reclamation':
#             observed_data = get_bureau_reclamation_observed_flow(gauge_loop)
#             if observed_data is not None:
#                 results.extend(observed_data)
#         else:
#             logger.info(f"⚠️ Unknown observed API for gauge {gauge_name}: {gauge_loop['observed_api']}")

#         # Forecast data
#         forecast_data = get_noaa_flow_forecast(gauge_loop)
#         if forecast_data is not None:
#             results.extend(clean_usgs_noaa_data(forecast_data))

#         return results

#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = {executor.submit(process_gauge, g): g for g in gauge_list}
#         for future in as_completed(futures):
#             try:
#                 gauge_data_all.extend(future.result())
#             except Exception as e:
#                 logger.info(f"❌ Unexpected error for gauge {futures[future]['gauge_name']}: {e}")

#     if not gauge_data_all:
#         return None

#     return pl.DataFrame(gauge_data_all)