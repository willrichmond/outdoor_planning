import streamlit as st

st.set_page_config(
    page_title="My App",
    page_icon="🏠",
)

st.title("🏠 Home")
st.write("Welcome! Use the sidebar to navigate between pages.")


# To-Dos

# add prediction type, and forecast type to gauges
# add dataset type (observed/forecast)
# clean the buerau of reclamation data
# update the logger statements
# remove other functions
# split the api calls
# Split guages into separate for NOAA only
# Fix the API calls (payette at cascade dam)
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