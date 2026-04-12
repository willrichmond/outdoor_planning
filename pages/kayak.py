import streamlit as st
import polars as pl
from data.kayak import section_list, gauge_list,river_list

st.title("📊 Page One")
st.write("This is page one.")

section = pl.DataFrame(section_list)
gauge = pl.DataFrame(gauge_list)
river = pl.DataFrame(river_list)

st.subheader("Sections")
st.dataframe(section)

st.subheader("Gauges")
st.dataframe(gauge)

st.subheader("Rivers")
st.dataframe(river)