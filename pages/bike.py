import streamlit as st
import altair as alt
import polars as pl

st.title("⚙️ Page Two")
st.write("This is page two.")

number = st.slider("Pick a number", 0, 100, 50)
st.write(f"You picked: **{number}**")

source = pl.DataFrame({
    "date": (
        ["2000-01-01", "2000-02-01", "2000-03-01", "2000-04-01", "2000-05-01",
         "2000-06-01", "2000-07-01", "2000-08-01", "2000-09-01", "2000-10-01"] * 3
    ),
    "symbol": (["AAPL"] * 10) + (["GOOG"] * 10) + (["MSFT"] * 10),
    "price": [
        25.0, 28.0, 33.0, 30.0, 27.0, 35.0, 38.0, 32.0, 36.0, 40.0,
        100.0, 110.0, 108.0, 115.0, 120.0, 118.0, 125.0, 130.0, 128.0, 135.0,
        45.0, 47.0, 50.0, 48.0, 52.0, 55.0, 53.0, 57.0, 60.0, 58.0,
    ],
}).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

base = alt.Chart(source).properties(width=550)

line = base.mark_line().encode(
    x="date",
    y="price",
    color="symbol"
)

rule = base.mark_rule().encode(
    y="average(price)",
    color="symbol",
    size=alt.value(2)
)

chart = line + rule

tab1, tab2 = st.tabs(["Streamlit theme (default)", "Altair native theme"])

with tab1:
    st.altair_chart(chart, theme="streamlit", width='stretch')
with tab2:
    st.altair_chart(chart, theme=None, width='stretch')



line_df = pl.DataFrame({
    "x": list(range(100)),
    "flow": [3600 + i * 15 + (i % 7) * 30 for i in range(100)],
})

bands_df = pl.DataFrame({
    "y1":    [0,    800,  2000, 4000],
    "y2":    [800,  2000, 4000, 7000],
    "color": ["#ff8080", "#90ee90", "#60d060", "#22bb22"],
})

bands = alt.Chart(bands_df).mark_rect(opacity=0.6).encode(
    y=alt.Y("y1:Q"),
    y2=alt.Y2("y2:Q"),
    color=alt.Color("color:N", scale=None),
)

line = alt.Chart(line_df).mark_line(color="#2c3e6b", strokeWidth=2).encode(
    x="x:Q",
    y=alt.Y("flow:Q", scale=alt.Scale(domain=[0, 7000]))
)

chart = (bands + line).properties(width=700, height=400)

st.altair_chart(chart, width='stretch')