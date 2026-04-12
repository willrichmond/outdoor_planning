import streamlit as st

st.title("⚙️ Page Two")
st.write("This is page two.")

number = st.slider("Pick a number", 0, 100, 50)
st.write(f"You picked: **{number}**")