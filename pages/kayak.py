import streamlit as st

st.title("📊 Page One")
st.write("This is page one.")

name = st.text_input("Enter your name")
if name:
    st.success(f"Hello, {name}!")