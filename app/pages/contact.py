import streamlit as st
from dotenv import load_dotenv
import os
from PIL import Image

load_dotenv('../.env')
current_dir = os.path.dirname(__file__)
image_path = os.path.join(current_dir, 'logo.png')
image_logo = Image.open(image_path)

st.set_page_config(page_title="YourFirstDataJob", page_icon="🎯",layout="wide")
st.sidebar.image(image_logo)


st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")


# Set page title
st.title("Contact Me")

st.write("""
        Do you want data used on this app for your analysis or any advice for your data career? Feel free to contact me!
        """)


# LinkedIn link
linkedin_url = "https://www.linkedin.com/in/eneko"  # Replace with your LinkedIn URL
st.markdown(f"[Connect with me on LinkedIn]({linkedin_url})", unsafe_allow_html=True)

personal_page = "https://eeguiguren.wordpress.com/"  # Replace with your LinkedIn URL
st.markdown(f"[Connect with me on my personal page]({personal_page})", unsafe_allow_html=True)


