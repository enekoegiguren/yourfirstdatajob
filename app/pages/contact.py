import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from dotenv import load_dotenv
import os
import re
from PIL import Image

# Load environment variables from .env
load_dotenv('../.env')
current_dir = os.path.dirname(__file__)
image_path = os.path.join(current_dir, 'logo.png')
image_logo = Image.open(image_path)



# Access the environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_PREFIX = os.getenv('FILE_PREFIX')

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

#@st.cache_data
def get_latest_file():
    # List files in the bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FILE_PREFIX)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # Extract dates from filenames and sort them to find the latest file
    date_pattern = re.compile(r'jobdata_(\d{8})\.parquet')
    files_with_dates = [(f, date_pattern.search(f).group(1)) for f in files if date_pattern.search(f)]
    latest_file = max(files_with_dates, key=lambda x: x[1])[0]  # Get the file with the latest date

    return latest_file

#@st.cache_data
def load_data():
    # Get the latest file and read it as a Parquet file into a DataFrame
    latest_file_key = get_latest_file()
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    data = pd.read_parquet(BytesIO(obj['Body'].read()))
    return data

data = load_data()
max_extracted_date = data['extracted_date'].max()

st.set_page_config(page_title="YourFirstDataJob", page_icon="🎯",layout="wide")
st.sidebar.image(image_logo)
st.sidebar.markdown(f"### Last actualization: {max_extracted_date}")
st.sidebar.markdown("Created by [Eneko Eguiguren](https://www.linkedin.com/in/enekoegiguren/)")


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


