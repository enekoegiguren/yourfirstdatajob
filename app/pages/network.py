import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import plotly.express as px
from PIL import Image


# Load environment variables from .env
load_dotenv('../.env')

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
files_path = os.path.join(app_dir, 'files')
image_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(image_path)

network_path = os.path.join(files_path, '7_network.png')
network = Image.open(network_path)

network_1_path = os.path.join(files_path, 'network_1.png')
network_1 = Image.open(network_1_path)



AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_PREFIX = os.getenv('FILE_PREFIX')

# Set up the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Helper functions
def get_latest_file():
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FILE_PREFIX)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    date_pattern = re.compile(r'jobdata_(\d{8})\.parquet')
    files_with_dates = [(f, date_pattern.search(f).group(1)) for f in files if date_pattern.search(f)]
    latest_file = max(files_with_dates, key=lambda x: x[1])[0]
    return latest_file

def load_data():
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

col1, col2, col3 = st.columns(3)
with col2:
    st.image(network, use_column_width=True)
    
st.image(network_1_path, use_column_width=True)
    

# LinkedIn link
col1, col2 = st.columns(2)
with col1:
    st.header("Network français:")
    robin = "https://www.linkedin.com/in/robin-conquet-3a510292/"  
    podcast_data_gen = "https://open.spotify.com/show/27XP61URSuKu9oeWR793D6"
    st.markdown(f"[Podcast DataGen]({podcast_data_gen})", unsafe_allow_html=True)
    st.markdown(f"[Robin Conquet]({robin})", unsafe_allow_html=True)


    b_fotzo = "https://www.linkedin.com/in/bricefotzo/"  
    st.markdown(f"[Brice Fotzo]({b_fotzo})", unsafe_allow_html=True)

    j_milpied = "https://www.linkedin.com/in/jeanmilpied/"  
    st.markdown(f"[Jean Milpied]({j_milpied})", unsafe_allow_html=True)

    florent = "https://www.linkedin.com/in/florent-j-93716410b/"  
    st.markdown(f"[Florent J.]({florent})", unsafe_allow_html=True)

    w_nana = "https://www.linkedin.com/in/willis-nana/"  
    st.markdown(f"[Willis Nana]({w_nana})", unsafe_allow_html=True)
    
with col2:
    st.header("Network international:")

    d_freitag = "https://www.linkedin.com/in/davidkfreitag/"  
    st.markdown(f"[David Freitag]({d_freitag})", unsafe_allow_html=True)

    a_kretz = "https://www.linkedin.com/in/andreas-kretz/"  
    st.markdown(f"[Andreas Kretz]({a_kretz})", unsafe_allow_html=True)

    p_labarta = "https://www.linkedin.com/in/pau-labarta-bajo-4432074b/"  
    st.markdown(f"[Pau Labarta]({p_labarta})", unsafe_allow_html=True)








    