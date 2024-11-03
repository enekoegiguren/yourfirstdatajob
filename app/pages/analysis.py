import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
from io import BytesIO
from dotenv import load_dotenv
import os
import re

# Load environment variables from .env
load_dotenv('../.env')

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

# Format salary as € in thousands (k)
def format_salary(value):
    return f"{value / 1000:.0f}k €"

# Fetch data
data = load_data()

data = data[data['max_salary'] < 200000]

# ---- Analysis Page Content ----
st.title("Job Opportunities Analysis - Detailed")


# ---- Experience Analysis ----
st.write("### Experience by Job")
data['experience_rounded'] = data['experience'].round().astype(int)  # Round experience for cleaner visuals

if not data.empty:
    fig_exp_box = px.box(
        data,
        x='job_category',
        y='experience_rounded',
        points=False,  # Disable outlier points
        title="Experience Requirement by Job Category (Without Outliers)",
        labels={'experience_rounded': 'Years of Experience', 'job_category': 'Job Category'}
    )
    st.plotly_chart(fig_exp_box)
else:
    st.write("No data available for Experience Analysis.")

# ---- Salary Analysis by Job ----
st.write("### Salary by Job")
data['avg_salary_rounded'] = data['avg_salary'].round()  # Round average salary

if not data.empty:
    # Box plot for salary distribution by job category
    fig_salary_job_box = px.box(
        data,
        x='job_category',
        y='avg_salary_rounded',
        points=False,  # Disable outlier points
        title="Salary Distribution by Job Category (Without Outliers)",
        labels={'avg_salary_rounded': 'Average Salary (€)', 'job_category': 'Job Category'}
    )
    st.plotly_chart(fig_salary_job_box)
else:
    st.write("No data available for Salary Analysis by Job.")

# ---- Salary Analysis by Years of Experience ----
st.write("### Salary by Years of Experience")
if not data.empty:
    fig_salary_exp = px.box(
        data,
        x='experience_rounded',
        y='avg_salary_rounded',
        points=False,  # Disable outlier points
        title="Salary by Years of Experience (Without Outliers)",
        labels={'experience_rounded': 'Years of Experience', 'avg_salary_rounded': 'Average Salary (€)'}
    )
    st.plotly_chart(fig_salary_exp)
else:
    st.write("No data available for Salary Analysis by Experience.")

# ---- Scatter Plot for Rounded Data ----
st.write("### Salary by Job Category and Experience")
# Group by job category and experience level, and calculate average salary to smooth out the data
grouped_data = data.groupby(['job_category', 'experience_rounded']).agg(avg_salary=('avg_salary_rounded', 'mean')).reset_index()

if not grouped_data.empty:
    fig_salary_exp_scatter = px.scatter(
        grouped_data,
        x='experience_rounded',
        y='avg_salary',
        color='job_category',
        size='avg_salary',
        title="Average Salary by Experience and Job Category",
        labels={'experience_rounded': 'Years of Experience', 'avg_salary': 'Average Salary (€)', 'job_category': 'Job Category'},
        color_discrete_sequence=px.colors.qualitative.Plotly
    )
    st.plotly_chart(fig_salary_exp_scatter)
else:
    st.write("No data available for Salary Analysis by Job Category and Experience.")
