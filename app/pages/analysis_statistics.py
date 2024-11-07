import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
from io import BytesIO
from dotenv import load_dotenv

from PIL import Image
import os
import re

# Set page config for full width layout
st.set_page_config(page_title="Job Opportunities Analysis", layout="wide")

# Load environment variables from .env
load_dotenv('../.env')
current_dir = os.path.dirname(__file__)
image_path = os.path.join(current_dir, 'logo.png')
image_logo = Image.open(image_path)

st.sidebar.image(image_logo)


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

def get_latest_file():
    # List files in the bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FILE_PREFIX)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # Extract dates from filenames and sort them to find the latest file
    date_pattern = re.compile(r'jobdata_(\d{8})\.parquet')
    files_with_dates = [(f, date_pattern.search(f).group(1)) for f in files if date_pattern.search(f)]
    latest_file = max(files_with_dates, key=lambda x: x[1])[0]  # Get the file with the latest date

    return latest_file

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

data = data[data['experience'].notnull()]
data['experience'] = data['experience'].astype(int) 
data = data[data['experience'] > 0]

#with col2:
st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")

# ---- Analysis Page Content ----
st.title("Statistics")

st.write("## Experience and salary by job .. ❓ ")

st.markdown("---")


# ---- Experience Analysis ----
st.write("## Experience by Job")
data['experience_rounded'] = data['experience'].round().astype(int)  # Round experience for cleaner visuals


if not data.empty:
    average_experience = data['experience'].mean() if not data['experience'].isnull().all() else None
    st.write(f"### The :blue[average experience] needed: :blue[{average_experience:.1f} years]")
    # Create box plot
    fig_exp_box = px.box(
        data,
        x='job_category',
        y='experience_rounded',
        points=False,  # Disable outlier points
        #title="Experience Requirement by Job Category (Without Outliers)",
        labels={'experience_rounded': 'Years of Experience', 'job_category': 'Job Category'}
    )
    st.plotly_chart(fig_exp_box)

    # Summary table for experience (transposed, only average)
    exp_summary = data.groupby('job_category')['experience_rounded'].mean().reset_index()
    exp_summary.columns = ['Job Category', 'Average Experience']
    exp_summary['Average Experience'] = exp_summary['Average Experience'].astype(int)  # Convert to int for display
    exp_summary = exp_summary.set_index('Job Category').T  # Transpose the summary


    # Insights for experience
    most_experience_job = exp_summary.loc['Average Experience'].idxmax()
    least_experience_job = exp_summary.loc['Average Experience'].idxmin()
    
    st.subheader(f"The job with the: ")
    
    st.subheader(f"⬆️ :blue[most years of experience] required is :blue[**{most_experience_job}**].")
    st.subheader(f"⬇️ :blue[least years of experience] required is :blue[**{least_experience_job}**].")
    st.table(exp_summary)



st.markdown("---")
data = data[data['max_salary'] < 200000]
# ---- Salary Analysis by Job ----


st.write("## Salary by Job")
data['avg_salary_rounded'] = data['avg_salary'].round()  # Round average salary

if not data.empty:
    avg_salary = data['avg_salary'].mean() if not data['avg_salary'].isnull().all() else None
    st.write(f"### 💰 **Average salary:** :blue[{format_salary(avg_salary)}]")
    # Create box plot
    fig_salary_job_box = px.box(
        data,
        x='job_category',
        y='avg_salary_rounded',
        points=False,
       # title="Salary Distribution by Job Category (Without Outliers)",
        labels={'avg_salary_rounded': 'Average Salary (€)', 'job_category': 'Job Category'}
    )
    st.plotly_chart(fig_salary_job_box)

    # Summary table for salary by job category (transposed, only average)
    salary_summary = data.groupby('job_category')['avg_salary_rounded'].mean().reset_index()
    salary_summary.columns = ['Job Category', 'Average Salary (€)']
    salary_summary['Average Salary (€)'] = salary_summary['Average Salary (€)'].apply(lambda x: f"{int(x):,}")  # Format salary
    salary_summary = salary_summary.set_index('Job Category').T  # Transpose the summary

    # Insights for salary
    highest_salary_job = salary_summary.loc['Average Salary (€)'].idxmax()
    lowest_salary_job = salary_summary.loc['Average Salary (€)'].idxmin()
    st.subheader(f"The job with the: ")
    st.subheader(f"⬆️ :blue[highest average salary] is :blue[**{highest_salary_job}**].")
    st.subheader(f"⬇️ :blue[lowest average salary] is :blue[**{lowest_salary_job}**].")
    st.table(salary_summary)



st.markdown("---")
# ---- Salary Analysis by Years of Experience ----
st.write("## Salary by Years of Experience")
if not data.empty:
    # Create box plot
    fig_salary_exp = px.box(
        data,
        x='experience_rounded',
        y='avg_salary_rounded',
        points=False,
        #title="Salary by Years of Experience (Without Outliers)",
        labels={'experience_rounded': 'Years of Experience', 'avg_salary_rounded': 'Average Salary (€)'}
    )
    st.plotly_chart(fig_salary_exp)

    # Summary table for salary by years of experience (transposed, only average)
    exp_salary_summary = data.groupby('experience_rounded')['avg_salary_rounded'].mean().reset_index()
    exp_salary_summary.columns = ['Years of Experience', 'Average Salary (€)']
    exp_salary_summary['Average Salary (€)'] = exp_salary_summary['Average Salary (€)'].apply(lambda x: f"{int(x):,}")  # Format salary
    exp_salary_summary = exp_salary_summary.set_index('Years of Experience').T  # Transpose the summary
    st.write("📈 **Average Salary by Years of Experience:**")
    st.table(exp_salary_summary)
