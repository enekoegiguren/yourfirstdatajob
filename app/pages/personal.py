import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import plotly.express as px
from PIL import Image


st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")

# Load environment variables from .env
load_dotenv('../.env')

current_dir = os.path.dirname(__file__)
image_path = os.path.join(current_dir, 'logo.png')
image_logo = Image.open(image_path)

st.sidebar.image(image_logo)


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

# Fetch data
data = load_data()

# Skills columns
skills_columns = [
    'sql', 'python', 'pyspark', 'azure', 'aws', 'gcp', 'etl', 'airflow', 'kafka', 'spark', 
    'power_bi', 'tableau', 'snowflake', 'docker', 'kubernetes', 'git', 'data_warehouse', 
    'hadoop', 'mlops', 'data_lake', 'bigquery', 'databricks', 'dbt', 'mlflow', 'java', 
    'scala', 'sas', 'matlab', 'power_query', 'looker', 'apache', 'hive', 'terraform', 
    'jenkins', 'gitlab', 'machine_learning', 'deep_learning', 'nlp', 'api', 'pipeline', 
    'data_governance', 'erp', 'ssis', 'ssas', 'ssrs', 'ssms', 'postgre', 'mysql', 'mongodb', 
    'cloud', 'synapse', 'blobstorage', 'azure_devops', 'fabric', 'glue', 'redshift', 's3', 
    'lambda', 'emr', 'athena', 'kinesis', 'rds', 'sagemaker'
]

# Convert 'Y'/'N' to binary values (1 for Y, 0 for N)
for col in skills_columns:
    data[col] = data[col].apply(lambda x: 1 if x == 'Y' else 0)

# Streamlit App Layout
st.title("Your profile analysis")
st.write("## Which profile are you ❓")
st.markdown("---")


if not data.empty:
    skill_counts = data[skills_columns].sum().sort_values(ascending=False)
    # Get the top 5 skills
    top_10_skills = skill_counts.head(5).index

# Skill Ranking for Jobs
st.write("## Job Ranking by Selected Skill")

all_skills = skills_columns 

# Select multiple skills for job ranking
selected_skills_for_ranking = st.multiselect(
    'Select the skills you have or want:',
    options=all_skills,
    default=top_10_skills  # Default to all top 10 skills selected
)

# Select a skill for proficiency calculation
selected_skill = st.selectbox(
    'Order the skills with:',
    options=all_skills
)

# Calculate proficiency percentage
if selected_skill:
    proficiency = skill_counts[selected_skill] / len(data) * 100 if len(data) > 0 else 0
    

# Filter data for the selected skills
if selected_skills_for_ranking:
    # Create a boolean mask for any of the selected skills being present
    mask = data[selected_skills_for_ranking].any(axis=1)  # Ensure there's at least one skill (1)

    # Filter jobs where at least one of the selected skills is present
    jobs_with_skills = data[mask]

    # Only count jobs with at least one skill (no all-zero rows)
    job_counts = jobs_with_skills['job_category'].value_counts().reset_index()
    job_counts.columns = ['Job Category', 'Count']  # Change 'Job Title' to 'Job Category'

    # Sort by Count
    job_counts = job_counts.sort_values(by='Count', ascending=False)

    # Display the top job category
    if not job_counts.empty:
        top_job_category = job_counts.iloc[0]
        
        st.write("### 📊 Insights")
        
        # Show proficiency for the top job category
        proficiency_top_category = (jobs_with_skills['job_category'] == top_job_category['Job Category']).mean() * 100
        st.write(f"#### You have a {proficiency_top_category:.2f}% match for the role: {top_job_category['Job Category']}")

    # Visualize the job counts
    fig_job_ranking = px.bar(
        job_counts,
        x='Job Category',
        y='Count',
        title='Job Counts for Selected Skills: {}'.format(', '.join(selected_skills_for_ranking)),
        labels={'Job Category': 'Job Category', 'Count': 'Number of Listings'},
        color='Count'
    )

    st.plotly_chart(fig_job_ranking)

