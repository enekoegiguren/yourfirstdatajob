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

profile_path = os.path.join(files_path, 'profile.png')
profile = Image.open(profile_path)



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



st.markdown(
    """
    <style>
    .rect-metric {
        padding: 20px;
        margin: 10px;
        border-radius: 8px;
        background-color: #f3f3f3;
        text-align: center;
        box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.1);
        font-size: 18px;
        font-weight: bold;
    }
    .rect-metric-title {
        font-size: 24px;
        margin-bottom: 5px;
        color: #333;
    }
    .rect-metric-value {
        font-size: 32px;
        color: #007bff;
    }
    .rect-metric-delta {
        font-size: 18px;
        margin-top: 5px;
        color: #28a745;
    }
    </style>
    """,
    unsafe_allow_html=True
)

def display_big_metric(title, value, delta=None):
    delta_html = f"<div class='rect-metric-delta'>{delta}</div>" if delta else ""
    st.markdown(
        f"""
        <div class='rect-metric'>
            <div class='rect-metric-title'>{title}</div>
            <div class='rect-metric-value'>{value}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True
    )
    
    
# Fetch data
data = load_data()

# Skills columns
skills_columns = [
    'sql', 'python', 'pyspark', 'etl', 'airflow', 'kafka', 'spark', 
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
# st.title("Your profile analysis")
# st.write("## Which profile are you ❓")
# st.markdown("---")

col1, col2, col3 = st.columns(3)
with col2:
    st.image(profile, use_column_width=True)
st.markdown("---")

if not data.empty:
    skill_counts = data[skills_columns].sum().sort_values(ascending=False)
    # Get the top 5 skills
    top_10_skills = skill_counts.head(5).index

# Skill Ranking for Jobs
st.write("## Job definition by selected skills")

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

    # Calculate the percentage of each job category
    total_jobs = job_counts['Count'].sum()  # Total number of jobs
    job_counts['Percentage'] = (job_counts['Count'] / total_jobs) * 100  # Calculate percentage

    # Display the top job category
    if not job_counts.empty:
        top_job_category = job_counts.iloc[0]
        
        # Show proficiency for the top job category
        proficiency_top_category = (jobs_with_skills['job_category'] == top_job_category['Job Category']).mean() * 100
        col1, col2 = st.columns(2)
        with col1:
            display_big_metric(f"Matched role:", f"{top_job_category['Job Category']}")
        with col2:
            display_big_metric(f"Match %:", f"{proficiency_top_category:.2f}%")
    # Visualize the job counts as percentages
    fig_job_ranking = px.bar(
        job_counts,
        x='Job Category',
        y='Percentage',  # Use percentage for the y-axis
        labels={'Job Category': 'Job Category', 'Percentage': 'Percentage of Listings (%)'},  # Update label for percentage
        color='Percentage',  # Color the bars based on the percentage
        text='Percentage'  # Add percentage text on top of each bar
    )

    # Customize layout to improve visibility of the text on bars
    fig_job_ranking.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    
    # Increase the height of the figure
    fig_job_ranking.update_layout(
        height=600  # Set the height to a larger value (adjust this as needed)
    )

    st.plotly_chart(fig_job_ranking)


# Radar Chart Section
st.write("## Top 8 Skills Demanded Per Job Category")
st.markdown("---")

# Filter data to include only jobs with at least one skill defined
has_at_least_one_skill = data[skills_columns].sum(axis=1) > 0
filtered_data = data[has_at_least_one_skill]

# Group by job_category and calculate percentages
skill_sums = filtered_data.groupby('job_category')[skills_columns].sum()
skill_percentages = skill_sums.div(skill_sums.sum(axis=1), axis=0) * 100

# Get the top 8 skills for each job category based on percentages
top_skills_per_category = {
    category: row.nlargest(8).index.tolist()
    for category, row in skill_percentages.iterrows()
}

# Job category selection
selected_job_category = st.selectbox(
    "Select a Job Category to view its top demanded skills:",
    options=skill_percentages.index,
    index=skill_percentages.index.get_loc(top_job_category['Job Category'])
)

# Create radar chart for the selected job category
if selected_job_category:
    top_skills = top_skills_per_category[selected_job_category]
    top_values = skill_percentages.loc[selected_job_category, top_skills].tolist()

    # Radar chart using Plotly
    fig_radar = px.line_polar(
        r=top_values + [top_values[0]],  # Close the radar chart loop
        theta=top_skills + [top_skills[0]],  # Repeat the first skill for closure
        line_close=True,
        title=f"Top 8 Skills for {selected_job_category.capitalize()} (in %)",
        markers=True,
        template="plotly",
    )
    fig_radar.update_traces(fill='toself', line_color='blue')
    fig_radar.update_layout(height=600)  # Adjust height if needed

    # Display radar chart
    st.plotly_chart(fig_radar)
