import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

# Set page config for full-width layout
st.set_page_config(page_title="Data stack", layout="wide")

# Load environment variables from .env
load_dotenv('../.env')
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
st.title("Data stack")
st.write("### Analyzing job listings to identify trends and correlations.")

# 1. Most Demanded Skills
st.write("## Top Demanded Skills")

if not data.empty:
   # filtered_data = data[data[skills_columns].any(axis=1)]  # Filter rows with at least one skill = 1
    skill_counts = data[skills_columns].sum().sort_values(ascending=False)


    # Option to select number of top skills to display
    top_n_options = st.selectbox("Select number of top skills to display:", options=["Top 10", "Top 20", "Top 30", "All"])
    
    if top_n_options == "Top 10":
        top_n_skills = skill_counts.head(10)
    elif top_n_options == "Top 20":
        top_n_skills = skill_counts.head(20)
    elif top_n_options == "Top 30":
        top_n_skills = skill_counts.head(30)
    else:
        top_n_skills = skill_counts  # "All"

    # Create bar chart for selected top skills
    fig = go.Figure(data=[go.Bar(
        x=top_n_skills.index,
        y=top_n_skills.values,
        text=top_n_skills.values,
        textposition='auto'
    )])
    
    fig.update_layout(
        xaxis_title="Skills",
        title=f"{top_n_options} Most Demanded Skills",
        template="plotly_white"
    )
    st.plotly_chart(fig)
else:
    st.write("No data available for the selected filters.")

if not data.empty:
# Filter for job category with default selection set to "Data Engineer"
    job_categories = data['job_category'].unique()
    default_category = "Data Engineer" if "Data Engineer" in job_categories else job_categories[0]
    selected_category = st.selectbox("Select Job Category", options=job_categories, index=list(job_categories).index(default_category))

    # 2. Filtered Top 10 Skills for Selected Job Category
    st.write(f"## Top 10 Skills for {selected_category} Category")

    if 'job_category' in data.columns:
        # Filter data based on selected job category
        filtered_data = data[data['job_category'] == selected_category]
        
        # Calculate skill counts for the filtered data
        filtered_skill_counts = filtered_data[skills_columns].sum().sort_values(ascending=False).head(10)
        
        # Display top 10 demanded skills for the selected job category
       # st.bar_chart(filtered_skill_counts)
           # Create bar chart for selected top skills
        fig = go.Figure(data=[go.Bar(
            x=filtered_skill_counts.index,
            y=filtered_skill_counts.values,
            text=filtered_skill_counts.values,
            textposition='auto'
        )])
        
        fig.update_layout(
            xaxis_title=f"Most demanded skills",
            title=f"{top_n_options} Most Demanded Skills for {selected_category}",
            template="plotly_white"
        )
        st.plotly_chart(fig)

else:      
    st.write("Job category data is not available for analysis.")
    

# 3. Correlation with Top Skills
st.write("## Correlation Between Top Skills")

if not data.empty:
    # Get the top 20 skills
    top_10_skills = skill_counts.head(5).index
    
    # Create a new DataFrame with only the top skills
    correlation_data = data[[*top_10_skills, 'job_category']].copy()
    
    # Convert job_category to numeric for correlation calculation
    correlation_data['job_category'] = pd.factorize(correlation_data['job_category'])[0]

    # Calculate the correlation matrix
    correlation_matrix = correlation_data.corr()

    # Create a heatmap for top skills using Plotly
    fig_correlation = px.imshow(
        correlation_matrix.loc[top_10_skills, top_10_skills],
        color_continuous_scale='RdBu',
        zmin=-1, zmax=1,
        title='Correlation Matrix of Top Skills',
        labels=dict(x="Skills", y="Skills"),
        aspect="auto"
    )
    st.plotly_chart(fig_correlation)


# Skill Ranking for Jobs
st.write("## Job Ranking by Selected Skill")

all_skills = skills_columns 
# Select a skill for job ranking
selected_skill = st.selectbox(
    'Select a Skill to Rank Jobs:',
    options=all_skills
)

# Skill Ranking for Jobs
st.write("## Job Ranking by Selected Skills")

# Select multiple skills for job ranking
selected_skills_for_ranking = st.multiselect(
    'Select Skills to Rank Jobs:',
    options=all_skills,
    default=top_10_skills  # Default to all top 10 skills selected
)

# Filter data for the selected skills
if selected_skills_for_ranking:
    # Create a boolean mask for any of the selected skills being present
    mask = data[selected_skills_for_ranking].any(axis=1)
    jobs_with_skills = data[mask]  # Filtered data with any of the selected skills

    # Count occurrences of job categories
    job_counts = jobs_with_skills['job_category'].value_counts().reset_index()
    job_counts.columns = ['Job Category', 'Count']  # Change 'Job Title' to 'Job Category'

    # Sort by Count
    job_counts = job_counts.sort_values(by='Count', ascending=False)

    # Display the job ranking
    #st.write("Job Categories that require any of the selected skills:")
  #  st.dataframe(job_counts)

    # Optionally, you can visualize the job counts
    fig_job_ranking = px.bar(
        job_counts,
        x='Job Category',
        y='Count',
        title='Job Counts for Selected Skills: {}'.format(', '.join(selected_skills_for_ranking)),
        labels={'Job Category': 'Job Category', 'Count': 'Number of Listings'},
        color='Count'
    )

    st.plotly_chart(fig_job_ranking)
    
# Temporal Evolution of Skills
st.write("## Temporal Evolution of Skills")

if not data.empty and 'date_creation' in data.columns:
    # Convert 'date_creation' to datetime format if it's not already
    data['date_creation'] = pd.to_datetime(data['date_creation'])

    # Filter to include only rows with at least one skill = 1
    filtered_data = data[data[skills_columns].any(axis=1)]

    # Create a DataFrame to count skills over time, grouping by month
    skills_over_time = (filtered_data.groupby(pd.Grouper(key='date_creation', freq='M'))[skills_columns]
                        .sum()
                        .reset_index())

    # Melt the DataFrame to long format for easier plotting
    skills_long = skills_over_time.melt(id_vars='date_creation', var_name='Skill', value_name='Count')

    # Get all skills for the multi-select
    all_skills = skills_columns  # Directly assign skills_columns if it's already a list
    top_10_skills = skill_counts.head(10).index.tolist()  # Convert to list for default selection

    # Multi-select for skills, allowing selection of all skills
    selected_skills = st.multiselect(
        'Select Skills to Display:',
        options=all_skills,
        default=top_10_skills  # Default to all top 10 skills selected
    )

    # Filter for selected skills
    skills_long_top10 = skills_long[skills_long['Skill'].isin(selected_skills)]

    # Remove entries where Count is 0
    skills_long_top10 = skills_long_top10[skills_long_top10['Count'] > 0]

    # Check if there are any remaining data points to plot
    if not skills_long_top10.empty:
        # Plot the temporal evolution
        fig_time_evolution = px.line(
            skills_long_top10,
            x='date_creation',
            y='Count',
            color='Skill',
            title='Temporal Evolution of Selected Skills (Monthly)',
            labels={'date_creation': 'Month', 'Count': 'Number of Listings'},
            line_shape='linear'
        )

        st.plotly_chart(fig_time_evolution)
    else:
        st.write("No data available for the selected skills for plotting.")
else:
    st.write("No data available for temporal evolution analysis.")




    
# Displaying all the results in a structured manner
st.write("### Summary")
st.write(f"The top demanded skills across all job categories have been displayed above. You can filter by job category to see the top skills required for that specific category.")