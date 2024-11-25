import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import plotly.graph_objects as go
import plotly.express as px
from PIL import Image

# Load environment variables from .env
load_dotenv('../.env')
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


files_path = os.path.join(app_dir, 'files')

image_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(image_path)

cloud_path = os.path.join(files_path, 'cloud.png')
cloud_path_2 = os.path.join(files_path, 'cloud_2.png')
cloud = Image.open(cloud_path)
cloud_2 = Image.open(cloud_path_2)

# AWS credentials
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

# Format salary as € in thousands (k)
def format_salary(value):
    return f"{value / 1000:.0f}k €"


# Load the data
data = load_data()
max_extracted_date = data['extracted_date'].max()

st.set_page_config(page_title="YourFirstDataJob", page_icon="🎯", layout="wide")
st.sidebar.image(image_logo)
st.sidebar.markdown(f"### Last actualization: {max_extracted_date}")
st.sidebar.markdown("Created by [Eneko Eguiguren](https://www.linkedin.com/in/enekoegiguren/)")


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
    
# Title and Markdown
st.title(":blue[yourfirstdatajob]")
st.markdown("---")

# ---- Analysis Page Content ----
col1, col2, col3 = st.columns(3)
with col2:
    st.image(cloud, use_column_width=True)
    st.image(cloud_2, use_column_width=True)
st.markdown("---")

platform_columns = ['azure', 'aws', 'gcp']

for col in platform_columns:
    data[col] = data[col].apply(lambda x: 1 if x == 'Y' else 0)

platform_labels = ['AWS', 'Azure', 'GCP']
platform_counts = [sum(data['aws']), sum(data['azure']), sum(data['gcp'])]

cloud_rows = (data[['azure', 'aws', 'gcp']] == 1).any(axis=1)
cloud_rows_count = cloud_rows.sum()

perc_cloud_providers = cloud_rows_count / len(data) * 100 if len(data) > 0 else 0

display_big_metric("Jobs with cloud provided demanded", f"{perc_cloud_providers:.1f}%")




st.markdown("---")

col1, col2, col3 = st.columns(3)

max_platform_index = platform_counts.index(max(platform_counts))
platform_colors = ['#1f3a8d' if i == max_platform_index else '#4a90e2' for i in range(3)]  # Highlight the max with the same color

fig = go.Figure(data=[go.Pie(
    labels=platform_labels,
    values=platform_counts,
    textinfo='percent+label',  # Show percentage and label
    pull=[0.1, 0.1, 0.1],  # Pull slices slightly for emphasis
    marker=dict(colors=platform_colors),  # Apply the custom colors
    textfont=dict(size=18)  # Increase the text size
)])

# Customize layout for the pie chart
fig.update_layout(
    template="plotly_white",
    width=450,  
    height=400, 
    margin=dict(t=20, b=20, l=20, r=20) 
)

with col1:
    st.subheader("The most demanded platform")
    st.plotly_chart(fig)

# Clean and prepare data for experience and salary comparison
data['avg_experience'] = data['experience'].fillna(0).apply(lambda x: round(x))

# AWS Data
aws_data = data[data['aws'] == 1]
aws_data = aws_data.groupby(['aws']).agg(
    avg_experience=('avg_experience', 'mean'),
    avg_salary=('avg_salary', 'mean')
).reset_index()

# Azure Data
azure_data = data[data['azure'] == 1]
azure_data = azure_data.groupby(['azure']).agg(
    avg_experience=('avg_experience', 'mean'),
    avg_salary=('avg_salary', 'mean')
).reset_index()

# GCP Data
gcp_data = data[data['gcp'] == 1]
gcp_data = gcp_data.groupby(['gcp']).agg(
    avg_experience=('avg_experience', 'mean'),
    avg_salary=('avg_salary', 'mean')
).reset_index()

# Combine the data for sorting
platform_data = {
    'Platform': ['AWS', 'Azure', 'GCP'],
    'Average Salary': [aws_data['avg_salary'].iloc[0], azure_data['avg_salary'].iloc[0], gcp_data['avg_salary'].iloc[0]],
    'Average Experience': [aws_data['avg_experience'].iloc[0], azure_data['avg_experience'].iloc[0], gcp_data['avg_experience'].iloc[0]]
}

# Sort the data by Average Salary (from highest to lowest)
salary_sorted_data = sorted(zip(platform_data['Platform'], platform_data['Average Salary'], platform_data['Average Experience']),
                            key=lambda x: x[1], reverse=True)

# Extract sorted values
sorted_platforms, sorted_salaries, sorted_experiences = zip(*salary_sorted_data)

# Format salary values
salary_labels = [format_salary(s) for s in sorted_salaries]

# Salary bar chart in the second column
with col2:
    st.subheader("Salary")
    
    salary_fig = go.Figure(data=[go.Bar(
        x=sorted_platforms,
        y=sorted_salaries,
        text=salary_labels,  # Add formatted salary text labels
        textposition='auto',  # Automatically position the text on the bars
        marker=dict(color=['#1f3a8d' if i == 0 else '#4a90e2' for i in range(3)]),  # Highlight the max salary with the same color
    )])

    salary_fig.update_layout(
        template="plotly_white",
        width=450,
        height=400,
        title="Average Salary Comparison",
        xaxis_title="Platform",
        yaxis_title="Average Salary (€)",
        margin=dict(t=30, b=30, l=30, r=30)
    )
    st.plotly_chart(salary_fig)

# Experience bar chart in the third column
with col3:
    st.subheader("Experience")
    
    experience_labels = [f"{x:.1f}" for x in sorted_experiences]
    
    experience_fig = go.Figure(data=[go.Bar(
        x=sorted_platforms,
        y=sorted_experiences,
        text=experience_labels,  # Add formatted experience text labels
        textposition='auto',  # Automatically position the text on the bars
        marker=dict(color=['#1f3a8d' if i == 0 else '#4a90e2' for i in range(3)]),  # Highlight the max experience with the same color
    )])

    experience_fig.update_layout(
        template="plotly_white",
        width=450,
        height=400,
        title="Average Experience Comparison",
        xaxis_title="Platform",
        yaxis_title="Average Experience (Years)",
        margin=dict(t=30, b=30, l=30, r=30)
    )
    st.plotly_chart(experience_fig)


st.markdown("---")
st.write("## Temporal evolution")

if not data.empty and 'date_creation' in data.columns:
    data['date_creation'] = pd.to_datetime(data['date_creation'])
    filtered_data = data[data[platform_columns].any(axis=1)]
    cloud_over_time = (filtered_data.groupby(pd.Grouper(key='date_creation', freq='W'))[platform_columns]
                        .sum()
                        .reset_index())
    
    platform_counts = data[platform_columns].sum().sort_values(ascending=False)
    platform_long = cloud_over_time.melt(id_vars='date_creation', var_name='Cloud platform', value_name='Count')

    all_platforms = platform_columns  
    top_platforms = platform_counts.head(3).index.tolist()  
    platform_long_top = platform_long[platform_long['Cloud platform'].isin(top_platforms)]
    platform_long_top = platform_long_top[platform_long_top['Count'] > 0]
        
    if not platform_long_top.empty:

        fig_time_evolution = px.line(
            platform_long_top,
            x='date_creation',
            y='Count',
            color='Cloud platform',
            labels={'date_creation': 'Month', 'Count': 'Number of Listings'},
            line_shape='linear'
        )

        st.plotly_chart(fig_time_evolution)
    else:
        st.write("No data available for the selected skills for plotting.")
else:
    st.write("No data available for temporal evolution analysis.")
    