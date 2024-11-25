import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import seaborn as sns

from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt


# Load environment variables from .env
load_dotenv('../.env')
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
files_path = os.path.join(app_dir, 'files')

image_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(image_path)

data_stack_path = os.path.join(files_path, 'data_stack.png')
data_stack_path_2 = os.path.join(files_path, 'data_stack_2.png')
data_stack = Image.open(data_stack_path)
data_stack_2 = Image.open(data_stack_path_2)



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

#with col2:
st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")

col1, col2, col3 = st.columns(3)
with col2:
    st.image(data_stack, use_column_width=True)
    st.image(data_stack_2, use_column_width=True)


st.markdown("---")

if not data.empty:
    rows_with_skill = data[skills_columns].any(axis=1).sum()
    perc_rows_with_skill = rows_with_skill / len(data) *100

    
    # filtered_data = data[data[skills_columns].any(axis=1)]  # Filter rows with at least one skill = 1
    skill_counts = data[skills_columns].sum().sort_values(ascending=False)
    
    top_skills = skill_counts.head(3).index.tolist() if not skill_counts.empty else []
    top_skill_1 = top_skills[0] if len(top_skills) > 0 else None
    top_skill_2 = top_skills[1] if len(top_skills) > 1 else None
    top_skill_3 = top_skills[2] if len(top_skills) > 2 else None
    perc_jobs_with_skill_1 = (data[top_skill_1].sum() / len(data)) * 100
    perc_jobs_with_skill_2 = (data[top_skill_2].sum() / len(data)) * 100
    perc_jobs_with_skill_3 = (data[top_skill_3].sum() / len(data)) * 100
    
    #Insights
    if perc_rows_with_skill is not None:
        col1, col2, col3, col4 = st.columns(4)
    
        with col1:
            display_big_metric("Job % with skills demanded", f"{perc_rows_with_skill:.0f}%")
        with col2:
            display_big_metric("Most demanded skill", f"{top_skill_1}: {perc_jobs_with_skill_1:.0f}%")
        with col3:
             display_big_metric("Second skill", f"{top_skill_2}: {perc_jobs_with_skill_2:.0f}%")
        with col4:
             display_big_metric("Third skill", f"{top_skill_3}: {perc_jobs_with_skill_3:.0f}%")
           # st.header(f" 📊 Insights: :blue[ Job % with skills demanded: {perc_rows_with_skill:.1f}%]")
    

    


    st.markdown("---")

    # Option to select number of top skills to display
    top_n_options = st.selectbox("Select number of top skills to display:", options=["Top 10", "Top 20", "Top 30", "All"])

    # Determine the top N skills based on user selection
    if top_n_options == "Top 10":
        top_n_skills = skill_counts.head(10)
    elif top_n_options == "Top 20":
        top_n_skills = skill_counts.head(20)
    elif top_n_options == "Top 30":
        top_n_skills = skill_counts.head(30)
    else:
        top_n_skills = skill_counts  # "All"

    # Calculate percentages for the selected top N skills
    perc_top_n_skills = (top_n_skills.values / len(data)) * 100  # Calculate percentages for the skills

    # Create a Plotly bar chart for the selected top skills
    fig = go.Figure(data=[go.Bar(
        x=top_n_skills.index,
        y=perc_top_n_skills,  # Use percentages for the y-axis
        text=[f"{perc:.0f}%" for perc in perc_top_n_skills],  # Display percentage in the text
        textposition='auto'
    )])

    fig.update_layout(
        xaxis_title="Skills",
        yaxis_title="Percentage",
        title=f"{top_n_options} most demanded skills",
        template="plotly_white"
    )

    st.plotly_chart(fig)

else:
    st.write("No data available for the selected filters.")

st.markdown("---")
if not data.empty:
    # Filter for job category with default selection set to "Data Engineer"
    job_categories = data['job_category'].unique()
    default_category = "Data Engineer" if "Data Engineer" in job_categories else job_categories[0]
    st.write("## Top 10 skills depending on the job category")
    selected_category = st.selectbox("Select job category", options=job_categories, index=list(job_categories).index(default_category))

    if 'job_category' in data.columns:
        # Filter data based on the selected job category
        filtered_data = data[data['job_category'] == selected_category]

        # Calculate skill counts for the filtered data
        filtered_skill_counts = filtered_data[skills_columns].sum().sort_values(ascending=False).head(10)

        # Calculate the total count of skills for the selected job category
        total_skill_count = filtered_skill_counts.sum()

        # Calculate the percentage for each skill
        skill_percentages = (filtered_skill_counts / total_skill_count) * 100

        # Create a Plotly bar chart for the selected top skills
        fig = go.Figure(data=[go.Bar(
            x=filtered_skill_counts.index,
            y=skill_percentages,  # Use skill percentages for the y-axis
            text=[f"{percentage:.1f}%" for percentage in skill_percentages],  # Display percentages
            textposition='auto'
        )])

        fig.update_layout(
            xaxis_title="Most Demanded Skills",
            yaxis_title="Percentage",
            title=f"Top 10 most demanded skills for {selected_category}",
            template="plotly_white"
        )
        st.plotly_chart(fig)

else:      
    st.write("Job category data is not available for analysis.")
    
st.markdown("---")
# Temporal Evolution of Skills
st.write("## Temporal evolution of skills")

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
        'Select skills to display:',
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
            #title='Temporal evolution of Selected Skills (Monthly)',
            labels={'date_creation': 'Month', 'Count': 'Number of Listings'},
            line_shape='linear'
        )

        st.plotly_chart(fig_time_evolution)
    else:
        st.write("No data available for the selected skills for plotting.")
else:
    st.write("No data available for temporal evolution analysis.")

st.markdown("---")

# Correlation with Top Skills
st.write("## Correlation between top skills")

if not data.empty:
    # Get the top 20 skills
    top_10_skills = skill_counts.head(20).index

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

    # Display the heatmap
    st.plotly_chart(fig_correlation)

    # Display insights based on the correlation matrix
    st.write("### 📊 Insights")

    # Extract significant correlations
    significant_correlations = correlation_matrix.loc[top_10_skills, top_10_skills].stack().reset_index()
    significant_correlations.columns = ['Skill 1', 'Skill 2', 'Correlation']

    # Filter for strong positive or negative correlations, excluding self-correlations
    strong_correlations = significant_correlations[
        (significant_correlations['Correlation'] > 0.4) | 
        (significant_correlations['Correlation'] < -0.5)
    ]
    strong_correlations = strong_correlations[strong_correlations['Skill 1'] != strong_correlations['Skill 2']]  # Exclude self-correlations

    # Remove duplicate pairs by ensuring Skill 1 < Skill 2
    strong_correlations = strong_correlations[
        strong_correlations['Skill 1'] < strong_correlations['Skill 2']
    ]

    # Display strong correlations
    if not strong_correlations.empty:
        for _, row in strong_correlations.iterrows():
            st.write(f"🔗 **{row['Skill 1']}** and **{row['Skill 2']}** have a correlation of **{row['Correlation']:.2f}**")
    else:
        st.write("🚫 No strong correlations found among the top skills.")
else:      
    st.write("❌ Data is not available for correlation analysis.")


    




    