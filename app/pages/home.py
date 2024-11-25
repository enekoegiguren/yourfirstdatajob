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
from datetime import datetime

# Load environment variables from .env
load_dotenv('../.env')
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


files_path = os.path.join(app_dir, 'files')

logo_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(logo_path)

slogan_path = os.path.join(files_path, '1_datamarket.png')
slogan = Image.open(slogan_path)
steps_path = os.path.join(files_path, '2_steps.png')
steps = Image.open(steps_path)
market_trends_path = os.path.join(files_path, '3_market_trends.png')
market_trends = Image.open(market_trends_path)
education_path = os.path.join(files_path, '4_education.png')
education = Image.open(education_path)
impostor_path = os.path.join(files_path, '5_impostor.png')
impostor = Image.open(impostor_path)
impostor_quest_path = os.path.join(files_path, '6_imp_questions.png')
impostor_quest = Image.open(impostor_quest_path)
network_path = os.path.join(files_path, '7_network.png')
network = Image.open(network_path)

network_1_path = os.path.join(files_path, 'network_1.png')
network_1 = Image.open(network_1_path)



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


data = load_data()

data = data[(data['year'] > 2023) & (data['month'] > 5)]
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



max_extracted_date = data['extracted_date'].max()

st.set_page_config(page_title="YourFirstDataJob", page_icon="🎯",layout="wide")
st.sidebar.image(image_logo)
st.sidebar.markdown(f"### Last Actualization: {max_extracted_date}")
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


st.title("""
        :blue[yourfirstdatajob]
        """)


st.markdown("---")
col1, col2 = st.columns(2)
with col1:
    st.image(slogan, use_column_width=True)
with col2:
    st.image(steps, use_column_width=True)

    
st.header(""" 
            Land _Your First Data Job_ or Level Up Your Career with :blue[Real Market Data & Insights!]
            """)
st.markdown("---")
st.subheader("""
    Daily data analysis from France Travail API gives you an edge 👇
            """)
st.subheader("""
    Before another Bootcamp or Udemy Course, _take the next step with real data insights_ to :blue[boost your career!]
            """)


### MARKET TRENDS
st.markdown("---")

col1, col2, col3 = st.columns(3)
with col2:
    st.image(market_trends, use_column_width=True)
col1, col2 = st.columns(2)
with col1:
    number_of_jobs = len(data)
    jobs_with_salary = len(data[data['avg_salary'].notnull()])
    jobs_with_experience = len(data[data['experience_bool'] != 'N'])

    percent_with_salary = (jobs_with_salary / number_of_jobs) * 100 if number_of_jobs > 0 else 0
    percent_with_experience = (jobs_with_experience / number_of_jobs) * 100 if number_of_jobs > 0 else 0
    display_big_metric("Number of jobs analyzed", number_of_jobs)
    display_big_metric("Jobs with Salary Specified", f"{percent_with_salary:.1f}%")
    display_big_metric("Jobs with Experience Specified", f"{percent_with_experience:.1f}%")
    
    
with col2:

    df = data.copy()
    

    df['date_creation'] = pd.to_datetime(df['date_creation'])
    df['extracted_date'] = pd.to_datetime(df['extracted_date'])

    df['effective_date'] = df.apply(
        lambda row: row['date_creation'] if row['extracted_date'] <= pd.Timestamp('2024-11-04') else row['extracted_date'], 
        axis=1
    )

    one_month_ago = pd.Timestamp.now() - pd.DateOffset(months=1)
    df_filtered = df[df['effective_date'] >= one_month_ago]

    if df_filtered.empty:
        st.write("No job data available for the last month.")
    else:
        job_counts = df_filtered.groupby('effective_date').size().reset_index(name='number_of_jobs')

        fig = px.line(
            job_counts, 
            x='effective_date', 
            y='number_of_jobs',
            title="Evolution of the jobs extracted",
            labels={'effective_date': 'Date', 'number_of_jobs': 'Number of Jobs'},
            markers=True
        )

        fig.update_traces(
            line=dict(color='blue'),
            fill='tozeroy',
            mode='lines+markers'
        )
        fig.update_layout(height=500)

        st.plotly_chart(fig)
        
col1, col2 = st.columns(2)

with col1:
    st.write("### Most demanded job categories")

    if not data.empty:

        most_demanded_jobs = (
            data['job_category']
            .value_counts()
            .sort_values(ascending=False)
            .head(10)
        )


        total_jobs = most_demanded_jobs.sum()  
        most_demanded_jobs_percentage = (most_demanded_jobs / total_jobs) * 100
        most_demanded_jobs_percentage = most_demanded_jobs_percentage.sort_values(ascending=True)


        fig = go.Figure(data=[
            go.Bar(
                x=most_demanded_jobs_percentage.values,  
                y=most_demanded_jobs_percentage.index,  
                orientation='h', 
                text=[f'{value:.0f}%' for value in most_demanded_jobs_percentage.values], 
                textposition='auto',
                textfont=dict(size=22)
            )
        ])

        fig.update_layout(

            xaxis_title="Job Category",
            yaxis_title="Percentage of Jobs (%)",
            template="plotly_white",
            bargap = 0.05,
            height = 600
        )

        st.plotly_chart(fig)
    
with col2:

    st.write("### Job Locations Map")
    if not data.empty:
        job_counts = data.groupby(['latitude', 'longitude']).size().reset_index(name='job_count')
        fig = px.scatter_mapbox(
                job_counts,
                lat="latitude",
                lon="longitude",
                size="job_count",
                color_continuous_scale=px.colors.cyclical.IceFire,
                size_max=15,
                zoom=5,
                mapbox_style="carto-positron",
        )
        fig.update_layout(
                autosize=False,
                width=1000,
                height=700
        )
        st.plotly_chart(fig)
    else:
        st.write("No data available to display on the map.")


### EDUCATION


st.markdown("---")
 
col1, col2, col3 = st.columns(3)

with col2:
    st.image(education, use_column_width=True)
    
    
rows_with_skill = data[skills_columns].any(axis=1).sum()
perc_rows_with_skill = rows_with_skill / len(data) *100
skill_counts = data[skills_columns].sum().sort_values(ascending=False)
top_skills = skill_counts.head(3).index.tolist() if not skill_counts.empty else []
top_skill_1 = top_skills[0] if len(top_skills) > 0 else None
top_skill_2 = top_skills[1] if len(top_skills) > 1 else None
top_skill_3 = top_skills[2] if len(top_skills) > 2 else None
perc_jobs_with_skill_1 = (data[top_skill_1].sum() / len(data)) * 100
perc_jobs_with_skill_2 = (data[top_skill_2].sum() / len(data)) * 100
perc_jobs_with_skill_3 = (data[top_skill_3].sum() / len(data)) * 100
    

col_1, col_2 = st.columns(2)
    
with col_1:
    display_big_metric("Job % with skills demanded", f"{perc_rows_with_skill:.0f}%")
    
    #Insights
    if perc_rows_with_skill is not None:
        col1, col2, col3 = st.columns(3)
        with col1:
            display_big_metric("Most demanded skill", f"{top_skill_1}: {perc_jobs_with_skill_1:.0f}%")
        with col2:
             display_big_metric("Second skill", f"{top_skill_2}: {perc_jobs_with_skill_2:.0f}%")
        with col3:
             display_big_metric("Third skill", f"{top_skill_3}: {perc_jobs_with_skill_3:.0f}%")
    st.write(" ")
    


with col_2:
    st.write("## Temporal evolution of skills")

    if not data.empty and 'date_creation' in data.columns:

        data['date_creation'] = pd.to_datetime(data['date_creation'])


        filtered_data = data[data[skills_columns].any(axis=1)]


        skills_over_time = (filtered_data.groupby(pd.Grouper(key='date_creation', freq='M'))[skills_columns]
                            .sum()
                            .reset_index())


        skills_long = skills_over_time.melt(id_vars='date_creation', var_name='Skill', value_name='Count')

        all_skills = skills_columns  
        top_10_skills = skill_counts.head(10).index.tolist()  
        skills_long_top10 = skills_long[skills_long['Skill'].isin(top_10_skills)]
        skills_long_top10 = skills_long_top10[skills_long_top10['Count'] > 0]
        
        if not skills_long_top10.empty:

            fig_time_evolution = px.line(
                skills_long_top10,
                x='date_creation',
                y='Count',
                color='Skill',
                labels={'date_creation': 'Month', 'Count': 'Number of Listings'},
                line_shape='linear'
            )

            st.plotly_chart(fig_time_evolution)
        else:
            st.write("No data available for the selected skills for plotting.")
    else:
        st.write("No data available for temporal evolution analysis.")
    

### IMPOSTOR


st.markdown("---")
    
col1, col2, col3 = st.columns(3)

with col2:
    st.image(impostor, use_column_width=True)
    
col_1, col_2 = st.columns(2)

with col_1:
    st.image(impostor_quest, use_column_width=True)

with col_2:

    skill_counts = data[skills_columns].sum().sort_values(ascending=False)

    if not data.empty:
        st.write("Tell Us About Yourself")

        # Skill selection for ranking
        all_skills = skills_columns
        top_5_skills = skill_counts.head(5).index
        selected_skills = st.multiselect(
            "Select the skills you have or want to evaluate:",
            options=all_skills,
            default=top_5_skills
        )

        # Add experience filter with slider (default range 0-2 years)
        experience_range = st.slider(
            "Select the range of experience (in years):",
            min_value=int(data['experience'].min()),
            max_value=int(data['experience'].max()),
            value=(0, 2),  # Set default value to (0, 2) years
            step=1
        )
        st.write(f"Selected Experience Range: {experience_range[0]} - {experience_range[1]} years")

        # Filter data by experience range
        filtered_data = data[(data['experience'] >= experience_range[0]) & (data['experience'] <= experience_range[1]) & (data['avg_salary']>0)]

        if selected_skills:
            # Create a boolean mask for any of the selected skills being present
            mask = filtered_data[selected_skills].any(axis=1)

            # Filter jobs where at least one of the selected skills is present
            jobs_with_skills = filtered_data[mask]

            # Count jobs by category
            job_counts = jobs_with_skills['job_category'].value_counts().reset_index()
            job_counts.columns = ['Job Category', 'Count']

            # Calculate percentage for each job category based on total dataset
            total_jobs = len(filtered_data)  # Total number of jobs in the filtered dataset
            job_counts['Percentage'] = (job_counts['Count'] / total_jobs) * 100

            # Display the most fitted job profile
            if not job_counts.empty:
                col_1, col_2 = st.columns(2)
                with col_1:
                    top_job_category = job_counts.iloc[0]
                    display_big_metric(f"Matched role:", f"{top_job_category['Job Category']}")
                with col_2:
                # Salary Range
                    salary_rows = jobs_with_skills[jobs_with_skills['job_category'] == top_job_category['Job Category']]
                    avg_salary = salary_rows['avg_salary'].mean()
                    display_big_metric(f"Average salary:", f"{format_salary(avg_salary)}")

                # Plot Salary Distribution with more detailed bins
                st.subheader("Salary Distribution")
                fig_salary_distribution = px.histogram(
                    jobs_with_skills,
                    x='avg_salary',
                    nbins=50,  # Increased number of bins for more detail
                    #title="Salary Distribution for Jobs Matching Selected Skills",
                    labels={'avg_salary': 'Salary (€)'},
                    opacity=0.7
                )

                # Update layout to fix the x-axis range from 20000 to 80000
                fig_salary_distribution.update_layout(
                    xaxis_title="Salary (€)",
                    yaxis_title="Frequency",
                    bargap=0.2,
                    xaxis=dict(range=[20000, 80000])  # Fixed salary range
                )

                # Display the updated salary distribution chart
                st.plotly_chart(fig_salary_distribution)
            else:
                st.error("No matching job categories found for the selected skills and experience range.")
        else:
            st.info("Please select skills to see matching job profiles.")
        
        
        
### NETWORK
st.markdown("---")

col1, col2, col3 = st.columns(3)
with col2:
    st.image(network, use_column_width=True)
    
st.image(network_1_path, use_column_width=True)
    
    
    
    
    
    
    
    
    
    
    