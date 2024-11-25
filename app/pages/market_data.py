import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from dotenv import load_dotenv

from PIL import Image
import os
import re



# Load environment variables from .env
load_dotenv('../.env')
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
files_path = os.path.join(app_dir, 'files')

image_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(image_path)

market_data_path = os.path.join(files_path, 'market_data.png')
market_data = Image.open(market_data_path)

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
data = data[(data['year'] > 2023) & (data['month'] > 5)]
number_of_jobs = len(data)
jobs_with_salary = len(data[data['avg_salary'].notnull()])
jobs_with_experience = len(data[data['experience_bool'] != 'N'])

percent_with_salary = (jobs_with_salary / number_of_jobs) * 100 if number_of_jobs > 0 else 0
percent_with_experience = (jobs_with_experience / number_of_jobs) * 100 if number_of_jobs > 0 else 0

data_experience = data[data['experience'].notnull()]
data_experience['experience'] = data_experience['experience'].astype(int) 
data_experience= data_experience[data_experience['experience'] > 0]


#with col2:
st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")

col1, col2, col3 = st.columns(3)
with col2:
    st.image(market_data, use_column_width=True)
st.markdown("---")

col1, col2, col3 = st.columns(3)
with col1:
    display_big_metric("Number of jobs analyzed", number_of_jobs)
with col2:
    display_big_metric("Jobs with Salary Specified", f"{percent_with_salary:.1f}%")
with col3:
    display_big_metric("Jobs with Experience Specified", f"{percent_with_experience:.1f}%")
    
st.markdown("---")
st.write("## Most demanded job categories ")
st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    if not data.empty:
        most_demanded_jobs = (
            data['job_category']
            .value_counts()
            .sort_values(ascending=False)
            .head(10)
        )

        # Calculate the percentage for each category
        total_jobs = most_demanded_jobs.sum()  # Total number of jobs
        most_demanded_jobs_percentage = (most_demanded_jobs / total_jobs) * 100
        most_demanded_jobs_percentage = most_demanded_jobs_percentage.sort_values(ascending=True)

        # Plot using Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=most_demanded_jobs_percentage.values,  # X-axis: percentage values
                y=most_demanded_jobs_percentage.index,  # Y-axis: job categories
                orientation='h',  # 'h' indicates horizontal bars
                text=[f'{value:.0f}%' for value in most_demanded_jobs_percentage.values],  # Text: formatted percentages
                textposition='auto',
                textfont=dict(size=22)
            )
        ])

        fig.update_layout(
            #title="Top 10 Most Demanded Job Categories (Percentage)",
            xaxis_title="Job Category",
            yaxis_title="Percentage of Jobs (%)",
            template="plotly_white",
            bargap = 0.05,
            height = 600
        )

        st.plotly_chart(fig)
    
with col2:
    if not data.empty and 'date_creation' in data.columns:
        data['date_creation'] = pd.to_datetime(data['date_creation'], errors='coerce')
        data['extracted_date'] = pd.to_datetime(data['extracted_date'], errors='coerce')

        data['effective_date'] = data.apply(
            lambda row: row['date_creation'] if row['extracted_date'] <= pd.Timestamp('2024-11-04') else row['extracted_date'], 
            axis=1
        )
        
        filtered_data = data[data['effective_date'] > pd.Timestamp('2024-11-01')]
        

        job_category_over_time = (
            filtered_data.groupby([pd.Grouper(key='effective_date', freq='W-SUN'), 'job_category'])
            .size()
            .reset_index(name='Count')
        )

        top_10_categories = job_category_over_time.groupby('job_category')['Count'].sum().nlargest(10).index
        job_category_top10 = job_category_over_time[job_category_over_time['job_category'].isin(top_10_categories)]

        if not job_category_top10.empty:
            fig_time_evolution = px.line(
                job_category_top10,
                x='effective_date', 
                y='Count',
                color='job_category',
                labels={'effective_date': 'Date', 'Count': 'Number of Listings'},
                line_shape='linear',
                title="Temporal Evolution of Top Job Categories",
                height=600
            )

            st.plotly_chart(fig_time_evolution)
        else:
            st.write("No data available for the selected job categories to plot temporal evolution.")
    else:
        st.write("No data available for temporal evolution analysis.")
        
st.markdown("---")

# ---- Filter Section ----
st.write("### Filter Options")
col1, col2, col3, col4 = st.columns(4)

with col1:
    job_categories = data['job_category'].unique().tolist()
    selected_category = st.selectbox("Select Job Category", options=["All"] + job_categories)

with col2:
    years = data['year'].unique().tolist()
    selected_year = st.selectbox("Select Year", options=["All"] + years)

with col3:
    months = data['month'].unique().tolist()
    selected_month = st.selectbox("Select Month", options=["All"] + months)


with col4:
    # Add salary filter using slider
    max_salary_data = data[data['avg_salary'] < 300000]
    max_salary = int(max_salary_data['avg_salary'].max())
    salary_range = st.slider(
        "Select Salary Range (€)", 
        min_value=0, 
        max_value=max_salary, 
        value=(0, max_salary), 
        step=5000,
        format="€%d"
    )
# Apply filters to data
filtered_data = data.copy()

# Apply the selected filters to the dataset
if selected_category != "All":
    filtered_data = filtered_data[filtered_data['job_category'] == selected_category]
if selected_year != "All":
    filtered_data = filtered_data[filtered_data['year'] == selected_year]
if selected_month != "All":
    filtered_data = filtered_data[filtered_data['month'] == selected_month]
    



# Apply filtering based on the dynamic max_salary
if salary_range != (0, max_salary):
    filtered_data = filtered_data[
        (filtered_data['avg_salary'] >= salary_range[0]) & 
        (filtered_data['avg_salary'] <= salary_range[1])
    ]

# ---- Check if data is empty after filtering ----
if filtered_data.empty:
    st.write("No data available for the selected filters.")
else:
    # ---- KPIs Calculation ----
    len_data = len(filtered_data)
    avg_experience_data = filtered_data[filtered_data['experience'] > 0]
    average_experience = avg_experience_data['experience'].mean() if not avg_experience_data['experience'].isnull().all() else 0

    # Calculate the contract type counts and percentages
    contract_counts = filtered_data['contract_type'].value_counts()
    total_contracts = contract_counts.sum()
    top_contract_type = contract_counts.idxmax() if not contract_counts.empty else "N/A"
    top_contract_percentage = (contract_counts.max() / total_contracts) * 100 if total_contracts > 0 else 0

    # Salary calculation
    filtered_data_salary = filtered_data[(filtered_data['avg_salary'] < 300000) & (filtered_data['avg_salary'] > 0)]
   # filtered_data_salary = filtered_data[(filtered_data['avg_salary'] > 0)]
    min_salary = filtered_data_salary['avg_salary'].min() if not filtered_data_salary['avg_salary'].isnull().all() else 0
    max_salary = filtered_data_salary['avg_salary'].max() if not filtered_data_salary['avg_salary'].isnull().all() else 0
    average_salary = filtered_data_salary['avg_salary'].mean() if not filtered_data_salary['avg_salary'].isnull().all() else 0

    # ---- KPI Section ----
    st.write("### Key Performance Indicators (KPIs)")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        display_big_metric("Number of jobs analyzed", f"{len_data}") 

    with col2:
        # Check if salary data is available, else display a message
        if average_salary == 0:
            display_big_metric("Average Salary (€)", "No salary data informed")
        else:
            display_big_metric("Average Salary (€)", format_salary(average_salary))

    with col3:
        # Check if min or max salary data is available, else display a message
        if min_salary == 0 and max_salary == 0:
            display_big_metric("Salary Range(€)", "No salary data informed")
        else:
            display_big_metric("Salary Range(€)", f"{format_salary(min_salary)} - {format_salary(max_salary)}")

    with col4:
        display_big_metric("Average Experience", f"{average_experience:.1f} years")

    with col5:
        display_big_metric("Permanent contract", f"{top_contract_percentage:.1f}%")
        
    st.markdown("---")

    # Now we define the columns for the charts
    col1, col2 = st.columns(2)

# Determine the column to use based on the date
with col1:
    # Job time series data
    st.write("### Number of jobs over the last month")
    df = filtered_data.copy()
    
    # Ensure both columns are datetime
    df['date_creation'] = pd.to_datetime(df['date_creation'])
    df['extracted_date'] = pd.to_datetime(df['extracted_date'])

    # Use the appropriate column for each row
    df['effective_date'] = df.apply(
        lambda row: row['date_creation'] if row['extracted_date'] <= pd.Timestamp('2024-11-04') else row['extracted_date'], 
        axis=1
    )

    # Filter to show data for the last month
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
            labels={'effective_date': 'Date', 'number_of_jobs': 'Number of Jobs'},
            markers=True
        )

        fig.update_traces(
            line=dict(color='blue'),
            fill='tozeroy',
            mode='lines+markers'
        )
        fig.update_layout(height=700)

        st.plotly_chart(fig)

    with col2:
        # ---- Job Locations Map Section ----
        st.write("### Job Locations Map")
        if not filtered_data.empty:
            job_counts = filtered_data.groupby(['latitude', 'longitude']).size().reset_index(name='job_count')
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


st.markdown("---")


# ---- Most Demanded Job Categories Section ----
st.write("### Top company fields demanding data jobs")

if not filtered_data.empty:

    # Calculate the percentage for each category
    most_demanded_company_fields = (
        filtered_data['company_field']
        .value_counts()
        .sort_values(ascending=False)
        .head(10)
    )


    # Calculate the percentage for each category
    #total_jobs = most_demanded_jobs.sum()  # Total number of jobs
    total_jobs = len(filtered_data)
    most_demanded_company_field_percentage = (most_demanded_company_fields / total_jobs) * 100
    most_demanded_company_field_percentage = most_demanded_company_field_percentage.sort_values(ascending=True)

    # Plot using Plotly
    fig = go.Figure(data=[
        go.Bar(
            x=most_demanded_company_field_percentage.values,  # X-axis: percentage values
            y=most_demanded_company_field_percentage.index,  # Y-axis: job categories
            orientation='h',  # 'h' indicates horizontal bars
            text=[f'{value:.0f}%' for value in most_demanded_company_field_percentage.values],  # Text: formatted percentages
            textposition='auto',
            textfont=dict(size=22)
        )
    ])

    fig.update_layout(
        #title="Top 10 Most Demanded Job Categories (Percentage)",
        xaxis_title="Company field",
        yaxis_title="Percentage of Jobs (%)",
        template="plotly_white",
        bargap = 0.05,
        height = 600
    )

    st.plotly_chart(fig)