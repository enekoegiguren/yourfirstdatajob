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

# Load environment variables from .env
load_dotenv('../.env')
current_dir = os.path.dirname(__file__)
image_path = os.path.join(current_dir, 'logo.png')
image_logo = Image.open(image_path)



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

st.set_page_config(page_title="YourFirstDataJob", page_icon="🎯",layout="wide")
st.sidebar.image(image_logo)

# Fetch data
data = load_data()

#col1, col2 = st.columns([1, 3])  # Adjust the width ratio as needed


#with col1:
#    st.image(image_logo, use_column_width=True)  # Replace with your logo path

#with col2:
st.title("""
        :blue[yourfirstdatajob]
        """)

st.markdown("---")
st.title("""
        Land _Your First Data Job_ or Level Up Your Career with :blue[Personalized Insights!]
        """)

st.markdown("---")

st.subheader("""
    Daily data analysis from the France Travail API gives you an edge 👇
            """)
st.subheader("""
    Before another Bootcamp or Udemy Course, _take the next step with real data insights_ to :blue[boost your career!]
            """)

st.markdown("---")

# ---- Insights Section ----
st.header(f"📊 Insights:  :blue[{len(data)}] jobs analyzed")


# Calculate insights only if there's data in the filtered dataset
if not data.empty:
    col1, col2 = st.columns(2)  # Adjust the width ratio as needed
    
    # Get the top two most demanded job categories
    top_categories = data['job_category'].value_counts().head(3)
    most_demanded_category_1 = top_categories.index[0] if len(top_categories) > 0 else None
    most_demanded_category_2 = top_categories.index[1] if len(top_categories) > 1 else None
    most_demanded_category_3 = top_categories.index[2] if len(top_categories) > 1 else None

    # Calculate median experience needed and average salary
    experience_data = data[data['experience'] > 0]
    average_experience = experience_data['experience'].mean() if not experience_data['experience'].isnull().all() else None
    
    salary_data = data[data['avg_salary'] > 0]
    avg_salary = salary_data['avg_salary'].mean() if not salary_data['avg_salary'].isnull().all() else None
    
    contract_counts = data['contract_type'].value_counts()
    total_contracts = contract_counts.sum()
    top_contract_type = contract_counts.idxmax()
    top_contract_percentage = (contract_counts.max() / total_contracts) * 100
    
    with col1:
        st.subheader("Most demanded jobs:")
    # Display the insights with emojis
        if most_demanded_category_1:
            st.subheader(f"1️⃣ {most_demanded_category_1}")
        if most_demanded_category_2:
            st.subheader(f"2️⃣ {most_demanded_category_2}")
        if most_demanded_category_3:
            st.subheader(f"3️⃣ {most_demanded_category_3}")
    with col2:
        if average_experience is not None:
            st.subheader(f" 📈 **Average experience:** {average_experience:.1f} years")
        
        if avg_salary is not None:
            st.subheader(f" 💰 **Average salary:** {format_salary(avg_salary)}")

        if top_contract_percentage is not None:
            st.subheader(f" 📝**Permanent contract:** {top_contract_percentage:.1f}%")

        
else:
    st.write("No data available to generate insights.")
    
st.markdown("---")

# ---- Filter Section ----
st.write("### Filter Options")
col1, col2, col3 = st.columns(3)

with col1:
    job_categories = data['job_category'].unique().tolist()
    selected_category = st.selectbox("Select Job Category", options=["All"] + job_categories)

with col2:
    #data['year'] = data['extracted_date'].dt.year
    years = data['year'].unique().tolist()
    selected_year = st.selectbox("Select Year", options=["All"] + years)

with col3:
    #data['month'] = data['extracted_date'].dt.month
    months = data['month'].unique().tolist()
    selected_month = st.selectbox("Select Month", options=["All"] + months)

# Apply filters to data
filtered_data = data.copy()
if selected_category != "All":
    filtered_data = filtered_data[filtered_data['job_category'] == selected_category]
if selected_year != "All":
    filtered_data = filtered_data[filtered_data['year'] == selected_year]
if selected_month != "All":
    filtered_data = filtered_data[filtered_data['month'] == selected_month]

# ---- KPIs Calculation ----
avg_experience_data = filtered_data[filtered_data['experience'] > 0]
average_experience = avg_experience_data['experience'].mean() if not avg_experience_data['experience'].isnull().all() else 0
min_salary = filtered_data['avg_salary'].min() if not filtered_data['avg_salary'].isnull().all() else 0
max_salary = filtered_data['avg_salary'].max() if not filtered_data['avg_salary'].isnull().all() else 0
average_salary = filtered_data['avg_salary'].mean() if not filtered_data['avg_salary'].isnull().all() else 0

# ---- KPI Section ----
st.write("### Key Performance Indicators (KPIs)")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Average Experience (Years)", f"{average_experience:.1f}", " ")

with col2:
    st.metric("Minimum Salary (€)", format_salary(min_salary))

with col3:
    st.metric("Maximum Salary (€)", format_salary(max_salary))

with col4:
    st.metric("Average Salary (€)", format_salary(average_salary))

st.markdown("---")

# ---- Most Demanded Job Categories Section ----
st.write("### Most Demanded Job Categories")

if not filtered_data.empty:
    # Get the top 10 most demanded job categories in descending order
    most_demanded_jobs = (
        filtered_data['job_category']
        .value_counts()
        .sort_values(ascending=False)
        .head(10)
    )

    # Calculate the percentage for each category
    total_jobs = most_demanded_jobs.sum()  # Total number of jobs
    most_demanded_jobs_percentage = (most_demanded_jobs / total_jobs) * 100

    # Plot using Plotly
    fig = go.Figure(data=[
        go.Bar(
            x=most_demanded_jobs_percentage.index,  # X-axis: job categories
            y=most_demanded_jobs_percentage.values,  # Y-axis: percentage values
            text=[f'{value:.0f}%' for value in most_demanded_jobs_percentage.values],  # Text: formatted percentages
            textposition='auto',
            textfont=dict(size=18)
        )
    ])
    fig.update_layout(
        title="Top 10 Most Demanded Job Categories (Percentage)",
        xaxis_title="Job Category",
        yaxis_title="Percentage of Jobs (%)",
        template="plotly_white"
    )
    st.plotly_chart(fig)
else:
    st.write("No data available for the selected filters.")

st.markdown("---")
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
        title="Job Density in France"
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
# ---- Job Contract Types ---- 
st.write("### Contract Types")

if not filtered_data.empty:
    # Calculate the contract type counts and percentages
    contract_counts = filtered_data['contract_type'].value_counts()
    total_contracts = contract_counts.sum()
    top_contract_type = contract_counts.idxmax()
    top_contract_percentage = (contract_counts.max() / total_contracts) * 100

    # Display an insight about the most common contract type
    st.subheader(f"🔹 {top_contract_percentage:.1f}% of the contracts are '{top_contract_type}'.")

    # Create a larger pie chart
    fig = px.pie(
        contract_counts,
        values=contract_counts.values,
        names=contract_counts.index,
       # title="Contract Type Distribution",
        hole=0.3,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )

    # Adjust the layout for a larger display
    fig.update_traces(textinfo='percent+label', pull=[0.1 if t == top_contract_type else 0 for t in contract_counts.index])
    fig.update_layout(width=1300, height=700)  # Adjust width and height as desired
    st.plotly_chart(fig)