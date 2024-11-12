import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
from io import BytesIO
from dotenv import load_dotenv

from PIL import Image
import os
import re



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
    average_experience = data['experience'].mean() if not data['experience'].isnull().all() else None
    col1, col2, col3 = st.columns(3)
    with col1:
        display_big_metric("Average experience needed:", f"{average_experience:.1f} years")
    with col2:
        display_big_metric("⬆️ Most experience needed for:", f"{most_experience_job}")
    with col3:
        display_big_metric("⬇️ Least experience needed for:", f"{least_experience_job}")

    st.write(" ")

    st.table(exp_summary)



st.markdown("---")
data = data[data['max_salary'] < 200000]
# ---- Salary Analysis by Job ----


st.write("## Salary by Job")
data['avg_salary_rounded'] = data['avg_salary'].round()  # Round average salary

if not data.empty:
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
    avg_salary = data['avg_salary'].mean() if not data['avg_salary'].isnull().all() else None
    
    col1, col2, col3 = st.columns(3)
    with col1:
        display_big_metric("Average salary:", format_salary(avg_salary))
    with col2:
        display_big_metric("⬆️ Highest salary for:", f"{highest_salary_job}")
    with col3:
        display_big_metric("⬇️ Lowest salary for:", f"{lowest_salary_job}")

    st.write(" ")

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
    st.table(exp_salary_summary)
