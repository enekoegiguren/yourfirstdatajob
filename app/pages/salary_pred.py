import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os
import re
import plotly.express as px
from PIL import Image
import joblib




# Load environment variables from .env
load_dotenv('../.env')

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


files_path = os.path.join(app_dir, 'files')

image_path = os.path.join(files_path, 'logo.png')
image_logo = Image.open(image_path)

predict_path = os.path.join(files_path, 'predict.png')
predict = Image.open(predict_path)

model_exp_path = os.path.join(files_path, 'model_explanation.png')
model_exp = Image.open(model_exp_path)

model_path = os.path.join(files_path, 'model.png')
model = Image.open(model_path)

model_2_path = os.path.join(files_path, 'model_2.png')
model_2 = Image.open(model_2_path)


AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_PREFIX = os.getenv('FILE_PREFIX')
S3_MODEL_PATH = os.getenv('S3_MODEL_PATH')

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


# Function to load model from S3
def load_model_from_s3():
    try:
        # Download the model file from S3
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_MODEL_PATH)
        model_data = response['Body'].read()
        
        # Load the model using joblib
        model = joblib.load(BytesIO(model_data))
        return model
    except Exception as e:
        print(f"Error loading model from S3: {e}")
        return None


# Load the model in your Streamlit app
pipeline = load_model_from_s3()
data = load_data()
# if pipeline:
#     print("Model loaded successfully")
    
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
    
# Format salary as € in thousands (k)
def format_salary(value):
    return f"{value / 1000:.0f}k €"
    
col1, col2, col3 = st.columns(3)
with col2:
    st.image(predict, use_column_width=True)
st.markdown("---")



# Function to predict salary
def predict_salary(job_category, experience):
    # Prepare the input data (make sure to match the model's input format)
    input_data = pd.DataFrame({
        'job_category': [job_category],
        'experience': [experience],
     #   **{skill: [1 if skill in skills else 0] for skill in reduced_skills_columns}  # Assuming binary skills representation
    })
    
    # Make a prediction
    salary = pipeline.predict(input_data)[0]
    return salary


# Function to compute evaluation metrics
from sklearn.metrics import mean_absolute_error, mean_squared_error

def compute_metrics(true_values, predicted_values):
    mae = mean_absolute_error(true_values, predicted_values)
    rmse = mean_squared_error(true_values, predicted_values, squared=False)
    return mae, rmse

# Function to predict salaries for the entire dataset
def predict_for_dataset(data):
    # Prepare the input data.
    input_data = data[(data['experience']>=0) & (data['avg_salary']>0)& (data['avg_salary']< 100000)]
    input_data = input_data[['job_category', 'experience']]
    
    # Predict salaries
    predicted_salaries = pipeline.predict(input_data)
    
    # Add predictions to the dataset
    data['predicted_salary'] = predicted_salaries
    return data


st.image(model, use_column_width=True)
    

st.image(model_2, use_column_width=True)


col1, col2 = st.columns(2)
with col1:  
    job_category = st.selectbox("Select Job Category", data['job_category'].unique())

with col2:
    experience = st.slider("Experience (in years)", 0, 20, 5)
    
    
if st.button("Predict Salary"):
    salary = predict_salary(job_category, experience)
        #st.write(f"Predicted Salary: {salary}")
    display_big_metric(f"Predicted Salary:", f"{format_salary(salary)}")



data_to_predict = data[(data['avg_salary']>0)&(data['experience']>=0) & (data['avg_salary']< 100000)]
data_with_predictions = predict_for_dataset(data_to_predict)


st.markdown("---")
col1, col2, col3 = st.columns(3)
with col2:
    st.image(model_exp, use_column_width=True)
st.markdown("---")


col1, col2 = st.columns(2)

with col1:
    mae, rmse = compute_metrics(data_with_predictions['avg_salary'], data_with_predictions['predicted_salary'])
    display_big_metric(f"Mean Absolute Error (MAE):", f"{mae:.2f}")
    display_big_metric(f"Root Mean Squared Error (RMSE):", f"{rmse:.2f}")

with col2:
    # Visualization: Predicted vs. Actual Salaries
    fig = px.scatter(
        data_with_predictions,
        x='avg_salary',
        y='predicted_salary',
        title="Predicted vs. Actual Salaries",
        labels={"avg_salary": "Actual Salary", "predicted_salary": "Predicted Salary"},
        opacity=0.6
    )
    fig.add_shape(
        type="line",
        x0=data_with_predictions['avg_salary'].min(),
        y0=data_with_predictions['avg_salary'].min(),
        x1=data_with_predictions['avg_salary'].max(),
        y1=data_with_predictions['avg_salary'].max(),
        line=dict(color="Red", width=2)
    )
    st.plotly_chart(fig)
