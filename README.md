# yourfirstdatajob Streamlit App

This repository contains the code for the **yourfirstdatajob** Streamlit app, which provides insights into the data job market in France. 

![Alt text](https://github.com/enekoegiguren/yourfirstdatajob/blob/main/yourfirstdatajob.jpg)

The app pulls data from the France Travail API, offering users daily updated job listings and analytics on in-demand skills, salary ranges, and job role trends to support aspiring data professionals in their job search.

## Features

- **Job Listing Insights**: View and filter data-related job postings, updated daily.
- **Skill Analytics**: Discover which technical and soft skills are most sought after by employers.
- **Salary Range Analysis**: Explore salary ranges across different data roles.

## Getting Started

Follow these instructions to run the Streamlit app locally.

### Prerequisites

- **Python 3.8+**
- **Streamlit**: Install via `pip install streamlit`.
- **AWS S3**: Required to access job data stored in a designated S3 bucket.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/yourfirstdatajob.git
   cd yourfirstdatajob

2. Install required dependencies:
   ```bash
    pip install -r requirements.txt


4. Set up environment variables for AWS access:
   ```bash
    export AWS_ACCESS_KEY_ID='your-access-key-id'
    export AWS_SECRET_ACCESS_KEY='your-secret-access-key'
    export S3_BUCKET_NAME='your-s3-bucket-name'

5. Run the app
   ```bash
    streamlit run app.py
