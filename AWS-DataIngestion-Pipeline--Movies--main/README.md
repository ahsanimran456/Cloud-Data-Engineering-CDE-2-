# AWS Data Ingestion Pipeline - Movies

![AWS Logo](https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg)  


## Project Overview
An end-to-end AWS data ingestion pipeline that collects, processes, and stores Marvel Cinematic Universe (MCU) movie data from multiple sources, showcasing a serverless architecture using AWS services.

---

## Architecture
This project demonstrates a serverless data pipeline using:

- **AWS Lambda**: Runs the data processing code  
- **Amazon S3**: Stores processed data  
- **AWS EventBridge**: Schedules and triggers the workflow  

![Architecture Diagram](https://i.postimg.cc/yYCTcm0f/Chat-GPT-Image-May-18-2025-06-16-12-PM.png)  


---

## Data Sources

- **Wikipedia**: Web scraping for movie metadata and character information  
- **OMDB API**: REST API for detailed movie information  

---

## Features

- Automated data collection from multiple sources  
- Data cleaning and transformation  
- Structured data storage in S3  
- Fully serverless architecture  

---

## How It Works

### Data Collection
- Scrapes Marvel movie information from Wikipedia  
- Fetches additional movie details from the OMDB API  
- Extracts character appearance data from Wikipedia  

### Data Processing
- Cleans movie titles and metadata  
- Normalizes dates and text fields  
- Transforms data into structured formats  

### Data Storage
- Uploads processed datasets to S3:  
  - `movies.csv`: Basic movie information and release dates  
  - `omdb.csv`: Detailed movie information from OMDB API  
  - `characters.csv`: Character appearances across movies  

---

## Setup Instructions

### Prerequisites
- AWS Account  
- Python 3.8+  
- Required Python packages: `pandas`, `requests`, `boto3`, `beautifulsoup4`  

### AWS Configuration
1. Create an S3 bucket to store processed data  
2. Set up an AWS Lambda function with the provided code  
3. Configure EventBridge to trigger the Lambda function on your desired schedule  

### API Keys
- Obtain an OMDB API key from [omdbapi.com](https://www.omdbapi.com/)  
- Update the `OMDB_API_KEY` variable in the code  

### Deployment
1. Clone this repository  
```bash
git clone https://github.com/zainalvi110/AWS-DataIngestion-Pipeline--Movies-.git
cd AWS-DataIngestion-Pipeline--Movies-
