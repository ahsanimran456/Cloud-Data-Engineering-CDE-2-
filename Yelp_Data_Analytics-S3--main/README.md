# 📊 Yelp Data Analytics Pipeline using Python, AWS S3 & Snowflake

This project demonstrates a complete data engineering pipeline that processes large-scale Yelp review data (in JSON format), splits it into manageable chunks, uploads the chunks to Amazon S3, and finally loads and transforms the data in Snowflake for structured analytics.

## 📁 Project Structure

├── data/ │ └── yelp_academic_dataset_review.json # Raw JSON dataset ├── scripts/ │ ├── split_json.py # Splits large JSON file into chunks │ ├── upload_to_s3.py # Uploads chunks to S3 bucket │ └── snowflake_loader.py # Loads data into Snowflake table ├── config/ │ └── config.yaml # Credentials and configurations ├── utils/ │ └── helpers.py # Utility functions └── README.md


## ⚙️ Tech Stack

- Python – for JSON splitting and automation scripts  
- Amazon S3 – to store processed data chunks  
- Snowflake – to load and flatten JSON into tabular format  
- SQL – for querying structured data  
- JSON – as the source data format

## 🚀 Pipeline Flow

1. Raw Yelp JSON data is ingested.
2. Python script splits large JSON into smaller chunks.
3. Chunked files are uploaded to AWS S3.
4. Snowflake loads files directly from S3.
5. JSON is flattened and transformed to tabular form using SQL.
6. The data is now ready for analysis and querying.

## 🧪 How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/zainalvi110/Yelp_Data_Analytics-S3-.git
   cd Yelp_Data_Analytics-S3-

