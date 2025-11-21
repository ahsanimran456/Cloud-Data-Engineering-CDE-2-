# 🪄 AWS Lambda S3 ETL Pipeline — Real-Time Data Cleaner

An event-driven, serverless data transformation pipeline using **AWS Lambda**, **Amazon S3**, and **CloudWatch Logs**. This project automatically processes uploaded CSV files and saves a cleaned, transformed version back to S3 — all without provisioning servers.

<p align="center">
  <a href="https://www.linkedin.com/in/zain-ul-abideen-alvi-32b3b6275/">
    <img src="https://raw.githubusercontent.com/zainalvi110/Automating-S3-to-Lambda-Triggers-with-Event-Driven-Architecture/main/banner.png" alt="AWS Lambda S3 ETL Pipeline" width="100%" />
  </a>
</p>


## 📌 How It Works

When a new `.csv` file is uploaded to the `Input-data/` folder in an S3 bucket:

1. **AWS Lambda** is triggered.
2. The file content is read and processed using custom logic.
3. The following transformations are applied:
   - ✅ Date format converted from `MM/DD/YYYY` → `YYYY-MM-DD`
   - ✅ Volume replaced with `N/A` if less than `100,000`
   - ✅ Percentage change between `open` and `close` is calculated
   - ✅ `created_at` timestamp added to each record
   - ✅ `low` field corrected if it's zero
4. The transformed data is saved in the `Output-data/` folder in the same bucket.

## 🧠 Lambda Function Logic

Written in **Python**, the Lambda script performs the following:

```python
# core features
- transform_date() → handles date formatting
- adjust_volume() → replaces low volume values
- recalculate_pct_change() → calculates % change from open to close
- Adds a 'created_at' timestamp to each row
🧪 Sample Input vs Output
Input (S3: Input-data/stock.csv)

date	open	close	volume
01/01/2024	100.0	105.0	90000

Output (S3: Output-data/stock.csv)

date	open	close	volume	close_pct_change	created_at
2024-01-01	100.0	105.0	N/A	5.0	2024-01-01T12:34:56

🛠️ AWS Services Used
Amazon S3 – Storage and event trigger

AWS Lambda – Executes transformation logic

Amazon CloudWatch – Logging and monitoring

IAM Role – Secured permission handling for Lambda

🚀 Deployment
You can deploy this pipeline via the AWS Console or using IaC (Terraform/CDK). Make sure the Lambda function has the correct permissions to access the S3 bucket and write logs to CloudWatch.

🤝 Acknowledgements
Special thanks to Sir Qasim and Ayyan Hussain for their valuable guidance throughout the development of this project.

🔗 Author
Zain Ul Abideen Alvi
Data Engineer | AI Developer
📧 zainalvi552@gmail.com
🔗 LinkedIn Profile
🧠 "One-click intelligent data reports — powered by serverless tech!"

🏷️ Tags
#AWS #Lambda #S3 #CloudWatch #Python #ETL #Serverless #Automation #DataEngineering #Mentorship
