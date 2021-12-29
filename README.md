# caldaia

insight_json_csv_gcs.py
- This script was originally used to back fill historic data
- Loop through a specified date range to capture a daily batch of per second time series data per tag
- Generates per second data set csv file, stores file in GCP storage bucket for processing into BigQuery Table

bp_insight.py 
- This script is called with GCP Scheduler to run every 5 minutes
- Captures a previous 5 minute batch of per second time series data
- Trasforms the data to fill in gaps of per second data
- Calculates per minute average
- Calculates per hour average
- Generates per second data set csv file, stores file in GCP storage bucket for processing into BigQuery Table
- Generates per minute average data set csv file, stores file in GCP storage bucket for processing into BigQuery Table
- Generates per hour average data set csv file, stores file in GCP storage bucket for processing into BigQuery Table

streaming.py
- This script is initiated when a new CSV file is stored in the GCP streaming bucket
- CSV data from the file is processed and appended to BigQuery Table
- On successfull processing of file, publish SUCCESS_TOPIC which initiates streaming_success.py
- On failure in processing of file, publish ERROR_TOPIC which initiates streaming_error.py

streaming_success.py
- The file is moved to a "success" bucket

streaming_error.py
- The file is moved to a "error" bucket

