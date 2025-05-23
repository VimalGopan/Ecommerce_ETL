# E-commerce ETL Pipeline using Airflow and Redshift

This project automates an ETL pipeline using Apache Airflow. It extracts e-commerce data from S3, transforms it using Pandas, and loads it into Amazon Redshift.

## Structure
- `dags/ecommerce_dag.py`: Orchestrates the full ETL pipeline.
- `etl_package/`: Modular Python package with:
  - `extract.py`: Extracts raw data from S3.
  - `transform.py`: Cleans the data.
  - `load.py`: Loads chunks to Redshift (Huge file may give us timeout - So I split into smaller chunks to reduce the load).
- `setup.py`: Enables the ETL package installation.
- `requirements.txt`: Python dependencies.

## Airflow Setup
1. Install packages:

pip install -r requirements.txt

2. Move this repo to Airflow home:

mv ecommerce-etl-airflow ~/airflow

3. Start Airflow:

airflow db init
airflow webserver & airflow scheduler

#Vimal Gopan M V