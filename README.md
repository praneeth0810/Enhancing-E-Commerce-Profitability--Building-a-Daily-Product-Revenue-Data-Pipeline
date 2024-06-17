Enhancing E-Commerce Profitability: Building a Daily Product Revenue Data Pipeline

Welcome to the repository where I've crafted a robust data pipeline to calculate daily product revenue for an e-commerce platform, enhancing decision-making capabilities and profitability. This project leverages the power of the Google Cloud Platform (GCP) to automate and optimize our data processing tasks.

## Project Description

In this project, I've utilized a series of GCP services to streamline the data ingestion, transformation, loading, and analysis phases. My goal is to provide accurate and timely financial insights that empower our business to make informed decisions and significantly boost profitability.

### Technology Stack

- **Google Cloud Storage (GCS)**: Chosen for its excellent scalability and integration capabilities with other GCP services, which simplifies managing our extensive data.
- **Google Cloud Dataproc**: Opted for its efficient handling of big data processing tasks using Apache Spark.
- **Apache Airflow**: Implemented for orchestrating the data pipeline workflows, ensuring all processes are reliably scheduled and executed.
- **Google BigQuery**: Used for its powerful analytical capabilities, allowing us to perform complex queries over large datasets quickly and cost-effectively.

### Detailed Pipeline Stages

### Ingestion:

- **Data Sources**: The pipeline ingests two primary datasets: `orders` and `order_items`, which include essential transaction details such as order ID, date, status, quantities, and product pricing.

### Pre-processing and Transformation:

- **Data Structuring**: Initially, the data lacks structure. I add column names and specify each column's data types, setting up a clear schema to guide the transformation processes.
- **Script Automation**: I developed `pre-processing.py`, a script that processes CSV files by uploading them to GCS after converting them into Parquet format. This format is chosen for its reduced storage requirements, faster query performance, and compatibility with big data tools.

### Load:

- **Data Loading**: After transformation, the data is loaded into a 'bronze' level database, serving as an intermediate stage for further processing.
- **Data Transformation**: I created a Spark-SQL script to set up a new database and table for storing and aggregating daily product revenue data. This script joins the `orders` and `order_items` tables, groups by relevant attributes, and calculates total quantities and revenues.

### Data Pipeline Orchestration:

- **Orchestration Tools**: Utilizing Dataproc and Apache Airflow, I manage the orchestration of the pipeline. I developed a Python script that initializes an Apache Airflow DAG named "daily_product_revenue_jobs_dag."
- **Task Management**: This DAG manages daily data processing tasks on Google Cloud Dataproc, involving tasks like data cleaning, loading orders, computing daily product revenue, and loading the computed revenue into BigQuery for analytics. Tasks are implemented using `DataprocSubmitSparkSqlJobOperator` for SQL scripts and `DataprocSubmitPySparkJobOperator` for PySpark applications, ensuring efficient and scalable data processing.

### Challenges and Innovative Solutions

- **Scalability and Performance**: I addressed scalability by optimizing Spark configurations and dynamically adjusting our resources to handle increased data volumes efficiently, ensuring our pipeline performs well under all conditions.

### Future Directions

- **Real-Time Data Integration**: I'm currently exploring ways to integrate real-time data feeds to enable on-the-fly revenue reporting.
- **Predictive Analytics**: Plans are underway to incorporate machine learning models to forecast sales trends and provide actionable insights for optimizing inventory and pricing strategies.
