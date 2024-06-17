import datetime

from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitSparkSqlJobOperator,
    DataprocSubmitPySparkJobOperator
)
from airflow.utils.dates import days_ago

project_id = Variable.get('project_id')
region = Variable.get('region')
bucket_name = Variable.get('bucket_name')
cluster_name = Variable.get('cluster_name')

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "project_id": project_id,
    "region": region,
    "cluster_name": cluster_name,
    "start_date": days_ago(1)
}

with models.DAG(
    "daily_product_revenue_jobs_dag",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    task_cleanup = DataprocSubmitSparkSqlJobOperator(
        task_id='run_cleanup',
        query_uri=f'gs://{bucket_name}/scripts/cleanup.sql',
    )

    task_load_orders = DataprocSubmitSparkSqlJobOperator(
        task_id='load_orders',
        query_uri=f'gs://{bucket_name}/scripts/create_tables.sql',
        variables={
            'bucket_name': f'gs://{bucket_name}',
            'table_name': 'orders'
        },
    )

    task_load_order_items = DataprocSubmitSparkSqlJobOperator(
        task_id='load_order_items',
        query_uri=f'gs://{bucket_name}/scripts/create_tables.sql',
        variables={
            'bucket_name': f'gs://{bucket_name}',
            'table_name': 'order_items'
        },
    )

    task_compute_daily_product_revenue = DataprocSubmitSparkSqlJobOperator(
        task_id='run_compute_daily_product_revenue',
        query_uri=f'gs://{bucket_name}/scripts/compute_daily_product_revenue.sql',
        variables={
            'bucket_name': f'gs://{bucket_name}'
        },
    )

    task_load_dpr_bq = DataprocSubmitPySparkJobOperator(
        task_id='run_load_dpr_bq',
        main=f'gs://{bucket_name}/apps/daily_product_revenue_bq/app.py',
        dataproc_properties={
            'spark.app.name': 'BigQuery Loader - Daily Product Revenue',
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://{bucket_name}/retail_gold.db/daily_product_revenue',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'retail',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name
        },
    )
    
    task_cleanup >> task_load_orders
    task_cleanup >> task_load_order_items

    task_load_orders >> task_compute_daily_product_revenue
    task_load_order_items >> task_compute_daily_product_revenue

    task_compute_daily_product_revenue >> task_load_dpr_bq
    