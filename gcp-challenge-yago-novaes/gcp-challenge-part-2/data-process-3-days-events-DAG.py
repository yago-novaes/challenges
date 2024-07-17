from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'bigquery_daily_update_sinch_datasets_events',
    default_args=default_args,
    description='Daily update sinch_datasets.events table in BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'sinch'],
) as dag:

    t1 = BigQueryExecuteQueryOperator(
        task_id='update_sinch_datasets_events',
        sql="""
            SELECT
                TIMESTAMP_TRUNC(created_at, HOUR) AS created_at,
                traffic_source,
                postal_code,
                COUNT(DISTINCT session_id) AS sessions,
                COUNTIF(event_type = 'purchase') AS purchase_events,
                COUNT(*) AS total_events
            FROM
                `bigquery-public-data.thelook_ecommerce.events`
            WHERE
                CAST(created_at AS DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
            GROUP BY
                created_at, traffic_source, postal_code
            """,
        destination_dataset_table='sinch-challenge-yago-novaes.sinch_datasets.events',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        dag=dag,
    )

t1
