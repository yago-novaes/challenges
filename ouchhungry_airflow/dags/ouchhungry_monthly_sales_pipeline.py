# dags/ouchhungry_monthly_sales_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.hooks.SFTPHook import SFTPHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import pandas as pd

# Importando funções auxiliares com docstrings
from plugins.helpers import (
    extract_transactions,
    extract_products,
    extract_clients,
    clean_nulls,
    remove_duplicates,
    validate_emails,
    validate_postal_codes,
    transform_transactions,
    aggregate_by_city,
    load_to_sftp,
    load_to_api
)

default_args = {
    'owner': 'ouchhungry',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['igor@empresa.com', 'diego@empresa.com', 'leonardo@empresa.com'],
}

def extract_and_push_monthly(**kwargs):
    """
    Função para extrair dados do BigQuery e empurrá-los para o XCom para relatórios mensais.
    """
    project_id = Variable.get('gcp_project_id')
    dataset_id = Variable.get('bigquery_dataset_id')
    
    transactions_df = extract_transactions(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='transactions',
        gcp_conn_id='google_cloud_default'
    )
    products_df = extract_products(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='products',
        gcp_conn_id='google_cloud_default'
    )
    clients_df = extract_clients(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='clients',
        gcp_conn_id='google_cloud_default'
    )
    
    # Push para XCom como JSON
    kwargs['ti'].xcom_push(key='transactions_df', value=transactions_df.to_json())
    kwargs['ti'].xcom_push(key='products_df', value=products_df.to_json())
    kwargs['ti'].xcom_push(key='clients_df', value=clients_df.to_json())

def notify_failure_monthly(context):
    """
    Função de callback para notificar em caso de falha no DAG mensal.
    """
    email = ['correa.igor@aiqfome.com', 'diego.pastrello@aiqfome.com', 'leonardo.artur@aiqfome.com']
    subject = "Falha no Pipeline Mensal OuchHungry"
    html_content = f"""
    <p>O DAG <b>{context['dag'].dag_id}</b> falhou na execução.</p>
    <p>Task: <b>{context['task_instance'].task_id}</b></p>
    <p>Data de execução: <b>{context['ts']}</b></p>
    <p>Erro: <b>{context['exception']}</b></p>
    """
    logging.error("Falha no DAG mensal. Notificando a equipe.")

def notify_success_monthly(**kwargs):
    """
    Função para notificar a equipe de vendas sobre o sucesso do pipeline mensal.
    """
    logging.info("Pipeline mensal concluído com sucesso. Notificando a equipe de vendas.")

with DAG(
    'ouchhungry_monthly_sales_pipeline',
    default_args=default_args,
    description='Pipeline modular para relatórios mensais de vendas da OuchHungry',
    schedule_interval='@monthly',
    catchup=False,
    on_failure_callback=notify_failure_monthly,
) as dag:

    with TaskGroup("extraction", tooltip="Extração de Dados") as extraction_group:
        extract_and_push_task = PythonOperator(
            task_id='extract_and_push',
            python_callable=extract_and_push_monthly,
            provide_context=True,
        )

    with TaskGroup("cleaning", tooltip="Limpeza de Dados") as cleaning_group:
        clean_transactions_nulls = PythonOperator(
            task_id='clean_transactions_nulls',
            python_callable=clean_nulls,
            op_kwargs={'df_json': "{{ ti.xcom_pull(task_ids='extraction.extract_and_push', key='transactions_df') }}"},
        )

        clean_products_nulls = PythonOperator(
            task_id='clean_products_nulls',
            python_callable=clean_nulls,
            op_kwargs={'df_json': "{{ ti.xcom_pull(task_ids='extraction.extract_and_push', key='products_df') }}"},
        )

        clean_clients_nulls = PythonOperator(
            task_id='clean_clients_nulls',
            python_callable=clean_nulls,
            op_kwargs={'df_json': "{{ ti.xcom_pull(task_ids='extraction.extract_and_push', key='clients_df') }}"},
        )

        dedup_transactions = PythonOperator(
            task_id='dedup_transactions',
            python_callable=remove_duplicates,
            op_kwargs={
                'df_json': "{{ ti.xcom_pull(task_ids='cleaning.clean_transactions_nulls') }}",
                'subset': ['transaction_id']
            },
        )

        validate_emails_task = PythonOperator(
            task_id='validate_emails',
            python_callable=validate_emails,
            op_kwargs={
                'df_json': "{{ ti.xcom_pull(task_ids='cleaning.clean_clients_nulls') }}",
                'email_column': 'email'
            },
        )

        validate_postal_codes_task = PythonOperator(
            task_id='validate_postal_codes',
            python_callable=validate_postal_codes,
            op_kwargs={
                'df_json': "{{ ti.xcom_pull(task_ids='cleaning.clean_clients_nulls') }}",
                'postal_code_column': 'postal_code'
            },
        )

    with TaskGroup("transformation", tooltip="Transformação de Dados") as transformation_group:
        transform_transactions_task = PythonOperator(
            task_id='transform_transactions',
            python_callable=transform_transactions,
            op_kwargs={
                'transactions_json': "{{ ti.xcom_pull(task_ids='cleaning.dedup_transactions') }}",
                'products_json': "{{ ti.xcom_pull(task_ids='cleaning.clean_products_nulls') }}",
            },
        )

        aggregate_by_city_task = PythonOperator(
            task_id='aggregate_by_city',
            python_callable=aggregate_by_city,
            op_kwargs={'transactions_json': "{{ ti.xcom_pull(task_ids='transformation.transform_transactions') }}"},
        )

    with TaskGroup("loading", tooltip="Carregamento de Dados") as loading_group:
        send_to_sftp_task = PythonOperator(
            task_id='send_to_sftp',
            python_callable=load_to_sftp,
            op_kwargs={
                'df_json': "{{ ti.xcom_pull(task_ids='transformation.aggregate_by_city') }}",
                'sftp_hook': SFTPHook(ftp_conn_id='sftp_ouchhungry'),
                'remote_path': '/path/to/monthly_report.csv',
                'local_path': '/tmp/monthly_report.csv',
            },
        )

        send_to_api_task = PythonOperator(
            task_id='send_to_api',
            python_callable=load_to_api,
            op_kwargs={
                'df_json': "{{ ti.xcom_pull(task_ids='transformation.aggregate_by_city') }}",
                'api_hook': HttpHook(http_conn_id='api_ouchhungry', method='POST'),
                'endpoint': '/upload_monthly_report',
                'token': Variable.get('api_token'),
            },
        )

    notify_success_task = PythonOperator(
        task_id='notify_sales_team',
        python_callable=notify_success_monthly,
    )

    # Definição da ordem de execução
    extraction_group >> cleaning_group >> transformation_group >> loading_group >> notify_success_task
