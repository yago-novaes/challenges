# plugins/helpers/extraction.py

"""
Módulo de Extração de Dados para o Pipeline da OuchHungry.

Este módulo contém funções para extrair dados de tabelas no BigQuery.
"""

import pandas as pd
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def extract_transactions(project_id, dataset_id, table_id, gcp_conn_id='google_cloud_default'):
    """
    Extrai dados da tabela de transações no BigQuery.

    Args:
        project_id (str): ID do projeto no Google Cloud.
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela de transações no BigQuery.
        gcp_conn_id (str, optional): ID da conexão do Airflow para o Google Cloud. Padrão é 'google_cloud_default'.

    Returns:
        pd.DataFrame: DataFrame contendo dados de transações.
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    transactions_df = hook.get_pandas_df(sql)
    logging.info("Transações extraídas com sucesso do BigQuery")
    return transactions_df

def extract_products(project_id, dataset_id, table_id, gcp_conn_id='google_cloud_default'):
    """
    Extrai dados da tabela de produtos no BigQuery.

    Args:
        project_id (str): ID do projeto no Google Cloud.
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela de produtos no BigQuery.
        gcp_conn_id (str, optional): ID da conexão do Airflow para o Google Cloud. Padrão é 'google_cloud_default'.

    Returns:
        pd.DataFrame: DataFrame contendo dados de produtos.
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    products_df = hook.get_pandas_df(sql)
    logging.info("Produtos extraídos com sucesso do BigQuery")
    return products_df

def extract_clients(project_id, dataset_id, table_id, gcp_conn_id='google_cloud_default'):
    """
    Extrai dados da tabela de clientes no BigQuery.

    Args:
        project_id (str): ID do projeto no Google Cloud.
        dataset_id (str): ID do dataset no BigQuery.
        table_id (str): ID da tabela de clientes no BigQuery.
        gcp_conn_id (str, optional): ID da conexão do Airflow para o Google Cloud. Padrão é 'google_cloud_default'.

    Returns:
        pd.DataFrame: DataFrame contendo dados de clientes.
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    clients_df = hook.get_pandas_df(sql)
    logging.info("Clientes extraídos com sucesso do BigQuery")
    return clients_df
