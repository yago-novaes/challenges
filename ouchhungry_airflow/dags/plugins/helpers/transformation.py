# plugins/helpers/transformation.py

"""
Módulo de Transformação de Dados para o Pipeline da OuchHungry.

Este módulo contém funções para transformar dados, incluindo mesclagem de DataFrames
e agregações específicas.
"""

import pandas as pd
import logging

def transform_transactions(transactions_json, products_json):
    """
    Mescla dados de transações com dados de produtos e calcula o preço total.

    Args:
        transactions_json (str): DataFrame de transações em formato JSON.
        products_json (str): DataFrame de produtos em formato JSON.

    Returns:
        str: DataFrame transformado com preço total calculado em formato JSON.
    """
    transactions_df = pd.read_json(transactions_json)
    products_df = pd.read_json(products_json)
    
    merged_df = transactions_df.merge(products_df, on='product_id', how='left', suffixes=('_transaction', '_product'))
    merged_df['total_price'] = merged_df['quantity'] * merged_df['price_transaction']
    logging.info("Transações transformadas com sucesso")
    return merged_df.to_json()

def aggregate_by_city(transactions_json):
    """
    Agrega dados de transações por cidade, calculando médias e somas.

    Args:
        transactions_json (str): DataFrame de transações transformadas em formato JSON.

    Returns:
        str: DataFrame agregado por cidade em formato JSON.
    """
    transactions_df = pd.read_json(transactions_json)
    aggregated_df = transactions_df.groupby('city').agg({
        'price_transaction': 'mean',
        'quantity': 'sum',
        'customer_reach': 'sum'
    }).reset_index().rename(columns={
        'price_transaction': 'average_price',
        'quantity': 'units_sold',
        'customer_reach': 'total_customer_reach'
    })
    logging.info("Dados agregados por cidade")
    return aggregated_df.to_json()
