# plugins/helpers/cleaning.py

"""
Módulo de Limpeza de Dados para o Pipeline da OuchHungry.

Este módulo contém funções para limpar dados, incluindo remoção de valores nulos,
eliminação de duplicatas e validação de campos específicos.
"""

import pandas as pd
import re
import logging

def clean_nulls(df_json):
    """
    Remove valores nulos de um DataFrame.

    Args:
        df_json (str): DataFrame em formato JSON a ser limpo.

    Returns:
        str: DataFrame sem valores nulos em formato JSON.
    """
    df = pd.read_json(df_json)
    df_clean = df.dropna()
    logging.info("Valores nulos removidos")
    return df_clean.to_json()

def remove_duplicates(df_json, subset=None):
    """
    Remove duplicatas de um DataFrame com base em colunas específicas.

    Args:
        df_json (str): DataFrame em formato JSON a ser limpo.
        subset (list, optional): Lista de colunas para verificar duplicatas.

    Returns:
        str: DataFrame sem duplicatas em formato JSON.
    """
    df = pd.read_json(df_json)
    df_clean = df.drop_duplicates(subset=subset)
    logging.info("Duplicatas removidas")
    return df_clean.to_json()

def validate_emails(df_json, email_column):
    """
    Valida endereços de e-mail em uma coluna específica usando regex.

    Args:
        df_json (str): DataFrame em formato JSON contendo os dados.
        email_column (str): Nome da coluna de e-mail a ser validada.

    Returns:
        str: DataFrame contendo apenas e-mails válidos em formato JSON.
    """
    df = pd.read_json(df_json)
    regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    df['email_valid'] = df[email_column].apply(lambda x: bool(re.match(regex, x)))
    df_clean = df[df['email_valid']].drop(columns=['email_valid'])
    logging.info("Emails validados")
    return df_clean.to_json()

def validate_postal_codes(df_json, postal_code_column):
    """
    Valida códigos postais em uma coluna específica usando regex.

    Args:
        df_json (str): DataFrame em formato JSON contendo os dados.
        postal_code_column (str): Nome da coluna de código postal a ser validada.

    Returns:
        str: DataFrame contendo apenas códigos postais válidos em formato JSON.
    """
    df = pd.read_json(df_json)
    regex = r'^\d{5}-\d{3}$'
    df['postal_code_valid'] = df[postal_code_column].apply(lambda x: bool(re.match(regex, x)))
    df_clean = df[df['postal_code_valid']].drop(columns=['postal_code_valid'])
    logging.info("Códigos postais validados")
    return df_clean.to_json()
