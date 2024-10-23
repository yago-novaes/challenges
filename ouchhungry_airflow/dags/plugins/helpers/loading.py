# plugins/helpers/loading.py

"""
Módulo de Carregamento de Dados para o Pipeline da OuchHungry.

Este módulo contém funções para carregar dados para diferentes destinos, como servidores SFTP
e APIs via HTTP.
"""

import pandas as pd
import logging

def load_to_sftp(df_json, sftp_hook, remote_path, local_path):
    """
    Carrega um DataFrame para um servidor SFTP.

    Args:
        df_json (str): DataFrame em formato JSON a ser carregado.
        sftp_hook (SFTPHook): Hook do Airflow para conexão SFTP.
        remote_path (str): Caminho remoto onde o arquivo será armazenado.
        local_path (str): Caminho local temporário para armazenar o arquivo antes do upload.
    """
    df = pd.read_json(df_json)
    df.to_csv(local_path, index=False)
    sftp_hook.store_file(remote_path, local_path)
    logging.info("Dados carregados para SFTP")

def load_to_api(df_json, api_hook, endpoint, token):
    """
    Envia um DataFrame para uma API via HTTP POST.

    Args:
        df_json (str): DataFrame em formato JSON a ser enviado.
        api_hook (HttpHook): Hook do Airflow para conexão HTTP.
        endpoint (str): Endpoint da API para envio dos dados.
        token (str): Token de autenticação para a API.

    Raises:
        Exception: Se a resposta da API não for bem-sucedida.
    """
    df = pd.read_json(df_json)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = api_hook.run(
        endpoint=endpoint,
        data=df.to_json(orient='records'),
        headers=headers
    )
    if response.status_code == 200:
        logging.info("Dados enviados para a API com sucesso")
    else:
        logging.error("Falha ao enviar dados para a API")
        response.raise_for_status()
