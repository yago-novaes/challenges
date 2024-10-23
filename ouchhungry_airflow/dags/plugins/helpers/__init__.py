# plugins/helpers/__init__.py

"""
Funções Disponíveis:
- extract_transactions: Extrai dados de transações do BigQuery.
- extract_products: Extrai dados de produtos do BigQuery.
- extract_clients: Extrai dados de clientes do BigQuery.
- clean_nulls: Remove valores nulos de um DataFrame.
- remove_duplicates: Remove duplicatas de um DataFrame com base em colunas específicas.
- validate_emails: Valida endereços de e-mail usando expressões regulares.
- validate_postal_codes: Valida códigos postais usando expressões regulares.
- transform_transactions: Transforma dados de transações mesclando com dados de produtos.
- aggregate_by_city: Agrega dados de transações por cidade.
- load_to_sftp: Carrega dados para um servidor SFTP.
- load_to_api: Envia dados para uma API via HTTP.
"""

from .extraction import (
    extract_transactions,
    extract_products,
    extract_clients
)
from .cleaning import (
    clean_nulls,
    remove_duplicates,
    validate_emails,
    validate_postal_codes
)
from .transformation import (
    transform_transactions,
    aggregate_by_city
)
from .loading import (
    load_to_sftp,
    load_to_api
)
