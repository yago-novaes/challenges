========================================
OuchHungry Airflow Data Pipeline
========================================

========================================
1. Project Overview
========================================

Este projeto implementa um pipeline de dados automatizado para a empresa fictícia OuchHungry, líder nacional em snacks e bebidas. 
Utilizando Apache Airflow, o pipeline realiza processos de Extração, Transformação, Limpeza, Carregamento (ETL), geração de relatórios diários e mensais, envio de relatórios via SFTP e API, além de monitoramento e notificações automáticas para a equipe de vendas.

========================================
2. Premissas
========================================

- Fonte de Dados: Os dados estão armazenados em tabelas no Google BigQuery.
- Produtos: Dois produtos principais são monitorados – o refrigerante 'Hungry' e o snack 'Ouch'.
- Relatórios:
  - Diários: Relatórios de desempenho diário enviados para um servidor SFTP e via API.
  - Mensal: Relatórios agregados por cidade enviados em formato resumido.
- Notificações: Emails automáticos são enviados após cada atualização de dados para notificar a equipe de vendas.
- Segurança: Conexões seguras são utilizadas para transferências de dados, e credenciais sensíveis são gerenciadas através das funcionalidades do Airflow.
- Chave pública: Não é necessário fornecer a chave pública no cliente, pois ela já está armazenada no servidor.

========================================
3. Estrutura do Projeto
========================================


ouchhungry_airflow/
├── dags/
│   ├── ouchhungry_sales_pipeline.py
│   └── ouchhungry_monthly_sales_pipeline.py
├── plugins/
│   └── helpers/
│       ├── __init__.py
│       ├── extraction.py
│       ├── cleaning.py
│       ├── transformation.py
│       └── loading.py
└── requirements.txt


- dags/: Contém os arquivos DAG do Airflow para processos diários.
- plugins/helpers/: Contém módulos auxiliares para extração, limpeza, transformação e carregamento de dados.
- requirements.txt: Lista todas as dependências necessárias para o projeto.

========================================
4. Configuração Inicial
========================================

### 4.1. Pré-requisitos

- Apache Airflow: Instalar e configurar o Airflow em seu ambiente.
- Google Cloud Account: Acesso ao Google BigQuery com permissões adequadas.
- Credenciais do Google Cloud: Arquivo JSON da chave de serviço para autenticação.
- Acesso ao Servidor SFTP: Detalhes de conexão para envio de relatórios.
- Acesso à API: Endpoint e credenciais para envio de relatórios via API.

### 4.2. Instalação de Dependências

1. Crie um ambiente virtual (opcional, mas recomendado):

   bash
   python3 -m venv venv
   source venv/bin/activate
   

2. Instale as dependências listadas no `requirements.txt`:

   bash
   pip install -r requirements.txt
   

### 4.3. Configuração de Variáveis e Conexões no Airflow

#### 4.3.1. Variáveis do Airflow

Adicione as seguintes variáveis através da UI do Airflow:

- gcp_project_id
  - Descrição: ID do projeto no Google Cloud.
  - Valor: Seu Project ID (ex: `meu-projeto`).

- bigquery_dataset_id
  - Descrição: ID do dataset no BigQuery.
  - Valor: ID do dataset (ex: `meu_dataset`).

- api_token
  - Descrição: Token de autenticação para a API.
  - Valor: `x9eYJIl0tERhj9BerXdG80C7`.

#### 4.3.2. Conexões do Airflow

Adicione as seguintes conexões através da UI do Airflow:

1. Conexão Google Cloud

   - ID da Conexão: `google_cloud_default`
   - Tipo: `Google Cloud`
   - Project ID: Seu Project ID (ex: `meu-projeto`)
   - Keyfile JSON: Cole o conteúdo do arquivo JSON da chave de serviço do Google Cloud.

2. Conexão SFTP

SFTPHook é uma abstração de nível mais alto fornecida pelo Airflow para interagir com servidores SFTP.
SFTPHook elimina a necessidade de configurar e gerenciar manualmente conexões, chaves e transferências de arquivos, simplificando o código.

   - ID da Conexão: `sftp_ouchhungry`
   - Tipo: `SFTP`
   - Host: Endereço do servidor SFTP (ex: `sftp.ouchhungry.com`)
   - Login: Usuário SFTP
   - Port: Porta SFTP

     json
     {
         "private_key": "J8lM-ld9e-4fg8-5VEH",
         "private_key_password": "",
         "timeout": 10
     }

     private_key_password na configuração da conexão SFTP no Airflow refere-se à senha (passphrase) que protege a chave privada. Pode ser deixado vazio (que foi o que fiz)
     

3. Conexão API

O HttpHook é uma ferramenta poderosa do Airflow para interagir com APIs via HTTP. Ele permite realizar requisições HTTP (GET, POST, PUT, DELETE, etc.) de forma simplificada,
integrando-se facilmente com as Connections do Airflow para gerenciar credenciais e configurações de forma segura.

   - ID da Conexão: `api_ouchhungry`
   - Tipo: `HTTP`
   - Host: URL base da API (ex: `https://api.ouchhungry.com`)
   - Login: `aiqfome`
   - Password: `x9eYJIl0tERhj9BerXdG80C7`

     json
     {
         "headers": {
             "Content-Type": "application/json"
         }
     }
     

========================================
5. Executando os DAGs
========================================

### 5.1. Pipeline Diário

- DAG: `ouchhungry_sales_pipeline`
- Descrição: Processa dados de vendas diários, gera relatórios e os envia via SFTP e API.
- Agendamento: Executa diariamente (`@daily`).

### 5.2. Pipeline Mensal

- DAG: `ouchhungry_monthly_sales_pipeline`
- Descrição: Agrega dados de vendas mensais por cidade, gera relatórios resumidos e os envia via SFTP e API.
- Agendamento: Executa mensalmente (`@monthly`).

========================================
6. Detalhes das Funcionalidades
========================================

### 6.1. Extração de Dados

- Fonte: Tabelas no Google BigQuery (`transactions`, `products`, `clients`).
- Método: Utiliza `BigQueryHook` para executar consultas SQL e extrair dados como DataFrames do Pandas.
- Funções:
  - `extract_transactions`: Extrai dados da tabela de transações.
  - `extract_products`: Extrai dados da tabela de produtos.
  - `extract_clients`: Extrai dados da tabela de clientes.

### 6.2. Limpeza de Dados

- Processos:
  - Remoção de valores nulos.
  - Eliminação de duplicatas com base em colunas específicas.
  - Validação de endereços de e-mail e códigos postais usando expressões regulares.
- Funções:
  - `clean_nulls`
  - `remove_duplicates`
  - `validate_emails`
  - `validate_postal_codes`

### 6.3. Transformação de Dados

- Processos:
  - Mesclagem de dados de transações com dados de produtos.
  - Cálculo de preço total por transação.
  - Agregação de dados por cidade para relatórios mensais.
- Funções:
  - `transform_transactions`
  - `aggregate_by_city`

### 6.4. Carregamento de Dados

- Destinos:
  - SFTP: Envio de relatórios em formato CSV para um servidor SFTP.
  - API: Envio de relatórios via requisições HTTP POST para uma API.
- Funções:
  - `load_to_sftp`
  - `load_to_api`

### 6.5. Notificações e Monitoramento

- Notificações:
  - Emails automáticos são enviados após a conclusão bem-sucedida do pipeline.
  - Emails de falha são enviados em caso de erros durante a execução.
- Monitoramento:
  - Logs são gerados em pontos críticos para facilitar a depuração.
  - Configuração de retries e lógica de idempotência para garantir resiliência.

========================================
7. Boas Práticas Implementadas
========================================

- Modularização: Utilização de TaskGroups e módulos auxiliares para separar etapas do pipeline.
- Documentação: Arquivos e funções bem documentados para facilitar a compreensão e manutenção. (Docstring)
- Segurança: Uso de Connections e Variables do Airflow para gerenciar credenciais de forma segura.
- Reutilização: Funções auxiliares são projetadas para serem reutilizadas em diferentes DAGs.
- Monitoramento e Alertas: Implementação de notificações por email para sucessos e falhas.
- Idempotência e Tolerância a Falhas: Configuração de retries e lógica para evitar duplicações e lidar com falhas temporárias.