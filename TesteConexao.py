"""
DAG de teste — verifica a conexão com o SQL Server (DW_SAFEPAR).

Usa a Connection 'dw_safepar' cadastrada no Airflow (Admin > Connections).
Roda manualmente. Conecta, executa uma query simples e mostra o resultado
no log. Se funcionar aqui, a conexão está pronta para as DAGs de produção.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pyodbc


def get_conn():
    """Monta a conexão pyodbc a partir da Connection do Airflow."""
    c = BaseHook.get_connection("PN3PSFWBI01")
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={c.host};"
        f"DATABASE={c.schema};"
        f"UID={c.login};"
        f"PWD={c.password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def testar_conexao():
    print("Tentando conectar no SQL Server...")

    conn = get_conn()
    print("  ✓ Conexão aberta com sucesso!")

    cursor = conn.cursor()

    # Query simples que sempre funciona — confirma que dá pra consultar
    cursor.execute("SELECT @@VERSION AS versao, DB_NAME() AS banco_atual")
    linha = cursor.fetchone()

    print("-" * 50)
    print(f"Banco conectado: {linha.banco_atual}")
    print(f"Versão do SQL Server:\n{linha.versao}")
    print("-" * 50)

    conn.close()
    print("  ✓ Conexão fechada. TESTE OK.")


with DAG(
    dag_id="teste_conexao_sql",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # só roda quando você disparar
    catchup=False,
    tags=["teste", "sql"],
) as dag:

    PythonOperator(
        task_id="testar_conexao",
        python_callable=testar_conexao,
    )