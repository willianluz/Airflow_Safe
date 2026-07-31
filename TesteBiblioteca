"""
DAG de diagnóstico — testa se as bibliotecas Python estão disponíveis no Airflow.

Roda manualmente (schedule=None). Tenta importar cada biblioteca e mostra
OK ou FALHA no log. Também lista os drivers ODBC de SQL Server encontrados.

Use para confirmar que a imagem personalizada do Airflow está com tudo pronto
antes de subir as DAGs de produção.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def testar_bibliotecas():
    # nome_do_import : nome_amigavel (alguns diferem do nome de instalação)
    bibliotecas = {
        "pandas": "pandas",
        "numpy": "numpy",
        "pyodbc": "pyodbc",
        "openpyxl": "openpyxl",
        "scipy": "scipy",
        "seaborn": "seaborn",
        "matplotlib": "matplotlib",
        "sqlalchemy": "sqlalchemy",
        "requests": "requests",
        "cloudinary": "cloudinary",
        "paramiko": "paramiko",
        "humanfriendly": "humanfriendly",
        "fastapi": "fastapi",
        "urllib3": "urllib3",
        "dotenv": "python-dotenv",
        "google.ads.googleads.client": "google-ads",
        "google_auth_oauthlib": "google-auth-oauthlib",
        "office365": "Office365-REST-Python-Client",
    }

    print("=" * 55)
    print("TESTE DE BIBLIOTECAS")
    print("=" * 55)

    ok = []
    falhas = []

    for modulo, nome_pip in bibliotecas.items():
        try:
            __import__(modulo)
            print(f"  OK    -> {nome_pip}")
            ok.append(nome_pip)
        except Exception as e:
            print(f"  FALHA -> {nome_pip}  ({e})")
            falhas.append(nome_pip)

    print("-" * 55)
    print(f"Total OK: {len(ok)}  |  Falhas: {len(falhas)}")

    # Teste do driver ODBC do SQL Server
    print("-" * 55)
    try:
        import pyodbc
        drivers_sql = [d for d in pyodbc.drivers() if "SQL Server" in d]
        if drivers_sql:
            print("Drivers SQL Server encontrados:")
            for d in drivers_sql:
                print(f"  -> {d}")
        else:
            print("NENHUM driver de SQL Server encontrado!")
    except Exception as e:
        print(f"Nao foi possivel checar drivers ODBC: {e}")

    print("=" * 55)

    # Se houver qualquer falha, derruba a tarefa para ficar visível (vermelho)
    if falhas:
        raise Exception(f"Bibliotecas faltando: {', '.join(falhas)}")

    print("TUDO CERTO — ambiente pronto.")


with DAG(
    dag_id="teste_bibliotecas",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # só roda quando você disparar manualmente
    catchup=False,
    tags=["teste", "diagnostico"],
) as dag:

    PythonOperator(
        task_id="testar_bibliotecas",
        python_callable=testar_bibliotecas,
    )