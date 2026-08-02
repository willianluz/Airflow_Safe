import requests
from datetime import datetime
import pandas as pd
import pyodbc  # só pros tipos, não é obrigatório

#---------------Função para buscar 1 dia da API e virar DataFrame
def buscar_atendimentos_dia(
    current_date: datetime,
    url_base: str,
    user_id: str,
    report_id: str,
    token_b64: str,  # Base64 de "usuario:senha"
) -> dict:
    data_params = {
        "action": "report_json",
        "userId": user_id,
        "reportId": report_id,
        "startDate": current_date.strftime("%d/%m/%Y"),
        "endDate": current_date.strftime("%d/%m/%Y"),
    }

    headers = {
        "Authorization": f"Basic {token_b64}",
        "Accept": "application/json",
    }

    response = requests.get(url_base, params=data_params, headers=headers, timeout=300)
    response.raise_for_status()
    return response.json()


#-------------Inserir um DataFrame na tabela staging
def inserir_staging(df: pd.DataFrame, conn: pyodbc.Connection, tabela: str) -> None:
    if df.empty:
        return  # nada pra inserir

    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))

    sql_insert = f"""
        INSERT INTO {tabela} 
        VALUES ({placeholders})
    """

    cursor = conn.cursor()
    cursor.fast_executemany = True
    data = df.values.tolist()

    cursor.executemany(sql_insert, data)
    conn.commit()




# ---------------- CONEXÃO COM SQL SERVER ----------------
def get_conn_dw_safepar() -> pyodbc.Connection:
    """
    Cria e retorna uma conexão com o DW_SAFEPAR.
    """
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=P-N3P-SFW-BI01;"
        "DATABASE=DW_SAFEPAR;"
        "UID=s_coleta_dw;"
        "PWD=0w4iprxw0D;"  # se quiser deixar mais pro, pega isso de variável de ambiente
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ---------------- CONFIG DA API ----------------
URL_BASE_ORPEN = "https://safeweb.orpen.com.br/rcx/ContactCenter/messages_api.php"
USER_ID_ORPEN = 162
TOKEN_ORPEN = "b3JwZW5TYWZlV2ViOlR6akNhUlp5ZndpeUBAIyMh"
