"""
DAG ETL Chat/WhatsApp — Atendimentos (Suporte.AtendimentosChatWhats)

Fluxo (staging + merge):
  1. limpar_staging       -> TRUNCATE da tabela de staging
  2. extrair_e_carregar   -> busca 7 dias da API e insere na staging
  3. merge_final          -> MERGE staging -> tabela final (insere/atualiza)

Roda diariamente. Cada etapa tem log próprio; se falhar, fica vermelha
no Airflow e o erro aparece no log daquela etapa. Retry automático 2x.

Credenciais:
  - Banco: Connection 'PN3PSFWBI01'
  - API Orpen: Connection 'orpen_api' (token na senha, user_id no extra)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import pyodbc
import pandas as pd

import utils   # usado apenas para buscar_atendimentos_dia e inserir_staging


# ─────────── CONFIG ───────────
REPORT_ID = 97
TABELA_STAGING = "[DW_Safepar].[staging].[AtendimentosChatWhats]"

# colunas esperadas no relatório (na ordem)
COLUNAS = [
    'Tipo de Atendimento', 'Canal', 'Protocolo', 'Fila', 'Nome Fila',
    'Filas', 'Total de Filas', 'Primeira mensagem do cliente',
    'Fim Atendimento', 'Tempo Atendimento', 'ID Contato', 'Origem',
    'Entrada', 'Primeiro agente', 'Nome primeiro agente', 'Agentes',
    'Total Agentes', 'Início do atendimento humano',
    'Fim do atendimento humano', 'Total de mensagens do cliente',
    'Total de mensagens do agente', 'Total de mensagens do bot',
    'Total de mensagens', 'Tempo no bot', 'Tempo Fila',
    'Tempo de resposta (primeiro agente)', 'Tempo atendimento humano',
    'Tabulação', 'Substatus', 'Nota'
]


# ══════════════════════════════════════════════════════════════
# CONEXÃO — lê da Connection do Airflow (não usa o utils pra isso)
# ══════════════════════════════════════════════════════════════
def get_conn():
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


def _get_orpen_config():
    """Lê as credenciais da API da Connection 'orpen_api'."""
    c = BaseHook.get_connection("orpen_api")
    extra = json.loads(c.extra or "{}")
    return {
        "url": c.host,
        "user_id": extra.get("user_id"),
        "token": c.password,
    }


def _periodo_7dias():
    """Últimos 7 dias (6 dias atrás até hoje)."""
    hoje = datetime.today()
    return hoje - timedelta(days=7), hoje


# ══════════════════════════════════════════════════════════════
# ETAPA 1 — limpar a staging
# ══════════════════════════════════════════════════════════════
def limpar_staging():
    print(f"Limpando staging: {TABELA_STAGING}")

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {TABELA_STAGING};")
    conn.commit()
    conn.close()

    print("Staging limpa.")


# ══════════════════════════════════════════════════════════════
# ETAPA 2 — buscar da API (7 dias) e inserir na staging
# ══════════════════════════════════════════════════════════════
def extrair_e_carregar():
    inicio, fim = _periodo_7dias()
    cfg = _get_orpen_config()

    conn = get_conn()

    data_atual = inicio
    total_inserido = 0

    while data_atual <= fim:
        dia_str = data_atual.strftime("%d/%m/%Y")
        try:
            print(f"Processando {dia_str}...")
            json_data = utils.buscar_atendimentos_dia(
                data_atual, cfg["url"], cfg["user_id"], REPORT_ID, cfg["token"]
            )

            if "data" not in json_data or not json_data["data"]:
                print("  sem dados para essa data.")
            else:
                df = pd.DataFrame(json_data["data"][0])

                # dia sem atendimentos pode vir com DataFrame vazio ou
                # sem as colunas esperadas — nesses casos, pula o dia
                if df.empty or not all(col in df.columns for col in COLUNAS):
                    print("  sem atendimentos válidos nessa data.")
                else:
                    df = df[COLUNAS]
                    utils.inserir_staging(df, conn, TABELA_STAGING)
                    total_inserido += len(df)
                    print(f"  {len(df)} linhas inseridas.")

        except Exception as e:
            # dia com problema: registra no log e SEGUE para o próximo
            print(f"  aviso: pulando {dia_str} — {e}")

        data_atual += timedelta(days=1)

    conn.close()

    print("-" * 50)
    print(f"Total inserido na staging: {total_inserido} linhas")


# ══════════════════════════════════════════════════════════════
# ETAPA 3 — MERGE staging -> tabela final
# ══════════════════════════════════════════════════════════════
def merge_final():
    print("Executando MERGE staging -> final...")

    merge_sql = """
MERGE [DW_Safepar].[Suporte].[AtendimentosChatWhats] AS tgt
USING [DW_Safepar].[staging].[AtendimentosChatWhats] AS src
ON  tgt.[Protocolo] = src.[Protocolo]
AND tgt.[Fila]      = src.[Fila]

WHEN MATCHED
AND EXISTS (
    SELECT
        src.[Tipo de Atendimento], src.[Canal], src.[Nome Fila], src.[Filas],
        src.[Total de Filas], src.[Primeira mensagem do cliente],
        src.[Fim Atendimento], src.[Tempo Atendimento], src.[ID Contato],
        src.[Origem], src.[Entrada], src.[Primeiro agente],
        src.[Nome primeiro agente], src.[Agentes], src.[Total Agentes],
        src.[Início do atendimento humano], src.[Fim do atendimento humano],
        src.[Total de mensagens do cliente], src.[Total de mensagens do agente],
        src.[Total de mensagens do bot], src.[Total de mensagens],
        src.[Tempo no bot], src.[Tempo Fila],
        src.[Tempo de resposta (primeiro agente)], src.[Tempo atendimento humano],
        src.[Tabulação], src.[Substatus], src.[Nota]
    EXCEPT
    SELECT
        tgt.[Tipo de Atendimento], tgt.[Canal], tgt.[Nome Fila], tgt.[Filas],
        tgt.[Total de Filas], tgt.[Primeira mensagem do cliente],
        tgt.[Fim Atendimento], tgt.[Tempo Atendimento], tgt.[ID Contato],
        tgt.[Origem], tgt.[Entrada], tgt.[Primeiro agente],
        tgt.[Nome primeiro agente], tgt.[Agentes], tgt.[Total Agentes],
        tgt.[Início do atendimento humano], tgt.[Fim do atendimento humano],
        tgt.[Total de mensagens do cliente], tgt.[Total de mensagens do agente],
        tgt.[Total de mensagens do bot], tgt.[Total de mensagens],
        tgt.[Tempo no bot], tgt.[Tempo Fila],
        tgt.[Tempo de resposta (primeiro agente)], tgt.[Tempo atendimento humano],
        tgt.[Tabulação], tgt.[Substatus], tgt.[Nota]
)
THEN UPDATE SET
    tgt.[Tipo de Atendimento]              = src.[Tipo de Atendimento],
    tgt.[Canal]                            = src.[Canal],
    tgt.[Nome Fila]                        = src.[Nome Fila],
    tgt.[Filas]                            = src.[Filas],
    tgt.[Total de Filas]                   = src.[Total de Filas],
    tgt.[Primeira mensagem do cliente]     = src.[Primeira mensagem do cliente],
    tgt.[Fim Atendimento]                  = src.[Fim Atendimento],
    tgt.[Tempo Atendimento]                = src.[Tempo Atendimento],
    tgt.[ID Contato]                       = src.[ID Contato],
    tgt.[Origem]                           = src.[Origem],
    tgt.[Entrada]                          = src.[Entrada],
    tgt.[Primeiro agente]                  = src.[Primeiro agente],
    tgt.[Nome primeiro agente]             = src.[Nome primeiro agente],
    tgt.[Agentes]                          = src.[Agentes],
    tgt.[Total Agentes]                    = src.[Total Agentes],
    tgt.[Início do atendimento humano]     = src.[Início do atendimento humano],
    tgt.[Fim do atendimento humano]        = src.[Fim do atendimento humano],
    tgt.[Total de mensagens do cliente]    = src.[Total de mensagens do cliente],
    tgt.[Total de mensagens do agente]     = src.[Total de mensagens do agente],
    tgt.[Total de mensagens do bot]        = src.[Total de mensagens do bot],
    tgt.[Total de mensagens]               = src.[Total de mensagens],
    tgt.[Tempo no bot]                     = src.[Tempo no bot],
    tgt.[Tempo Fila]                       = src.[Tempo Fila],
    tgt.[Tempo de resposta (primeiro agente)] = src.[Tempo de resposta (primeiro agente)],
    tgt.[Tempo atendimento humano]         = src.[Tempo atendimento humano],
    tgt.[Tabulação]                        = src.[Tabulação],
    tgt.[Substatus]                        = src.[Substatus],
    tgt.[Nota]                             = src.[Nota]

WHEN NOT MATCHED BY TARGET THEN
INSERT (
    [Tipo de Atendimento], [Canal], [Protocolo], [Fila], [Nome Fila],
    [Filas], [Total de Filas], [Primeira mensagem do cliente],
    [Fim Atendimento], [Tempo Atendimento], [ID Contato], [Origem],
    [Entrada], [Primeiro agente], [Nome primeiro agente], [Agentes],
    [Total Agentes], [Início do atendimento humano],
    [Fim do atendimento humano], [Total de mensagens do cliente],
    [Total de mensagens do agente], [Total de mensagens do bot],
    [Total de mensagens], [Tempo no bot], [Tempo Fila],
    [Tempo de resposta (primeiro agente)], [Tempo atendimento humano],
    [Tabulação], [Substatus], [Nota]
)
VALUES (
    src.[Tipo de Atendimento], src.[Canal], src.[Protocolo], src.[Fila],
    src.[Nome Fila], src.[Filas], src.[Total de Filas],
    src.[Primeira mensagem do cliente], src.[Fim Atendimento],
    src.[Tempo Atendimento], src.[ID Contato], src.[Origem], src.[Entrada],
    src.[Primeiro agente], src.[Nome primeiro agente], src.[Agentes],
    src.[Total Agentes], src.[Início do atendimento humano],
    src.[Fim do atendimento humano], src.[Total de mensagens do cliente],
    src.[Total de mensagens do agente], src.[Total de mensagens do bot],
    src.[Total de mensagens], src.[Tempo no bot], src.[Tempo Fila],
    src.[Tempo de resposta (primeiro agente)], src.[Tempo atendimento humano],
    src.[Tabulação], src.[Substatus], src.[Nota]
);
"""

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(merge_sql)
    linhas = cursor.rowcount
    conn.commit()
    conn.close()

    print(f"MERGE concluído. Linhas afetadas: {linhas}")


# ══════════════════════════════════════════════════════════════
# DEFINIÇÃO DA DAG
# ══════════════════════════════════════════════════════════════
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_orpen_97",
    start_date=datetime(2026, 1, 1),
    schedule="0 6,12 * * *",     # todo dia às 6h
    catchup=False,
    default_args=default_args,
    tags=["etl", "orpen", "chat", "whatsapp"],
) as dag:

    t1 = PythonOperator(
        task_id="limpar_staging",
        python_callable=limpar_staging,
    )

    t2 = PythonOperator(
        task_id="extrair_e_carregar",
        python_callable=extrair_e_carregar,
    )

    t3 = PythonOperator(
        task_id="merge_final",
        python_callable=merge_final,
    )

    t1 >> t2 >> t3   # ordem: limpa -> carrega -> merge