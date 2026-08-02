"""
DAG ETL Orpen — Relatório de Atendimentos (Suporte.R1)

Fluxo:
  1. limpar_periodo   -> apaga no banco os registros do mês atual
  2. extrair_e_carregar -> busca dia a dia da API Orpen e insere no banco

Roda diariamente. Se qualquer etapa falhar, ela fica vermelha no Airflow
e o erro completo aparece no log daquela etapa. Configurado para tentar
novamente 2 vezes antes de desistir (retries).

Credenciais:
  - Banco: Connection 'PN3PSFWBI01'  (usada dentro do utils.get_conn_dw_safepar)
  - API Orpen: Connection 'orpen_api' (host + user_id no extra + token na senha)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import calendar
import json
import pandas as pd

import utils   # nosso utils.py na pasta de DAGs


# tabela de destino
TABELA = "[dw_safepar].[suporte].[R1]"
REPORT_ID = 1


def _get_orpen_config():
    """Lê as credenciais da API da Connection 'orpen_api'."""
    c = BaseHook.get_connection("orpen_api")
    extra = json.loads(c.extra or "{}")
    return {
        "url": c.host,                    # URL completa da API
        "user_id": extra.get("user_id"),  # 162
        "token": c.password,              # token base64
    }


def _periodo_7dias():
    """
    Retorna (data_inicial, data_final) do período a processar.
    Pega os ÚLTIMOS 7 DIAS (de 6 dias atrás até hoje).
    """
    hoje = datetime.today()
    inicio = hoje - timedelta(days=6)   # 6 dias atrás + hoje = 7 dias
    fim = hoje
    return inicio, fim


# ══════════════════════════════════════════════════════════════
# ETAPA 1 — apagar o período atual no banco
# ══════════════════════════════════════════════════════════════
def limpar_periodo():
    inicio, fim = _periodo_7dias()
    print(f"Limpando período (últimos 7 dias): {inicio.date()} até {fim.date()}")

    conn = utils.get_conn_dw_safepar()
    cursor = conn.cursor()

    sql = """
        DELETE FROM [dw_safepar].[Suporte].[R1]
        WHERE CAST([Data/Hora] AS date) BETWEEN ? AND ?;
    """
    cursor.execute(sql, inicio.date(), fim.date())
    conn.commit()
    print(f"Linhas apagadas: {cursor.rowcount}")

    conn.close()


# ══════════════════════════════════════════════════════════════
# ETAPA 2 — buscar da API dia a dia e inserir no banco
# ══════════════════════════════════════════════════════════════
def extrair_e_carregar():
    inicio, fim = _periodo_7dias()
    cfg = _get_orpen_config()

    conn = utils.get_conn_dw_safepar()

    data_atual = inicio
    total_inserido = 0
    dias_com_erro = []

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
                # normaliza: pode vir dict ou lista
                if isinstance(json_data["data"], dict):
                    d = json_data["data"]
                elif isinstance(json_data["data"], list) and json_data["data"]:
                    d = json_data["data"][0]
                else:
                    d = {}

                # se depois de normalizar 'd' não for um dicionário
                # (ex.: dia sem atendimento vem como lista vazia), pula o dia
                if not isinstance(d, dict):
                    print("  sem atendimentos nessa data.")
                else:
                    linhas_dia = []
                    for pessoa, eventos in d.items():
                        for ev in eventos:
                            linhas_dia.append({
                                **ev,
                                "Pessoa": pessoa,
                                "Data_Consulta": data_atual.date(),
                            })

                    if linhas_dia:
                        df = pd.DataFrame(linhas_dia)
                        utils.inserir_staging(df, conn, TABELA)
                        total_inserido += len(df)
                        print(f"  {len(df)} linhas inseridas.")

        except Exception as e:
            # não derruba a DAG por causa de 1 dia — registra e segue
            print(f"  ERRO em {dia_str}: {e}")
            dias_com_erro.append(dia_str)

        data_atual += timedelta(days=1)

    conn.close()

    print("-" * 50)
    print(f"Total inserido: {total_inserido} linhas")

    # Se algum dia falhou, derruba a tarefa no final para ficar visível
    if dias_com_erro:
        raise Exception(f"Falha em {len(dias_com_erro)} dia(s): {', '.join(dias_com_erro)}")


# ══════════════════════════════════════════════════════════════
# DEFINIÇÃO DA DAG
# ══════════════════════════════════════════════════════════════
default_args = {
    "retries": 2,                          # tenta 2x antes de desistir
    "retry_delay": timedelta(minutes=5),   # espera 5 min entre tentativas
}

with DAG(
    dag_id="etl_orpen_r1",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",     # todo dia às 6h da manhã
    catchup=False,
    default_args=default_args,
    tags=["etl", "orpen", "suporte"],
) as dag:

    t1 = PythonOperator(
        task_id="limpar_periodo",
        python_callable=limpar_periodo,
    )

    t2 = PythonOperator(
        task_id="extrair_e_carregar",
        python_callable=extrair_e_carregar,
    )

    t1 >> t2   # t1 roda primeiro, depois t2