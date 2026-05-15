"""
Fase 3: Pipeline ELT Moderno (DuckDB) - Coopllactia
===================================================
Ingere dados Raw, aplica regras do Data Contract (Limpeza - Silver)
e consolida métricas de negócio (Camada Gold).
"""

import duckdb
import logging
from pathlib import Path

# Configuração de Logging e Diretórios
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent / "data" / "raw"
GOLD_DIR = Path(__file__).parent / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

def run_pipeline():
    logger.info("Iniciando Pipeline DuckDB (Modern Data Stack)...")

    con = duckdb.connect(database='coopllactia.db', read_only=False)

    # ---------------------------------------------------------
    # 1. CAMADA SILVER: Tratamento de Qualidade (Data Cleansing)
    # ---------------------------------------------------------
    logger.info("Criando Camada Silver (Tratamento de Nulos e Integridade)...")

    # Materialize dimensions first so they can be referenced multiple times
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_dim_cooperado AS
        SELECT * FROM read_csv_auto('{RAW_DIR}/dim_cooperado.csv');
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_dim_veiculo AS
        SELECT * FROM read_csv_auto('{RAW_DIR}/dim_veiculo.csv');
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_fact_coleta AS
        SELECT * FROM read_csv_auto('{RAW_DIR}/fact_coleta.csv');
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_fact_processamento AS
        SELECT * FROM read_csv_auto('{RAW_DIR}/fact_processamento.csv');
    """)

    # Limpeza da Fato Coleta:
    # - INNER JOIN elimina registros com FK de cooperado inválida (orphans)
    # - Temperatura nula imputada com 4.0°C (valor ideal de transporte) + flag de qualidade
    # - Registros descartados são logados explicitamente
    total_raw = con.execute("SELECT COUNT(*) FROM raw_fact_coleta").fetchone()[0]

    con.execute("""
        CREATE OR REPLACE TABLE silver_fact_coleta AS
        SELECT
            c.id_coleta,
            c.id_cooperado,
            c.id_veiculo,
            c.data_hora_coleta,
            c.volume_coletado_l,
            COALESCE(c.temperatura_c, 4.0) AS temperatura_c,
            CASE WHEN c.temperatura_c IS NULL THEN 1 ELSE 0 END AS flag_sensor_falho,
            c.distancia_percorrida_km
        FROM raw_fact_coleta c
        INNER JOIN raw_dim_cooperado dim_c ON c.id_cooperado = dim_c.id_cooperado;
    """)

    total_silver = con.execute("SELECT COUNT(*) FROM silver_fact_coleta").fetchone()[0]
    descartados  = total_raw - total_silver
    logger.info(f"  └─ Silver Coleta: {total_raw} brutos → {total_silver} válidos "
                f"({descartados} descartados por FK inválida de cooperado)")

    con.execute("""
        CREATE OR REPLACE TABLE silver_fact_processamento AS
        SELECT * FROM raw_fact_processamento;
    """)

    # ---------------------------------------------------------
    # 2. CAMADA GOLD: Modelagem de Negócio
    # ---------------------------------------------------------
    logger.info("Criando Camada Gold (Data Marts de Negócio)...")

    # ── Data Mart 1: Eficiência de Fornecedores ──────────────────────────────
    #
    # CORREÇÃO DO JOIN (v1 usava id_veiculo + data como ponte entre coleta e
    # processamento, o que atribuía rejeições de forma incorreta quando múltiplos
    # cooperados usavam o mesmo caminhão no mesmo dia).
    #
    # Solução: o status do lote (aprovado/rejeitado) é uma propriedade do
    # VEÍCULO naquele dia, não do cooperado individualmente.  Calculamos a taxa
    # de rejeição como:
    #   (nº de dias em que o lote do veículo foi rejeitado /
    #    nº total de dias em que o cooperado teve coleta)
    #
    # Isso é honesto com o dado disponível: não temos rastreabilidade direta
    # entre id_coleta e id_lote_recebimento.
    con.execute("""
        CREATE OR REPLACE TABLE gold_kpi_fornecedor AS
        WITH coleta_diaria AS (
            -- Uma linha por cooperado × veículo × dia
            SELECT
                c.id_cooperado,
                c.id_veiculo,
                CAST(c.data_hora_coleta AS DATE) AS data_operacao,
                SUM(c.volume_coletado_l)          AS volume_enviado_lt
            FROM silver_fact_coleta c
            GROUP BY 1, 2, 3
        ),
        coleta_com_status AS (
            -- Enriquece cada linha com o status do lote daquele veículo naquele dia
            SELECT
                cd.id_cooperado,
                cd.data_operacao,
                cd.volume_enviado_lt,
                p.status_lote,
                p.motivo_rejeicao
            FROM coleta_diaria cd
            LEFT JOIN silver_fact_processamento p
                ON  cd.id_veiculo   = p.id_veiculo
                AND cd.data_operacao = p.data_recebimento
        )
        SELECT
            dim.nome_fazenda,
            dim.municipio,
            COUNT(DISTINCT cs.data_operacao)                                   AS dias_com_coleta,
            ROUND(SUM(cs.volume_enviado_lt), 2)                                AS volume_total_lt,
            -- Volume potencialmente perdido: assume perda proporcional quando o
            -- lote do caminhão é rejeitado (conservador; ideal seria rastreio por lote)
            ROUND(SUM(
                CASE WHEN cs.status_lote = 'Rejeitado'
                     THEN cs.volume_enviado_lt ELSE 0 END
            ), 2)                                                              AS volume_risco_lt,
            ROUND(
                SUM(CASE WHEN cs.status_lote = 'Rejeitado' THEN cs.volume_enviado_lt ELSE 0 END)
                / NULLIF(SUM(cs.volume_enviado_lt), 0) * 100
            , 2)                                                               AS taxa_risco_pct,
            COUNT(CASE WHEN cs.status_lote = 'Rejeitado' THEN 1 END)          AS qtd_lotes_rejeitados,
            -- Motivo de rejeição mais frequente (moda simples)
            MODE(cs.motivo_rejeicao)                                           AS motivo_rejeicao_frequente
        FROM coleta_com_status cs
        INNER JOIN raw_dim_cooperado dim ON cs.id_cooperado = dim.id_cooperado
        GROUP BY dim.nome_fazenda, dim.municipio
        ORDER BY taxa_risco_pct DESC NULLS LAST;
    """)

    # ── Data Mart 2: Eficiência Logística ────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE gold_kpi_logistica AS
        SELECT
            v.placa,
            v.capacidade_carga_l,
            COUNT(c.id_coleta)                                                 AS qtd_paradas_coleta,
            COUNT(DISTINCT CAST(c.data_hora_coleta AS DATE))                   AS dias_operacao,
            ROUND(SUM(c.distancia_percorrida_km), 1)                           AS km_total_rodado,
            ROUND(SUM(c.distancia_percorrida_km) * v.custo_fixo_km, 2)        AS custo_total_frete_rs,
            -- Taxa de ocupação: volume médio coletado por dia / capacidade do tanque
            ROUND(
                SUM(c.volume_coletado_l)
                / NULLIF(COUNT(DISTINCT CAST(c.data_hora_coleta AS DATE)) * v.capacidade_carga_l, 0)
                * 100
            , 2)                                                               AS taxa_ocupacao_pct,
            -- Custo por litro transportado (métrica de eficiência)
            ROUND(
                SUM(c.distancia_percorrida_km) * v.custo_fixo_km
                / NULLIF(SUM(c.volume_coletado_l), 0)
            , 4)                                                               AS custo_por_litro_rs,
            SUM(c.flag_sensor_falho)                                           AS alertas_sensor_temp
        FROM silver_fact_coleta c
        INNER JOIN raw_dim_veiculo v ON c.id_veiculo = v.id_veiculo
        GROUP BY v.placa, v.capacidade_carga_l, v.custo_fixo_km
        ORDER BY taxa_ocupacao_pct DESC;
    """)

    # ---------------------------------------------------------
    # 3. EXPORTAÇÃO: Data Lake (Parquet)
    # ---------------------------------------------------------
    logger.info("Exportando Data Marts Gold para formato Parquet...")
    con.execute(f"COPY (SELECT * FROM gold_kpi_fornecedor) TO '{GOLD_DIR}/gold_kpi_fornecedor.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM gold_kpi_logistica)  TO '{GOLD_DIR}/gold_kpi_logistica.parquet'  (FORMAT PARQUET);")

    # ---------------------------------------------------------
    # 4. SUMÁRIO DO PIPELINE
    # ---------------------------------------------------------
    n_forn = con.execute("SELECT COUNT(*) FROM gold_kpi_fornecedor").fetchone()[0]
    n_vei  = con.execute("SELECT COUNT(*) FROM gold_kpi_logistica").fetchone()[0]
    logger.info(f"✓ Pipeline finalizado com sucesso!")
    logger.info(f"  └─ gold_kpi_fornecedor: {n_forn} cooperados analisados")
    logger.info(f"  └─ gold_kpi_logistica:  {n_vei}  veículos analisados")
    logger.info(f"  └─ Parquets em: {GOLD_DIR}")

    con.close()

if __name__ == '__main__':
    run_pipeline()
