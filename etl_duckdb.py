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
    
    # Conecta a um banco persistente local
    con = duckdb.connect(database='coopllactia.db', read_only=False)
    
    # ---------------------------------------------------------
    # 1. CAMADA SILVER: Tratamento de Qualidade (Data Cleansing)
    # ---------------------------------------------------------
    logger.info("Criando Camada Silver (Tratamento de Nulos e Integridade)...")
    
    # Limpeza da Fato Coleta: 
    # - Filtra as FKs inválidas (INNER JOIN elimina os 2 registros fantasmas)
    # - Imputa temperatura nula com um valor padrão (4.0 - ideal) e cria flag de qualidade
    con.execute(f"""
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
        FROM read_csv_auto('{RAW_DIR}/fact_coleta.csv') c
        INNER JOIN read_csv_auto('{RAW_DIR}/dim_cooperado.csv') dim_c 
            ON c.id_cooperado = dim_c.id_cooperado;
    """)
    
    # Silver Processamento: Apenas leitura formatada
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_fact_processamento AS
        SELECT * FROM read_csv_auto('{RAW_DIR}/fact_processamento.csv')
    """)
    
    # ---------------------------------------------------------
    # 2. CAMADA GOLD: Modelagem de Negócio (Redução de Custos)
    # ---------------------------------------------------------
    logger.info("Criando Camada Gold (Data Marts de Negócio)...")
    
    # Data Mart 1: Eficiência de Fornecedores (Taxa de Rejeição)
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_kpi_fornecedor AS
        WITH BaseColeta AS (
            SELECT 
                c.id_cooperado,
                CAST(c.data_hora_coleta AS DATE) AS data_operacao,
                c.id_veiculo,
                SUM(c.volume_coletado_l) AS volume_enviado
            FROM silver_fact_coleta c
            GROUP BY 1, 2, 3
        )
        SELECT 
            dim.nome_fazenda,
            dim.municipio,
            COUNT(DISTINCT b.data_operacao) AS dias_coleta,
            SUM(b.volume_enviado) AS volume_total_lt,
            SUM(CASE WHEN p.status_lote = 'Rejeitado' THEN b.volume_enviado ELSE 0 END) AS volume_perdido_lt,
            ROUND(SUM(CASE WHEN p.status_lote = 'Rejeitado' THEN b.volume_enviado ELSE 0 END) / SUM(b.volume_enviado) * 100, 2) AS taxa_rejeicao_pct
        FROM BaseColeta b
        INNER JOIN read_csv_auto('{RAW_DIR}/dim_cooperado.csv') dim ON b.id_cooperado = dim.id_cooperado
        LEFT JOIN silver_fact_processamento p 
            ON b.id_veiculo = p.id_veiculo AND b.data_operacao = p.data_recebimento
        GROUP BY 1, 2;
    """)
    
    # Data Mart 2: Eficiência Logística (Custo do Frete Ocioso)
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_kpi_logistica AS
        SELECT 
            v.placa,
            v.capacidade_carga_l,
            COUNT(c.id_coleta) AS qtd_paradas_coleta,
            SUM(c.distancia_percorrida_km) AS km_total_rodado,
            ROUND(SUM(c.distancia_percorrida_km) * v.custo_fixo_km, 2) AS custo_total_frete_rs,
            ROUND(SUM(c.volume_coletado_l) / (COUNT(DISTINCT CAST(c.data_hora_coleta AS DATE)) * v.capacidade_carga_l) * 100, 2) AS taxa_ocupacao_tanque_pct
        FROM silver_fact_coleta c
        INNER JOIN read_csv_auto('{RAW_DIR}/dim_veiculo.csv') v ON c.id_veiculo = v.id_veiculo
        GROUP BY 1, 2, v.custo_fixo_km;
    """)

    # ---------------------------------------------------------
    # 3. EXPORTAÇÃO: Salvando no Data Lake (Parquet)
    # ---------------------------------------------------------
    logger.info("Exportando Data Marts Gold para formato Parquet...")
    con.execute(f"COPY (SELECT * FROM gold_kpi_fornecedor) TO '{GOLD_DIR}/gold_kpi_fornecedor.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM gold_kpi_logistica) TO '{GOLD_DIR}/gold_kpi_logistica.parquet' (FORMAT PARQUET);")
    
    con.close()
    logger.info("✓ Pipeline finalizado com sucesso! Dados analíticos prontos no diretório /gold.")

if __name__ == '__main__':
    run_pipeline()