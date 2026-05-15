#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline: Bronze → Silver → Gold
Coopllactia Data Engineering PoC (Phase 3)

Executa validações de qualidade de dados e gera layers Parquet finais.
Implementa Shift-Left Quality: validações declarativas baseadas em data_contract.md

Uso:
    python etl_duckdb.py
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# ================================================================
# LOGGING
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ================================================================
# PATHS
# ================================================================
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
DOCS_DIR = Path("docs")

for d in [SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ================================================================
# DATABASE (In-Memory DuckDB)
# ================================================================
DB = duckdb.connect(":memory:")


# ================================================================
# STEP 1: LOAD BRONZE LAYER (CSV Files)
# ================================================================
def load_bronze():
    """Load raw CSV files into DuckDB Bronze (raw) schema."""
    logger.info("═" * 70)
    logger.info("STEP 1: LOAD BRONZE LAYER (Raw CSV)")
    logger.info("═" * 70)

    try:
        DB.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Load Cooperado
        path = RAW_DIR / "dim_cooperado.csv"
        logger.info(f"Loading: {path}")
        df_coop = pd.read_csv(path)
        DB.register("dim_cooperado_temp", df_coop)
        DB.execute("""
            CREATE TABLE raw.dim_cooperado AS
            SELECT * FROM dim_cooperado_temp
        """)
        logger.info(f"  ✓ {len(df_coop)} cooperados loaded")

        # Load Veiculo
        path = RAW_DIR / "dim_veiculo.csv"
        logger.info(f"Loading: {path}")
        df_veic = pd.read_csv(path)
        DB.register("dim_veiculo_temp", df_veic)
        DB.execute("""
            CREATE TABLE raw.dim_veiculo AS
            SELECT * FROM dim_veiculo_temp
        """)
        logger.info(f"  ✓ {len(df_veic)} veículos loaded")

        # Load Coleta
        path = RAW_DIR / "fact_coleta.csv"
        logger.info(f"Loading: {path}")
        df_col = pd.read_csv(path)
        DB.register("fact_coleta_temp", df_col)
        DB.execute("""
            CREATE TABLE raw.fact_coleta AS
            SELECT * FROM fact_coleta_temp
        """)
        logger.info(f"  ✓ {len(df_col)} coletas loaded (anomalies present)")

        # Load Processamento
        path = RAW_DIR / "fact_processamento.csv"
        logger.info(f"Loading: {path}")
        df_proc = pd.read_csv(path)
        DB.register("fact_processamento_temp", df_proc)
        DB.execute("""
            CREATE TABLE raw.fact_processamento AS
            SELECT * FROM fact_processamento_temp
        """)
        logger.info(f"  ✓ {len(df_proc)} processamentos loaded (anomalies present)")

        logger.info("✓ All Bronze tables loaded successfully\n")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to load Bronze: {e}")
        return False


# ================================================================
# STEP 2: DATA QUALITY VALIDATION
# ================================================================
def validate_data_contracts():
    """Validate Bronze layer against data_contract.md constraints."""
    logger.info("═" * 70)
    logger.info("STEP 2: DATA QUALITY VALIDATION (Shift-Left)")
    logger.info("═" * 70)

    issues = []

    # ================================================================
    # Constraint 1: Temperatura esperada BETWEEN 2.0-6.0
    # ================================================================
    logger.info("Validating: fact_coleta.temperatura_c [EXPECTED: 2.0-6.0°C]")
    result = DB.execute("""
        SELECT
            id_coleta,
            temperatura_c,
            'TEMP_OUT_OF_RANGE' as issue_type
        FROM raw.fact_coleta
        WHERE temperatura_c IS NOT NULL
          AND (temperatura_c < 2.0 OR temperatura_c > 6.0)
    """).fetchall()

    if result:
        logger.warning(f"  ⚠ {len(result)} records with temperature outside 2.0-6.0°C range")
        for row in result:
            issues.append({
                "table": "fact_coleta",
                "record_id": row[0],
                "issue": "TEMP_OUT_OF_RANGE",
                "details": f"temp={row[1]}°C"
            })

    # ================================================================
    # Constraint 2: Temperatura nula (Sensor falha)
    # ================================================================
    logger.info("Validating: fact_coleta.temperatura_c [NULL allowed but tracked]")
    result = DB.execute("""
        SELECT COUNT(*) as count
        FROM raw.fact_coleta
        WHERE temperatura_c IS NULL
    """).fetchone()

    null_temps = result[0] if result else 0
    if null_temps > 0:
        logger.warning(f"  ⚠ {null_temps} coletas com temperatura nula (sensor failure or data gap)")
        issues.append({
            "table": "fact_coleta",
            "issue": "TEMP_MISSING",
            "count": null_temps
        })

    # ================================================================
    # Constraint 3: Acidez esperada BETWEEN 6.6-6.8 (Quality Gate)
    # ================================================================
    logger.info("Validating: fact_processamento.acidez_ph [EXPECTED: 6.6-6.8]")
    result = DB.execute("""
        SELECT
            id_lote_recebimento,
            acidez_ph,
            'ACIDEZ_OUT_OF_SPEC' as issue_type
        FROM raw.fact_processamento
        WHERE acidez_ph < 6.6 OR acidez_ph > 6.8
    """).fetchall()

    if result:
        logger.warning(f"  ⚠ {len(result)} recordes com acidez fora da especificação (6.6-6.8)")
        for row in result:
            issues.append({
                "table": "fact_processamento",
                "record_id": row[0],
                "issue": "ACIDEZ_OUT_OF_SPEC",
                "details": f"ph={row[1]}"
            })

    # ================================================================
    # Constraint 4: Foreign Key Integrity
    # ================================================================
    logger.info("Validating: Foreign Key Integrity (Referential)")
    result = DB.execute("""
        SELECT COUNT(*) as orphan_count
        FROM raw.fact_coleta c
        LEFT JOIN raw.dim_cooperado coop ON c.id_cooperado = coop.id_cooperado
        WHERE coop.id_cooperado IS NULL
    """).fetchone()

    orphan_coops = result[0] if result else 0
    if orphan_coops > 0:
        logger.warning(f"  ⚠ {orphan_coops} coletas com id_cooperado orfão (referential integrity failure)")
        issues.append({
            "table": "fact_coleta",
            "issue": "ORPHAN_FK_COOPERADO",
            "count": orphan_coops
        })

    result = DB.execute("""
        SELECT COUNT(*) as orphan_count
        FROM raw.fact_coleta c
        LEFT JOIN raw.dim_veiculo v ON c.id_veiculo = v.id_veiculo
        WHERE v.id_veiculo IS NULL
    """).fetchone()

    orphan_veics = result[0] if result else 0
    if orphan_veics > 0:
        logger.warning(f"  ⚠ {orphan_veics} coletas com id_veiculo orfão")
        issues.append({
            "table": "fact_coleta",
            "issue": "ORPHAN_FK_VEICULO",
            "count": orphan_veics
        })

    # ================================================================
    # Summary
    # ================================================================
    logger.info(f"\n✓ Validation complete: {len(issues)} distinct issue patterns found")
    logger.info(f"  → Full data will be persisted to Silver (no hard stops)")
    logger.info(f"  → Issues logged for debugging and root cause analysis\n")

    return issues


# ================================================================
# STEP 3: GENERATE SILVER LAYER (Clean, Validated Parquet)
# ================================================================
def generate_silver():
    """Generate Silver layer with data quality annotations."""
    logger.info("═" * 70)
    logger.info("STEP 3: GENERATE SILVER LAYER (Parquet)")
    logger.info("═" * 70)

    try:
        # ================================================================
        # Silver Dim Cooperado
        # ================================================================
        logger.info("Generating: silver/dim_cooperado.parquet")
        DB.execute("""
            CREATE TABLE silver.dim_cooperado AS
            SELECT
                id_cooperado,
                nome_fazenda,
                municipio,
                estado,
                capacidade_litros_dia,
                CURRENT_TIMESTAMP() as loaded_at
            FROM raw.dim_cooperado
            ORDER BY id_cooperado
        """)
        df = DB.execute("SELECT * FROM silver.dim_cooperado").fetch_df()
        parquet_path = SILVER_DIR / "dim_cooperado.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} rows → {parquet_path.name}")

        # ================================================================
        # Silver Dim Veiculo
        # ================================================================
        logger.info("Generating: silver/dim_veiculo.parquet")
        DB.execute("""
            CREATE TABLE silver.dim_veiculo AS
            SELECT
                id_veiculo,
                placa,
                capacidade_carga_l,
                custo_fixo_km,
                CURRENT_TIMESTAMP() as loaded_at
            FROM raw.dim_veiculo
            ORDER BY id_veiculo
        """)
        df = DB.execute("SELECT * FROM silver.dim_veiculo").fetch_df()
        parquet_path = SILVER_DIR / "dim_veiculo.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} rows → {parquet_path.name}")

        # ================================================================
        # Silver Fact Coleta (with imputation for NULLs)
        # ================================================================
        logger.info("Generating: silver/fact_coleta.parquet [IMPUTATION APPLIED]")
        logger.info("  → temperatura_c: NULL values imputed with 4.0°C (⚠ Known Issue #2)")
        DB.execute("""
            CREATE TABLE silver.fact_coleta AS
            SELECT
                id_coleta,
                id_cooperado,
                id_veiculo,
                data_hora_coleta,
                volume_coletado_l,
                COALESCE(temperatura_c, 4.0) as temperatura_c,
                CASE
                    WHEN temperatura_c IS NULL THEN 1
                    ELSE 0
                END as temperatura_imputed,
                distancia_percorrida_km,
                CURRENT_TIMESTAMP() as loaded_at
            FROM raw.fact_coleta
            ORDER BY data_hora_coleta DESC
        """)
        df = DB.execute("SELECT * FROM silver.fact_coleta").fetch_df()
        parquet_path = SILVER_DIR / "fact_coleta.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} rows (with 1 imputation flag) → {parquet_path.name}")

        # ================================================================
        # Silver Fact Processamento
        # ================================================================
        logger.info("Generating: silver/fact_processamento.parquet")
        DB.execute("""
            CREATE TABLE silver.fact_processamento AS
            SELECT
                id_lote_recebimento,
                id_veiculo,
                data_recebimento,
                acidez_ph,
                status_lote,
                motivo_rejeicao,
                CASE
                    WHEN acidez_ph < 6.6 OR acidez_ph > 6.8 THEN 1
                    ELSE 0
                END as acidez_out_of_spec,
                CURRENT_TIMESTAMP() as loaded_at
            FROM raw.fact_processamento
            ORDER BY data_recebimento DESC
        """)
        df = DB.execute("SELECT * FROM silver.fact_processamento").fetch_df()
        parquet_path = SILVER_DIR / "fact_processamento.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} rows (with quality flags) → {parquet_path.name}\n")

        logger.info("✓ Silver layer generated successfully")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to generate Silver: {e}")
        return False


# ================================================================
# STEP 4: GENERATE GOLD LAYER (KPIs & Aggregations)
# ================================================================
def generate_gold():
    """Generate Gold layer with KPIs."""
    logger.info("═" * 70)
    logger.info("STEP 4: GENERATE GOLD LAYER (KPIs)")
    logger.info("═" * 70)

    try:
        # ================================================================
        # KPI 1: Eficiência Logística (por Veículo)
        # ================================================================
        logger.info("Generating: gold/kpi_eficiencia_logistica.parquet")
        DB.execute("""
            CREATE TABLE gold.kpi_eficiencia_logistica AS
            SELECT
                v.id_veiculo,
                v.placa,
                COUNT(DISTINCT c.id_coleta) as total_coletas,
                SUM(c.volume_coletado_l) as volume_total_l,
                AVG(c.volume_coletado_l) as volume_medio_l,
                SUM(c.distancia_percorrida_km) as distancia_total_km,
                ROUND(
                    SUM(c.volume_coletado_l) / NULLIF(SUM(c.distancia_percorrida_km), 0),
                    2
                ) as eficiencia_l_por_km,
                COUNT(CASE WHEN c.temperatura_imputed = 1 THEN 1 END) as coletas_com_temp_imputed,
                CURRENT_TIMESTAMP() as calculated_at
            FROM silver.dim_veiculo v
            LEFT JOIN silver.fact_coleta c ON v.id_veiculo = c.id_veiculo
            GROUP BY v.id_veiculo, v.placa
            ORDER BY eficiencia_l_por_km DESC
        """)
        df = DB.execute("SELECT * FROM gold.kpi_eficiencia_logistica").fetch_df()
        parquet_path = GOLD_DIR / "kpi_eficiencia_logistica.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} veículos → {parquet_path.name}")
        logger.info(f"    Melhor eficiência: {df['eficiencia_l_por_km'].max():.2f} L/km")

        # ================================================================
        # KPI 2: Taxa de Rejeição (por Cooperado)
        # ================================================================
        logger.info("Generating: gold/kpi_rejeicoes_por_cooperado.parquet")
        DB.execute("""
            CREATE TABLE gold.kpi_rejeicoes_por_cooperado AS
            SELECT
                coop.id_cooperado,
                coop.nome_fazenda,
                coop.municipio,
                COUNT(DISTINCT c.id_coleta) as total_coletas,
                COUNT(DISTINCT CASE WHEN p.status_lote = 'Rejeitado' THEN p.id_lote_recebimento END) as total_rejeicoes,
                ROUND(
                    COUNT(DISTINCT CASE WHEN p.status_lote = 'Rejeitado' THEN p.id_lote_recebimento END)
                    / NULLIF(COUNT(DISTINCT c.id_coleta), 0) * 100,
                    2
                ) as taxa_rejeicao_pct,
                COUNT(DISTINCT CASE 
                    WHEN p.status_lote = 'Rejeitado' 
                    AND p.motivo_rejeicao LIKE '%temperatura%' 
                    THEN p.id_lote_recebimento 
                END) as rejeicoes_por_temperatura,
                COUNT(DISTINCT CASE 
                    WHEN p.status_lote = 'Rejeitado' 
                    AND p.motivo_rejeicao LIKE '%acidez%' 
                    THEN p.id_lote_recebimento 
                END) as rejeicoes_por_acidez,
                CURRENT_TIMESTAMP() as calculated_at
            FROM silver.dim_cooperado coop
            LEFT JOIN silver.fact_coleta c ON coop.id_cooperado = c.id_cooperado
            LEFT JOIN silver.fact_processamento p ON c.id_veiculo = p.id_veiculo
            GROUP BY coop.id_cooperado, coop.nome_fazenda, coop.municipio
            ORDER BY taxa_rejeicao_pct DESC
        """)
        df = DB.execute("SELECT * FROM gold.kpi_rejeicoes_por_cooperado").fetch_df()
        parquet_path = GOLD_DIR / "kpi_rejeicoes_por_cooperado.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: {len(df)} cooperados → {parquet_path.name}")
        logger.info(f"    Taxa média de rejeição: {df['taxa_rejeicao_pct'].mean():.2f}%")

        # ================================================================
        # KPI 3: Qualidade Agregada (Overall Dashboard)
        # ================================================================
        logger.info("Generating: gold/kpi_dashboard_geral.parquet")
        DB.execute("""
            CREATE TABLE gold.kpi_dashboard_geral AS
            SELECT
                CURRENT_DATE() as data_relatorio,
                (SELECT COUNT(*) FROM silver.dim_cooperado) as total_cooperados,
                (SELECT COUNT(*) FROM silver.dim_veiculo) as total_veiculos,
                (SELECT COUNT(*) FROM silver.fact_coleta) as total_coletas,
                (SELECT SUM(volume_coletado_l) FROM silver.fact_coleta) as volume_total_l,
                (SELECT AVG(volume_coletado_l) FROM silver.fact_coleta) as volume_medio_coleta_l,
                (SELECT COUNT(*) FROM silver.fact_processamento WHERE status_lote = 'Rejeitado') as total_rejeicoes,
                (SELECT COUNT(*) FROM silver.fact_processamento) as total_processamentos,
                ROUND(
                    (SELECT COUNT(*) FROM silver.fact_processamento WHERE status_lote = 'Rejeitado')
                    / NULLIF((SELECT COUNT(*) FROM silver.fact_processamento), 0) * 100,
                    2
                ) as taxa_rejeicao_geral_pct,
                ROUND(
                    (SELECT SUM(volume_coletado_l) 
                     FROM silver.fact_coleta 
                     WHERE temperatura_imputed = 1)
                    / NULLIF((SELECT SUM(volume_coletado_l) FROM silver.fact_coleta), 0) * 100,
                    2
                ) as pct_volume_com_temp_imputed,
                CURRENT_TIMESTAMP() as calculated_at
        """)
        df = DB.execute("SELECT * FROM gold.kpi_dashboard_geral").fetch_df()
        parquet_path = GOLD_DIR / "kpi_dashboard_geral.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"  ✓ Saved: Dashboard agregado → {parquet_path.name}")
        logger.info(f"    Volume total: {df['volume_total_l'].values[0]:,.0f} L")
        logger.info(f"    Taxa de rejeição: {df['taxa_rejeicao_geral_pct'].values[0]:.2f}%")

        logger.info(f"\n✓ Gold layer generated successfully\n")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to generate Gold: {e}")
        return False


# ================================================================
# STEP 5: SUMMARY & CLEANUP
# ================================================================
def print_summary():
    """Print execution summary."""
    logger.info("═" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("═" * 70)

    try:
        # ================================================================
        # File Sizes
        # ================================================================
        logger.info("\n📦 Output File Sizes:")
        for layer, directory in [("Silver", SILVER_DIR), ("Gold", GOLD_DIR)]:
            total_size = sum(f.stat().st_size for f in directory.glob("*.parquet"))
            logger.info(f"  {layer}: {total_size / (1024**2):.2f} MB")

        # ================================================================
        # Row Counts
        # ================================================================
        logger.info("\n📊 Row Counts:")
        results = DB.execute("""
            SELECT
                'raw.dim_cooperado' as layer,
                COUNT(*) as row_count FROM raw.dim_cooperado
            UNION ALL
            SELECT 'raw.dim_veiculo', COUNT(*) FROM raw.dim_veiculo
            UNION ALL
            SELECT 'raw.fact_coleta', COUNT(*) FROM raw.fact_coleta
            UNION ALL
            SELECT 'raw.fact_processamento', COUNT(*) FROM raw.fact_processamento
            UNION ALL
            SELECT 'silver.dim_cooperado', COUNT(*) FROM silver.dim_cooperado
            UNION ALL
            SELECT 'silver.fact_coleta', COUNT(*) FROM silver.fact_coleta
            UNION ALL
            SELECT 'gold.kpi_eficiencia_logistica', COUNT(*) FROM gold.kpi_eficiencia_logistica
            UNION ALL
            SELECT 'gold.kpi_rejeicoes_por_cooperado', COUNT(*) FROM gold.kpi_rejeicoes_por_cooperado
        """).fetch_df()

        for _, row in results.iterrows():
            logger.info(f"  {row['layer']:.<45} {row['row_count']:>6} rows")

        logger.info("\n" + "═" * 70)
        logger.info("✓ ETL Pipeline completed successfully!")
        logger.info("═" * 70 + "\n")

    except Exception as e:
        logger.error(f"Summary generation failed: {e}")


# ================================================================
# MAIN EXECUTION
# ================================================================
def main():
    """Execute the full ETL pipeline."""

    logger.info("\n")
    logger.info("╔" + "═" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + f"  ETL Pipeline: Bronze → Silver → Gold".center(68) + "║")
    logger.info("║" + "  Coopllactia Data Engineering PoC (Phase 3)".center(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "═" * 68 + "╝" + "\n")

    # Create schema if not exists
    DB.execute("CREATE SCHEMA IF NOT EXISTS silver")
    DB.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # Execute pipeline steps
    if not load_bronze():
        sys.exit(1)

    issues = validate_data_contracts()

    if not generate_silver():
        sys.exit(1)

    if not generate_gold():
        sys.exit(1)

    print_summary()

    logger.info("Next steps:")
    logger.info("  1. Load Gold KPIs to BI tool (Tableau, Power BI, etc.)")
    logger.info("  2. Monitor Known Issues (see README.md)")
    logger.info("  3. Implement incremental load strategy (⚠ Known Issue #1)")
    logger.info("\n")


if __name__ == "__main__":
    main()