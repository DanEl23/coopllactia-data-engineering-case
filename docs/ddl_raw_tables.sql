-- DDL para as tabelas de dados sintéticos (Camada Raw/Bronze)
-- Dialeto: ANSI SQL / DuckDB

CREATE TABLE dim_cooperado (
    id_cooperado INTEGER PRIMARY KEY,
    nome_fazenda VARCHAR NOT NULL,
    municipio VARCHAR NOT NULL,
    estado VARCHAR DEFAULT 'MG',
    capacidade_litros_dia FLOAT NOT NULL
);

CREATE TABLE dim_veiculo (
    id_veiculo INTEGER PRIMARY KEY,
    placa VARCHAR NOT NULL,
    capacidade_carga_l INTEGER NOT NULL,
    custo_fixo_km FLOAT NOT NULL
);

CREATE TABLE fact_coleta (
    id_coleta UUID PRIMARY KEY,
    id_cooperado INTEGER NOT NULL, -- FK referenciando dim_cooperado
    id_veiculo INTEGER NOT NULL,   -- FK referenciando dim_veiculo
    data_hora_coleta TIMESTAMP NOT NULL,
    volume_coletado_l FLOAT NOT NULL,
    temperatura_c FLOAT,           -- Permite NULL (simulação de falha de sensor)
    distancia_percorrida_km FLOAT NOT NULL
);

CREATE TABLE fact_processamento (
    id_lote_recebimento UUID PRIMARY KEY,
    id_veiculo INTEGER NOT NULL,   -- FK referenciando dim_veiculo
    data_recebimento DATE NOT NULL,
    acidez_ph FLOAT NOT NULL,
    status_lote VARCHAR NOT NULL,  -- 'Aprovado' ou 'Rejeitado'
    motivo_rejeicao VARCHAR        -- Preenchido apenas se status_lote = 'Rejeitado'
);