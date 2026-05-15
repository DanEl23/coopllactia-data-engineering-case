"""
Fase 2: Geração de Dados Sintéticos - Coopllactia
===================================================

Script modularizado para gerar dados sintéticos realistas, respeitando o contrato
de dados (data_contract.md) e injetando anomalias propositais para testar
pipelines de limpeza (Fase 3).

Estratégia:
1. Gera dimensões primeiro (sem chaves estrangeiras)
2. Gera fato de coleta respeitando os limites operacionais
3. Gera fato de processamento DERIVADA das coletas (garantindo causalidade física e temporal)
4. Injeta erros controlados para simular dados reais problemáticos

Anomalias Injetadas:
- 5% de temperaturas nulas em fact_coleta
- 3% de acidez_ph fora do padrão (< 6.6 ou > 6.8) em fact_processamento
- 2 IDs de cooperados inválidos em fact_coleta (quebra de FK)
"""

import pandas as pd
import numpy as np
from faker import Faker
import uuid
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================================
# CONFIGURAÇÃO E LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Definir seed para reprodutibilidade
SEED = 42
np.random.seed(SEED)
Faker.seed(SEED)

# Configuração de diretórios
DATA_DIR = Path(__file__).parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CONSTANTES E CONFIGURAÇÕES
# ============================================================================

# Municípios da Bacia Leiteira de Minas Gerais
MUNICIPIOS_MG = [
    "Sete Lagoas", "Pompéu", "Pará de Minas", "Arcos", "Formiga",
    "Luz", "Cachoeira da Prata", "Itaguara", "Córrego d'Anta", "Mateus Leme",
    "Piracema", "Bom Despacho", "Moema", "Resende Costa", "Divinópolis"
]

# Quantidade de registros base
NUM_COOPERADOS = 50
NUM_VEICULOS = 15
NUM_COLETAS = 500

# Anomalias (proporções)
PCT_TEMPERATURA_NULA = 0.05  # 5%
PCT_ACIDEZ_INVALIDA = 0.03   # 3%
NUM_FK_INVALIDAS = 2           # IDs de cooperados que não existem

# Capacidades de carga padronizadas
CAPACIDADES_VEICULO = [5000, 10000, 15000]

# ============================================================================
# GERAÇÃO DE DIM_COOPERADO (Dimensão de Fornecedores)
# ============================================================================

def generate_dim_cooperado(n=NUM_COOPERADOS):
    logger.info(f"Gerando {n} cooperados...")
    fake = Faker('pt_BR')
    
    data = {
        'id_cooperado': range(1, n + 1),
        'nome_fazenda': [f"{fake.word().title()} {fake.last_name()}" for _ in range(n)],
        'municipio': np.random.choice(MUNICIPIOS_MG, n),
        'estado': ['MG'] * n,
        'capacidade_litros_dia': np.round(np.random.uniform(500, 14999, n), 2).astype(float)
    }
    df = pd.DataFrame(data)
    logger.info(f"✓ {n} cooperados gerados")
    return df

# ============================================================================
# GERAÇÃO DE DIM_VEICULO (Dimensão Logística)
# ============================================================================

def generate_dim_veiculo(n=NUM_VEICULOS):
    logger.info(f"Gerando {n} veículos...")
    
    def gerar_placa_mercosul():
        letras1 = ''.join(np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 3))
        digito1 = np.random.randint(0, 10)
        letra2 = np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
        digitos2 = ''.join(map(str, np.random.choice(range(10), 2)))
        return f"{letras1}{digito1}{letra2}{digitos2}"
    
    data = {
        'id_veiculo': range(1, n + 1),
        'placa': [gerar_placa_mercosul() for _ in range(n)],
        'capacidade_carga_l': np.random.choice(CAPACIDADES_VEICULO, n),
        'custo_fixo_km': np.round(np.random.uniform(2.5, 8.0, n), 2).astype(float)
    }
    df = pd.DataFrame(data)
    logger.info(f"✓ {n} veículos gerados")
    return df

# ============================================================================
# GERAÇÃO DE FACT_COLETA (Fato Transacional)
# ============================================================================

def generate_fact_coleta(dim_cooperado, dim_veiculo, n=NUM_COLETAS):
    logger.info(f"Gerando {n} coletas (com anomalias)...")
    
    cooperados_validos = dim_cooperado['id_cooperado'].tolist()
    veiculos_validos = dim_veiculo['id_veiculo'].tolist()
    veiculo_capacidade = dict(zip(dim_veiculo['id_veiculo'], dim_veiculo['capacidade_carga_l']))
    
    rows = []
    data_base = datetime.now() - timedelta(days=30)
    
    for i in range(n):
        id_veiculo = np.random.choice(veiculos_validos)
        capacidade_veiculo = veiculo_capacidade[id_veiculo]
        
        # Injetar FK inválida nos primeiros registros
        if i < NUM_FK_INVALIDAS:
            id_cooperado = np.random.randint(max(cooperados_validos) + 1, max(cooperados_validos) + 100)
        else:
            id_cooperado = np.random.choice(cooperados_validos)
            
        dias_atras = np.random.randint(0, 30)
        horas_atras = np.random.randint(0, 24)
        data_hora = data_base + timedelta(days=dias_atras, hours=horas_atras)
        
        volume = np.random.uniform(100, capacidade_veiculo * 0.95)
        
        # Anomalia: Temperatura Nula ou fora do padrão
        if np.random.uniform(0, 1) < PCT_TEMPERATURA_NULA:
            temperatura = None
        else:
            if np.random.uniform(0, 1) < 0.05:
                temperatura = np.random.choice([np.random.uniform(-5, 2), np.random.uniform(7, 12)])
            else:
                temperatura = np.round(np.random.uniform(2.0, 6.0), 1)
                
        distancia = np.round(np.random.uniform(5, 250), 1)
        
        rows.append({
            'id_coleta': str(uuid.uuid4()),
            'id_cooperado': int(id_cooperado),
            'id_veiculo': int(id_veiculo),
            'data_hora_coleta': data_hora,
            'volume_coletado_l': np.round(volume, 2),
            'temperatura_c': temperatura if temperatura is None else np.round(temperatura, 1),
            'distancia_percorrida_km': distancia
        })
        
    df = pd.DataFrame(rows)
    
    num_temp_nula = df['temperatura_c'].isna().sum()
    num_fk_invalida = (~df['id_cooperado'].isin(cooperados_validos)).sum()
    
    logger.info(f"✓ {n} coletas geradas")
    logger.info(f"  └─ Anomalias injetadas: Temperaturas nulas: {num_temp_nula} | FKs inválidas: {num_fk_invalida}")
    
    return df

# ============================================================================
# GERAÇÃO DE FACT_PROCESSAMENTO (Derivada para garantir Causalidade)
# ============================================================================

def generate_fact_processamento(df_coleta):
    """
    Gera tabela de processamento baseada exatamente nas coletas realizadas.
    Agrupa as coletas por veículo e data para criar um 'lote' lógico de chegada na fábrica.
    """
    logger.info("Gerando processamentos com base nas rotas de coleta (Causalidade Ativa)...")
    
    # Derivar a data sem a hora para agrupar o "lote do dia" por caminhão
    df_lotes = df_coleta.copy()
    df_lotes['data_recebimento'] = df_lotes['data_hora_coleta'].dt.date
    
    # Agrupar veículos por dia (Cada linha resultante é um caminhão descarregando na fábrica)
    lotes_unicos = df_lotes.groupby(['id_veiculo', 'data_recebimento']).size().reset_index()
    
    rows = []
    motivos_rejeicao = ['Acidez', 'Temperatura', 'Contaminação', 'Volume Insuficiente']
    
    for _, row in lotes_unicos.iterrows():
        id_veiculo = row['id_veiculo']
        data_recebimento = row['data_recebimento']
        
        # Anomalia: 3% de acidez fora do padrão
        if np.random.uniform(0, 1) < PCT_ACIDEZ_INVALIDA:
            acidez_ph = np.random.choice([
                np.round(np.random.uniform(5.5, 6.59), 2), 
                np.round(np.random.uniform(6.81, 7.5), 2)
            ])
            status_lote = 'Rejeitado'
            motivo_rejeicao = 'Acidez'
        else:
            acidez_ph = np.round(np.random.uniform(6.6, 6.8), 2)
            if np.random.uniform(0, 1) < 0.05:
                status_lote = 'Rejeitado'
                motivo_rejeicao = np.random.choice(motivos_rejeicao[1:])
            else:
                status_lote = 'Aprovado'
                motivo_rejeicao = None
                
        rows.append({
            'id_lote_recebimento': str(uuid.uuid4()),
            'id_veiculo': id_veiculo,
            'data_recebimento': data_recebimento,
            'acidez_ph': acidez_ph,
            'status_lote': status_lote,
            'motivo_rejeicao': motivo_rejeicao
        })
        
    df = pd.DataFrame(rows)
    
    num_rejeicoes = (df['status_lote'] == 'Rejeitado').sum()
    num_acidez_invalida = ((df['acidez_ph'] < 6.6) | (df['acidez_ph'] > 6.8)).sum()
    
    logger.info(f"✓ {len(df)} lotes de processamento gerados (1 para cada rota de veículo/dia)")
    logger.info(f"  └─ Anomalias injetadas: Taxa de rejeição: {num_rejeicoes} | Acidez fora do padrão: {num_acidez_invalida}")
    
    return df

# ============================================================================
# SALVAMENTO EM CSV
# ============================================================================

def save_to_csv(dataframes_dict):
    logger.info(f"\nSalvando dados em CSV em: {DATA_DIR}")
    for nome_tabela, df in dataframes_dict.items():
        filepath = DATA_DIR / f"{nome_tabela}.csv"
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"✓ {filepath.name} ({len(df)} registros)")
    logger.info(f"✓ Dados salvos com sucesso!")

# ============================================================================
# FUNÇÃO MAIN
# ============================================================================

def main():
    logger.info("=" * 70)
    logger.info("FASE 2: GERAÇÃO DE DADOS SINTÉTICOS - COOPLLACTIA (Causalidade Ativa)")
    logger.info("=" * 70)
    
    dim_cooperado = generate_dim_cooperado(NUM_COOPERADOS)
    dim_veiculo = generate_dim_veiculo(NUM_VEICULOS)
    fact_coleta = generate_fact_coleta(dim_cooperado, dim_veiculo, NUM_COLETAS)
    
    # A Geração do processamento agora recebe diretamente as coletas realizadas
    fact_processamento = generate_fact_processamento(fact_coleta)
    
    dataframes = {
        'dim_cooperado': dim_cooperado,
        'dim_veiculo': dim_veiculo,
        'fact_coleta': fact_coleta,
        'fact_processamento': fact_processamento
    }
    save_to_csv(dataframes)
    logger.info("\n✓ Pipeline Fase 2 concluído com sucesso!")

if __name__ == '__main__':
    main()
