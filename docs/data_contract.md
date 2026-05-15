# Data Contract - Coopllactia

## Índice
1. [Visão Geral](#visão-geral)
2. [Entidades do Modelo](#entidades-do-modelo)

---

## Visão Geral

### Contexto da Governança de Dados
Este documento formaliza o **Contrato de Dados** da Coopllactia, operacionalizando o modelo conceitual de negócio para garantir **Data Governance** (Governança de Dados) e **Data Quality** (Qualidade de Dados) *antes* da ingestão (**Shift-Left Data Quality**).

A Coopllactia é uma cooperativa agrícola focada na captação e processamento de leite bruto na Bacia Leiteira de Minas Gerais, com operações logísticas críticas onde a qualidade e eficiência determinam a lucratividade.

### Princípios de Design
- **Star Schema:** Modelagem dimensional com dimensões de contexto (Quem e Onde) e fatos transacionais (O Quê, Quando e Quanto)
- **Shift-Left Quality:** Validações e expectativas de qualidade documentadas *antes* da implementação
- **Data Lineage:** Rastreamento claro entre captação, logística e processamento

---

## Entidades do Modelo

### Entidade 1: `dim_cooperado` (Dimensão de Fornecedores)

**Descrição:** Cadastro dos produtores rurais que fornecem o leite bruto.  
**SLA de Atualização:** D-1 (Diário)

| Coluna | Tipo Físico | Chave | Restrição (Constraint) / Expectativa de Qualidade | Descrição de Negócio |
| --- | --- | --- | --- | --- |
| `id_cooperado` | INT | PK | `NOT NULL`, `UNIQUE` | Identificador único do produtor na cooperativa. |
| `nome_fazenda` | STRING | - | `NOT NULL` | Razão social ou nome fantasia da propriedade rural. |
| `municipio` | STRING | - | `NOT NULL` | Cidade sede da fazenda (ex: Sete Lagoas, Pompéu, Pará de Minas). |
| `estado` | STRING | - | `DEFAULT 'MG'` | Unidade Federativa. |
| `capacidade_litros_dia` | FLOAT | - | `NOT NULL`, `> 0`, `< 15000` | Estimativa de teto produtivo para cálculo de ociosidade de rota. |

**Notas de Implementação:**
- Validar que `capacidade_litros_dia` está entre 1 e 15.000 litros
- `municipio` deve ser mapeado contra lista conhecida de municípios da Bacia Leiteira
- SLA de D-1 implica que dados desatualizados há mais de 24 horas disparam alertas

---

### Entidade 2: `dim_veiculo` (Dimensão Logística)

**Descrição:** Frota de caminhões responsável pela captação refrigerada.  
**SLA de Atualização:** Sob demanda (Eventual)

| Coluna | Tipo Físico | Chave | Restrição (Constraint) / Expectativa de Qualidade | Descrição de Negócio |
| --- | --- | --- | --- | --- |
| `id_veiculo` | INT | PK | `NOT NULL`, `UNIQUE` | Identificador da frota. |
| `placa` | STRING | - | `NOT NULL`, Regex Padrão Mercosul | Placa do veículo logístico. |
| `capacidade_carga_l` | INT | - | `NOT NULL`, `IN (5000, 10000, 15000)` | Capacidade do tanque isotérmico em litros. |
| `custo_fixo_km` | FLOAT | - | `NOT NULL`, `> 0` | Custo operacional fixo do veículo por quilômetro rodado. |

**Regra de Validação - Placa Mercosul:**
```
Padrão: [A-Z]{3}[0-9]{1}[A-Z]{1}[0-9]{2}
Exemplo: ABC1D23
```

**Notas de Implementação:**
- `capacidade_carga_l` deve seguir rigorosamente os valores permitidos (5000, 10000 ou 15000)
- Validar que `custo_fixo_km` é positivo; custo zero não é realista
- Dados não mudam frequentemente, mas mudanças em `custo_fixo_km` devem gerar auditoria

---

### Entidade 3: `fact_coleta` (Fato Transacional - Core Business)

**Descrição:** Registro de cada operação de captação de leite nas fazendas. É aqui que o custo de frete e o risco de perda por qualidade começam.  
**SLA de Atualização:** D0 (Streaming ou Micro-batches)

| Coluna | Tipo Físico | Chave | Restrição (Constraint) / Expectativa de Qualidade | Descrição de Negócio |
| --- | --- | --- | --- | --- |
| `id_coleta` | STRING | PK | `NOT NULL`, `UNIQUE` (UUID format) | Identificador da transação. |
| `id_cooperado` | INT | FK | `NOT NULL`, Deve existir em `dim_cooperado` | Referência ao produtor. |
| `id_veiculo` | INT | FK | `NOT NULL`, Deve existir em `dim_veiculo` | Referência ao caminhão que fez a rota. |
| `data_hora_coleta` | DATETIME | - | `NOT NULL`, `<= NOW()` | Timestamp exato da captação na fazenda. |
| `volume_coletado_l` | FLOAT | - | `NOT NULL`, `> 0`, `<= capacidade_carga_l` | Volume bombeado para o caminhão. |
| `temperatura_c` | FLOAT | - | **Expectativa:** `Entre 2.0 e 6.0` | **Dado Crítico:** Fora deste limite, o leite perde validade (aumenta o custo). |
| `distancia_percorrida_km` | FLOAT | - | `NOT NULL`, `> 0` | Distância da rota para cálculo de eficiência logística. |

**Regras de Negócio Críticas:**
- **Temperatura de Transporte:** Deve estar entre 2.0°C e 6.0°C. Desvios indicam:
  - Falha no sistema de refrigeração do caminhão
  - Risco de contaminação bacteriana (Shift-Left Quality)
  - Perda total do lote na recepção
  
- **Volume vs. Capacidade:** `volume_coletado_l <= capacidade_carga_l` (do veículo referenciado)

- **Data/Hora:** Não pode ser no futuro. Detecta erros de coleta de timestamp

**Notas de Implementação:**
- Esta é a tabela de maior volume (transacional)
- Implementar alerts automáticos via Great Expectations ou dbt tests para temperaturas fora do range
- Monitorar distribuição de volumes por cooperado para detectar anomalias

---

### Entidade 4: `fact_processamento` (Fato de Chão de Fábrica)

**Descrição:** Registro da chegada do leite na indústria e o resultado do teste de qualidade antes da pasteurização.  
**SLA de Atualização:** D0

| Coluna | Tipo Físico | Chave | Restrição (Constraint) / Expectativa de Qualidade | Descrição de Negócio |
| --- | --- | --- | --- | --- |
| `id_lote_recebimento` | STRING | PK | `NOT NULL`, `UNIQUE` | Identificador do lote descarregado na plataforma. |
| `id_veiculo` | INT | FK | `NOT NULL` | Caminhão que descarregou o lote. |
| `data_recebimento` | DATE | - | `NOT NULL` | Data de entrada na indústria. |
| `acidez_ph` | FLOAT | - | **Expectativa:** `Entre 6.6 e 6.8` | Teste de qualidade (Alizarol/Dornic). Valores < 6.6 indicam leite ácido. |
| `status_lote` | STRING | - | `IN ('Aprovado', 'Rejeitado')` | Decisão industrial. |
| `motivo_rejeicao` | STRING | - | `NULL` se Aprovado. Ex: 'Acidez', 'Temperatura' | Motivo do descarte (impacto direto na perda financeira). |

**Regras de Negócio Críticas:**
- **Acidez pH:** Fora do range 6.6–6.8 implica em rejeição automática
  - Raiz comum: fazendas com problemas de higiene pós-ordenha
  - Impacto: Perda total do lote + custo de transporte não recuperado

- **Status Lote:** 
  - Se `Rejeitado`, então `motivo_rejeicao` NÃO NULL
  - Se `Aprovado`, então `motivo_rejeicao` DEVE ser NULL

- **Rastreabilidade:** Permite linkagem inversa até o `id_veiculo` e depois até `id_cooperado` para identifying root causes de qualidade

**Notas de Implementação:**
- Prioridade máxima para alertas de rejeição (impacto financeiro direto)
- Integrar com BI para painel "Taxa de Rejeição por Produtor"
- Dados sensíveis para análise de fornecedor (pode influenciar contratos de exclusividade)

---

**Última atualização:** Maio de 2026  
**Versão:** 1.0 - Contrato de Dados Formal
