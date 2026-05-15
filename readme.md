```markdown
# 🥛 Coopllactia - Data Engineering Case Study
## Transformando Dados Brutos em Inteligência Logística para Laticínios

### 📋 Índice
1. [Visão Geral](#visão-geral)
2. [Contexto de Negócio](#contexto-de-negócio)
3. [Arquitetura](#arquitetura)
4. [Como Usar](#como-usar)
5. [Decisões de Design e Valor Agregado](#decisões-de-design-e-valor-agregado)
6. [Roadmap Futuro](#roadmap-futuro)

---

## 🎯 Visão Geral

**Coopllactia** é um projeto de Engenharia de Dados que demonstra a construção de um pipeline End-to-End focado em **Governança de Dados** e **Eficiência Operacional**. O cenário simula uma cooperativa agrícola que atua na captação e processamento de leite bruto na Bacia Leiteira de Minas Gerais.

O projeto adota o paradigma **Shift-Left Data Quality**, estabelecendo contratos e validações de negócio antes da ingestão física dos dados, reduzindo o custo de manutenção do pipeline e garantindo a confiabilidade da camada analítica.

### 🎬 O Problema de Negócio

A cooperativa enfrenta desafios operacionais críticos que impactam sua margem de lucro:
- **Perda de Produto:** Leite rejeitado na chegada à fábrica devido a desvios de qualidade (ex: acidez elevada, quebra de refrigeração).
- **Custo Logístico Ocioso:** Caminhões rodando com baixa ocupação do tanque isotérmico ou percorrendo distâncias ineficientes.

O objetivo deste pipeline é fornecer as fundações de dados necessárias para que a área de negócio monitore essas ineficiências.

---

## 🏗️ Arquitetura da Solução

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)** combinada com um processo moderno de **ELT**:

1. **Governança (Design Phase):** Definição do Contrato de Dados (`docs/data_contract.md`).
2. **Camada Raw / Bronze:** Script em Python (`generate_data.py`) que simula o sistema transacional da cooperativa, gerando entidades com integridade referencial estrita e injetando anomalias controladas (falhas de sensor, chaves órfãs).
3. **Camada Silver & Gold:** Pipeline analítico construído com **DuckDB** (`etl_duckdb.py`). O processamento é feito puramente via SQL para garantir performance vetorizada, expurgando dados corrompidos e gerando Data Marts consolidados em formato `.parquet`.

---

## 🚀 Como Usar (Reprodução do Ambiente)

O projeto foi desenhado para ser totalmente reprodutível. Para executar o pipeline localmente, siga os passos abaixo em seu terminal:

**1. Clone o repositório e acesse a pasta:**
```bash
git clone <seu-link-do-github>
cd coopllactia-case

```

**2. Crie e ative um ambiente virtual (Isolamento de dependências):**

*No Linux/Mac:*

```bash
python3 -m venv venv
source venv/bin/activate

```

*No Windows (PowerShell):*

```bash
python -m venv venv
.\venv\Scripts\activate

```

**3. Instale as dependências rigorosas:**

```bash
pip install -r requirements.txt

```

**4. Execute o Pipeline:**

```bash
# Passo A: Geração da Camada Bronze (Arquivos CSV na pasta data/raw)
python generate_data.py

# Passo B: Execução do ELT DuckDB (Geração do Banco e Exportação Parquet)
python etl_duckdb.py

```

*A camada analítica otimizada estará disponível no diretório `data/gold/`.*

---

## 🧠 Decisões de Design e Valor Agregado

A construção deste case foi pautada nas seguintes premissas arquiteturais:

* **Foco no Domínio de Negócio:** A modelagem dimensional foi restrita aos processos de Captação e Processamento Industrial, garantindo que os dados respondam diretamente à dor da diretoria (custos e perdas), evitando o anti-padrão de extrair dados sem propósito analítico.
* **Governança by Design:** A criação de um Contrato de Dados formal padroniza tipos, chaves e SLAs operacionais, servindo como documentação viva para analistas e engenheiros.
* **Resiliência a Anomalias:** O simulador injeta ruídos propositais (5% de sensores de temperatura nulos, quebras de FKs e variações de pH). O pipeline foi desenhado para identificar, sinalizar via *flags* de qualidade e tratar essas anomalias sem interromper a esteira.
* **Processamento Escalável (Modern Data Stack):** A substituição de loops imperativos em Pandas por processamento relacional vetorizado com DuckDB e o armazenamento final em formato colunar (Parquet) preparam a base para uma migração fluida para Data Warehouses em nuvem.

---

## 🛣️ Roadmap Futuro

Visando a evolução do ecossistema de dados da Coopllactia, os próximos passos lógicos incluem:

* [ ] **Orquestração:** Conteinerização da aplicação (Docker) e agendamento das DAGs via **Apache Airflow**.
* [ ] **Cloud Migration:** Transição do armazenamento local para *Object Storage* (Amazon S3 / Google Cloud Storage).
* [ ] **Data Warehouse:** Migração do motor de processamento do DuckDB para **Snowflake** ou **Google BigQuery**, com as transformações sendo gerenciadas via **dbt**.
* [ ] **Observabilidade:** Implementação de testes automatizados de qualidade de dados com **Great Expectations**.
* [ ] **Data Viz:** Conexão da camada Gold a uma ferramenta de BI (Tableau, Looker, PowerBI) para consumo da diretoria.
