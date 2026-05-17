# Casa dos Ventos — Pipeline de Dados Eólicos

Pipeline ELT de ponta a ponta para coleta, processamento e exposição dos dados de geração e constrained-off das 47 SPEs da Casa dos Ventos, a partir dos dados públicos do ONS.

---

## Estrutura do Projeto

```
casa_dos_ventos/
├── config/
│   └── settings.py               # Configuração central (env vars + defaults)
├── src/
│   ├── logger.py                  # Logging centralizado
│   ├── extract/
│   │   └── downloader.py          # Etapa E: download S3 com retry
│   ├── load/
│   │   └── loader.py              # Etapa L: ingestão raw no DuckDB
│   ├── transform/
│   │   └── transformer.py         # Etapa T: staging, join, star schema
│   ├── validate/
│   │   └── quality.py             # Validação de qualidade
│   └── api/
│       └── main.py                # FastAPI REST API
├── data/
│   ├── spes_casa_dos_ventos.csv   # Referência das 47 SPEs
│   ├── raw/                       # CSVs brutos baixados do ONS
│   ├── processed/                 # quality_report.json
│   ├── parquet/                   # Saída Parquet particionada
│   └── warehouse.duckdb           # Banco DuckDB (gerado pelo pipeline)
├── tests/
│   └── test_pipeline.py
├── logs/
├── pipeline.py                    # Entry-point principal
└── requirements.txt
```

---

## Instalação

```bash
git clone https://github.com/Emmanuel-Sampaio/Case-casa-dos-ventos-.git
cd Case-casa-dos-ventos-

python -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
```

---

## Execução do Pipeline

```bash
# Execução padrão (out/2025 a mar/2026)
python pipeline.py

# Parametrizado via CLI
python pipeline.py --start 2025-10 --end 2026-03 --log-level DEBUG

# Via variáveis de ambiente
export START_YEAR_MONTH=2025-10
export END_YEAR_MONTH=2026-03
python pipeline.py

# Pular etapas (útil para reprocessamento parcial)
python pipeline.py --skip-extract      # reutiliza arquivos locais
python pipeline.py --skip-transform    # só faz Extract + Load
```

### Variáveis de Ambiente

| Variável | Default | Descrição |
|---|---|---|
| `START_YEAR_MONTH` | `2025-10` | Mês inicial da coleta |
| `END_YEAR_MONTH` | `2026-03` | Mês final da coleta |
| `RAW_DIR` | `data/raw` | Diretório de CSVs brutos |
| `PARQUET_DIR` | `data/parquet` | Saída Parquet |
| `DB_PATH` | `data/warehouse.duckdb` | Caminho do DuckDB |
| `LOG_LEVEL` | `INFO` | Nível de log |
| `MAX_RETRIES` | `3` | Tentativas de download |
| `COMPLETENESS_THRESHOLD` | `0.95` | Limiar de completude por projeto |

---

## Execução da API

```bash
uvicorn src.api.main:app --reload --port 8000
```

Documentação interativa: **http://localhost:8000/docs**

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/projects` | Lista projetos com metadados |
| `GET` | `/generation/{project_id}` | Geração agregada por projeto |
| `GET` | `/restrictions/summary` | Resumo de constrained-off |

```bash
# Exemplos
curl "http://localhost:8000/generation/RVD?granularidade=diario&data_inicio=2026-02-01&data_fim=2026-02-28"
curl "http://localhost:8000/restrictions/summary?projeto=TGR"
```

---

## Testes

```bash
pytest tests/ -v
```

---

## Arquitetura

O pipeline segue o paradigma ELT com DuckDB como motor analítico:

```
ONS S3 (CSVs mensais)
    │
    ▼
 EXTRACT ──► data/raw/ (CSVs brutos, idempotente por arquivo)
    │
    ▼
 LOAD ──► DuckDB raw_usinas / raw_detail (controle via _load_control)
    │
    ▼
 TRANSFORM ──► staging views ──► star schema ──► data/parquet/
    │
    ▼
 VALIDATE ──► quality_report.json
    │
    ▼
 SERVE ──► FastAPI (read-only sobre DuckDB)
```

Usei DuckDB ao invés de SQLite/Postgres porque ele é otimizado para queries analíticas colunares, suporta SQL avançado (QUALIFY, window functions) sem dependência de servidor, e faz export direto para Parquet. Como o volume de dados é modesto (~800K linhas na fato), não justifica subir um Postgres.

---

## Modelagem Dimensional

A fato `fato_geracao_spe` tem granularidade **SPE × semi-hora** (30 min), que é a resolução nativa dos dados do ONS. Optei por não agregar porque eventos de restrição podem durar apenas 30 minutos e seriam perdidos numa agregação horária/diária. As dimensões são:

- **dim_tempo** — atributos temporais derivados do timestamp (ano, mês, dia, hora, minuto, dia da semana, flag de fim de semana)
- **dim_spe** — identifica cada usina individual, com o projeto CdV associado e o CEG
- **dim_conjunto** — identifica o complexo eólico ONS ao qual a SPE pertence
- **dim_restricao** — razão e origem da restrição (ex: transmissão, energética)

### Junção SPE ↔ Conjunto

Os dois datasets do ONS não compartilham uma chave explícita. O `id_ons` tem domínios disjuntos (SPE vs. conjunto). A estratégia de join adotada usa o **prefixo numérico do CEG** (os 6 dígitos antes do hífen) como chave de ligação: SPEs e conjuntos que compartilham o mesmo empreendimento físico possuem o mesmo prefixo CEG. O join é LEFT JOIN do detail para usinas, com `din_referencia` como condição temporal. Linhas sem correspondência (SPE sem restrição ativa) ficam com `sk_conjunto = NULL` na fato — isso é esperado e documentado nos logs.

Antes do join, o dataset de usinas é filtrado via `stg_usinas_cdv` para manter apenas conjuntos cujo prefixo CEG coincide com alguma SPE da Casa dos Ventos, evitando matches espúrios com conjuntos de outras empresas.

### Decisões de Design

**Desnormalização:** as métricas de conjunto (`val_geracao`, `val_geracaoLimitada`, `val_disponibilidade`, `val_geracaoReferencia`) foram denormalizadas na fato. Elas pertencem logicamente ao nível do conjunto, mas como não mudam historicamente e seriam necessárias em praticamente toda query analítica, evito um join adicional.

**SCD:** a `dim_spe` contém atributos que raramente mudam (CEG, projeto, nome). No período de 6 meses analisado não há mudanças, então não implementei SCD Type 2. Em produção com janela maior, valeria a pena implementar SCD Type 2 na `dim_spe` para capturar eventuais reclassificações de projeto, e Type 1 na `dim_conjunto`.

**Particionamento Parquet:** escolhi `projeto / ano_mes` porque as queries analíticas tipicamente filtram por projeto e período. Com 7 projetos × 6 meses = 42 partições, o tamanho é gerenciável.

---

## Orquestração em Produção

Em produção, orquestraria com **Cloud Scheduler + Cloud Run** no GCP:

- **Trigger:** Cloud Scheduler dispara no dia 1 de cada mês (cron `0 6 1 * *`)
- **Execução:** Cloud Run Job roda `pipeline.py` em container
- **Armazenamento:** raw CSVs no GCS, dados processados no BigQuery
- **Transformações:** migraria o SQL do DuckDB para dbt rodando sobre BigQuery
- **Monitoramento:** alertas via Cloud Monitoring para falhas de qualidade

Alternativa com **Airflow**: uma DAG mensal com tasks `extract >> load >> transform >> validate`, cada uma como `PythonOperator`. O Airflow daria visibilidade de execução e retry automático por task, mas adiciona complexidade operacional que só se justifica com mais pipelines.

---

## Premissas

1. Os CSVs do ONS seguem o padrão `<PREFIXO>_<AAAA-MM>.csv` com separador `;` e encoding `latin-1`.
2. O link SPE ↔ Conjunto é feito pelo prefixo numérico do CEG (6 dígitos antes do hífen), já que não há chave explícita entre os datasets.
3. Registros com `flg_dadoInvalido = 1` têm a velocidade do vento anulada, mas são mantidos na fato para preservar o histórico de disponibilidade e geração.
4. "MWh restringidos" é calculado como `(geracaoEstimada - geracaoVerificada) × 0.5h` — proxy da energia que deixou de ser gerada por restrição.
