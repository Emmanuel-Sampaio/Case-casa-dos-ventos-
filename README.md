# Casa dos Ventos — Pipeline de Dados Eólicos

Pipeline ELT de ponta a ponta para coleta, processamento e exposição dos dados de geração e constrained-off das 47 SPEs da Casa dos Ventos, a partir dos dados públicos do ONS.

---

## Estrutura do Projeto

```
casa_dos_ventos/
├── config/
│   └── settings.py          # Configuração central (env vars + defaults)
├── src/
│   ├── logger.py            # Logging centralizado
│   ├── extract/
│   │   └── downloader.py    # Etapa E: download S3 com retry
│   ├── load/
│   │   └── loader.py        # Etapa L: ingestão raw no DuckDB
│   ├── transform/
│   │   └── transformer.py   # Etapa T: staging, join, star schema
│   ├── validate/
│   │   └── quality.py       # Validação de qualidade e regras de negócio
│   └── api/
│       └── main.py          # FastAPI REST API
├── data/
│   ├── spes_casa_dos_ventos.csv   # Referência das 47 SPEs
│   ├── raw/                       # CSVs brutos baixados do ONS
│   ├── processed/                 # quality_report.json
│   ├── parquet/                   # Saída Parquet particionada
│   └── warehouse.duckdb           # Banco DuckDB (gerado pelo pipeline)
├── tests/
│   └── test_pipeline.py
├── logs/                          # Logs de execução
├── pipeline.py                    # Entry-point principal
└── requirements.txt
```

---

## Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/casa-dos-ventos.git
cd casa_dos_ventos

# Crie e ative um virtualenv
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .venv\Scripts\activate    # Windows

# Instale as dependências
pip install -r requirements.txt
```

---

## Execução do Pipeline

```bash
# Execução padrão (out/2025 → mar/2026)
python pipeline.py

# Parametrizado via CLI
python pipeline.py --start 2025-10 --end 2026-03 --log-level DEBUG

# Parametrizado via variáveis de ambiente
export START_YEAR_MONTH=2025-10
export END_YEAR_MONTH=2026-03
export LOG_LEVEL=INFO
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

Acesse a documentação interativa em: **http://localhost:8000/docs**

### Endpoints disponíveis

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/projects` | Lista projetos com metadados |
| `GET` | `/generation/{project_id}` | Geração agregada por projeto |
| `GET` | `/restrictions/summary` | Resumo de constrained-off |

**Exemplos:**
```bash
# Geração diária do projeto RVD em fevereiro/2026
curl "http://localhost:8000/generation/RVD?granularidade=diario&data_inicio=2026-02-01&data_fim=2026-02-28"

# Resumo de restrições do projeto TGR
curl "http://localhost:8000/restrictions/summary?projeto=TGR"
```

---

## Testes

```bash
pytest tests/ -v
```

---

## Premissas

1. Os arquivos S3 do ONS seguem o padrão `<PREFIXO>_<AAAA-MM>.csv` com separador `;` e encoding `latin-1`.
2. O link SPE ↔ Conjunto é inferido por similaridade de nome + prefixo CEG, pois não há chave explícita entre os dois datasets.
3. Registros com `flg_dadoInvalido = 1` têm a velocidade do vento anulada, mas são mantidos na fato para preservar o histórico de disponibilidade.
4. "MWh restringidos" é calculado como `(geracaoEstimada - geracaoVerificada) × 0.5h` — proxy da energia não gerada por restrição.
