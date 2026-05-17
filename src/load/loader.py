"""
Responsável por ingerir os CSVs brutos baixados na etapa Extract
para o DuckDB (raw layer), sem nenhuma transformação de negócio.

Idempotência: antes de inserir cada arquivo, verifica se o par
(dataset, year_month) já existe na tabela de controle.
Se existir, pula a carga — re-executar o pipeline é seguro.
"""

from pathlib import Path

import duckdb
import pandas as pd

from src.logger import get_logger
from config.settings import PipelineConfig, DATASET_USINAS, DATASET_DETAIL

logger = get_logger(__name__)



DDL_RAW_USINAS = """
CREATE TABLE IF NOT EXISTS raw_usinas (
    -- controle de carga
    _source_file  VARCHAR,
    _year_month   VARCHAR,

    -- campos originais ONS (dataset 1 — granularidade conjunto/complexo)
    id_ons              VARCHAR,
    nom_usina           VARCHAR,
    ceg                 VARCHAR,
    din_referencia      TIMESTAMP,
    val_geracao         DOUBLE,
    val_geracaoLimitada DOUBLE,
    val_disponibilidade DOUBLE,
    val_geracaoReferencia DOUBLE,
    cod_razaorestricao  VARCHAR,
    nom_razaorestricao  VARCHAR,
    cod_origemrestricao VARCHAR,
    nom_origemrestricao VARCHAR
);
"""

DDL_RAW_DETAIL = """
CREATE TABLE IF NOT EXISTS raw_detail (
    -- controle de carga
    _source_file  VARCHAR,
    _year_month   VARCHAR,

    -- campos originais ONS (dataset 2 — granularidade SPE)
    id_ons              VARCHAR,
    nom_usina           VARCHAR,
    ceg                 VARCHAR,
    din_referencia      TIMESTAMP,
    val_velocidadeVento DOUBLE,
    flg_dadoInvalido    INTEGER,
    val_geracaoEstimada DOUBLE,
    val_geracaoVerificada DOUBLE
);
"""

DDL_LOAD_CONTROL = """
CREATE TABLE IF NOT EXISTS _load_control (
    dataset     VARCHAR,
    year_month  VARCHAR,
    source_file VARCHAR,
    loaded_at   TIMESTAMP DEFAULT current_timestamp,
    row_count   INTEGER,
    PRIMARY KEY (dataset, year_month)
);
"""



def _is_already_loaded(conn: duckdb.DuckDBPyConnection, dataset: str, ym: str) -> bool:
    result = conn.execute(
        "SELECT 1 FROM _load_control WHERE dataset = ? AND year_month = ?",
        [dataset, ym]
    ).fetchone()
    return result is not None


def _read_csv_safe(path: Path) -> pd.DataFrame | None:
    """
    Lê CSV com encoding latin-1 (padrão ONS) e sep=';'.
    Retorna None se falhar.
    """
    try:
        df = pd.read_csv(
            path,
            sep=";",
            encoding="latin-1",
            low_memory=False,
        )
        return df
    except Exception as exc:
        logger.error("Falha ao ler CSV %s: %s", path.name, exc)
        return None


def _normalize_columns(df: pd.DataFrame, expected_cols: list[str]) -> pd.DataFrame:
    """
    Normaliza nomes de colunas (strip + lowercase) e garante
    que as colunas esperadas existam (adiciona como NULL se ausente).
    """
    df.columns = [c.strip() for c in df.columns]

    # Tenta mapear insensível a case
    col_map = {c.lower(): c for c in df.columns}
    rename = {}
    for exp in expected_cols:
        key = exp.lower()
        if key in col_map and col_map[key] != exp:
            rename[col_map[key]] = exp
    if rename:
        df = df.rename(columns=rename)

    for col in expected_cols:
        if col not in df.columns:
            logger.warning("Coluna '%s' ausente no arquivo — preenchendo com NULL.", col)
            df[col] = None

    return df


USINAS_COLS = [
    "id_ons", "nom_usina", "ceg", "din_referencia",
    "val_geracao", "val_geracaoLimitada", "val_disponibilidade",
    "val_geracaoReferencia", "cod_razaorestricao", "nom_razaorestricao",
    "cod_origemrestricao", "nom_origemrestricao",
]

DETAIL_COLS = [
    "id_ons", "nom_usina", "ceg", "din_referencia",
    "val_velocidadeVento", "flg_dadoInvalido",
    "val_geracaoEstimada", "val_geracaoVerificada",
]


def _load_file(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    ym: str,
    dataset: str,
    table: str,
    expected_cols: list[str],
) -> int:
    """Carrega um arquivo CSV para a tabela raw correspondente. Retorna nº de linhas."""
    df = _read_csv_safe(path)
    if df is None:
        return 0

    df = _normalize_columns(df, expected_cols)
    df["_source_file"] = path.name
    df["_year_month"]  = ym

    # Parseia data de forma robusta
    if "din_referencia" in df.columns:
        df["din_referencia"] = pd.to_datetime(
            df["din_referencia"], dayfirst=True, errors="coerce"
        )

    # Seleciona apenas colunas relevantes
    all_cols = ["_source_file", "_year_month"] + expected_cols
    df = df[[c for c in all_cols if c in df.columns]]

    conn.register("_df_tmp", df)
    conn.execute(f"INSERT INTO {table} SELECT * FROM _df_tmp")
    conn.unregister("_df_tmp")

    # Registra controle de carga
    conn.execute(
        "INSERT OR REPLACE INTO _load_control (dataset, year_month, source_file, loaded_at, row_count) "
        "VALUES (?, ?, ?, current_timestamp, ?)",
        [dataset, ym, path.name, len(df)],
    )

    logger.info("Carregado %s → %s: %d linhas", path.name, table, len(df))
    return len(df)



def init_database(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Abre/cria o DuckDB e garante que as tabelas raw existam."""
    conn = duckdb.connect(str(db_path))
    conn.execute(DDL_LOAD_CONTROL)
    conn.execute(DDL_RAW_USINAS)
    conn.execute(DDL_RAW_DETAIL)
    logger.info("DuckDB inicializado em: %s", db_path)
    return conn


def run_load(
    conn: duckdb.DuckDBPyConnection,
    results_usinas: dict[str, Path | None],
    results_detail: dict[str, Path | None],
) -> None:
    """
    Entry-point da etapa Load.
    Itera sobre os resultados do Extract e carrega apenas arquivos novos.
    """
    logger.info("Iniciando Load…")
    total_rows = 0

    for ym, path in results_usinas.items():
        if path is None:
            logger.warning("Mês %s indisponível para usinas — pulando.", ym)
            continue
        if _is_already_loaded(conn, DATASET_USINAS, ym):
            logger.info("Mês %s já carregado (usinas) — idempotência aplicada.", ym)
            continue
        total_rows += _load_file(conn, path, ym, DATASET_USINAS, "raw_usinas", USINAS_COLS)

    for ym, path in results_detail.items():
        if path is None:
            logger.warning("Mês %s indisponível para detail — pulando.", ym)
            continue
        if _is_already_loaded(conn, DATASET_DETAIL, ym):
            logger.info("Mês %s já carregado (detail) — idempotência aplicada.", ym)
            continue
        total_rows += _load_file(conn, path, ym, DATASET_DETAIL, "raw_detail", DETAIL_COLS)

    logger.info("Load concluído. Total de linhas carregadas nesta execução: %d", total_rows)
