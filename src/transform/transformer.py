"""
Realiza todas as transformações de negócio diretamente em DuckDB (SQL),
seguindo o paradigma ELT: dados brutos já estão na raw layer.
"""

import duckdb
from pathlib import Path

from src.logger import get_logger
from config.settings import PipelineConfig

logger = get_logger(__name__)


# STAGING VIEWS

SQL_STAGING_DETAIL = r"""
CREATE OR REPLACE VIEW stg_detail AS
SELECT
    regexp_extract(ceg, '(\d{6}-\d)', 1)   AS ceg_nucleo,
    id_ons,
    nom_usina,
    nom_conjuntousina                       AS nom_conjunto_spe,
    ceg                                     AS ceg_completo,
    CAST(din_instante AS TIMESTAMP)         AS din_referencia,

    CASE WHEN flg_dadoventoinvalido = 1 THEN NULL
         ELSE val_ventoverificado END       AS val_velocidadeVento,

    flg_dadoventoinvalido                   AS flg_dadoInvalido,
    val_geracaoestimada                     AS val_geracaoEstimada,
    val_geracaoverificada                   AS val_geracaoVerificada,
    _year_month,
    _source_file
FROM raw_detail
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ceg, din_instante
    ORDER BY _source_file DESC
) = 1;
"""

SQL_STAGING_USINAS = r"""
CREATE OR REPLACE VIEW stg_usinas AS
SELECT
    regexp_extract(ceg, '(\d{6}-\d)', 1)   AS ceg_nucleo,
    id_ons                                  AS id_ons_conjunto,
    nom_usina                               AS nom_conjunto,
    ceg                                     AS ceg_completo_conjunto,
    CAST(din_instante AS TIMESTAMP)         AS din_referencia,
    val_geracao,
    val_geracaolimitada                     AS val_geracaoLimitada,
    val_disponibilidade,
    val_geracaoreferencia                   AS val_geracaoReferencia,
    COALESCE(cod_razaorestricao, 'SEM_REST') AS cod_razaorestricao,
    COALESCE(dsc_restricao, 'Sem Restricao') AS nom_razaorestricao,
    COALESCE(cod_origemrestricao, 'N/A')    AS cod_origemrestricao,
    'N/A'                                   AS nom_origemrestricao,
    _year_month,
    _source_file
FROM raw_usinas
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY nom_usina, din_instante
    ORDER BY _source_file DESC
) = 1;
"""

# Filtro Casa dos Ventos

SQL_SPES_TABLE = """
CREATE TABLE IF NOT EXISTS ref_spes (
    projeto VARCHAR,
    spe     VARCHAR,
    ceg     VARCHAR PRIMARY KEY
);
"""

SQL_FILTERED_DETAIL = """
CREATE OR REPLACE VIEW stg_detail_cdv AS
SELECT
    d.*,
    s.spe,
    s.projeto
FROM stg_detail d
INNER JOIN ref_spes s ON d.ceg_nucleo = s.ceg;
"""

# Filtra o dataset de usinas para manter apenas conjuntos Casa dos Ventos
SQL_FILTERED_USINAS = r"""
CREATE OR REPLACE VIEW stg_usinas_cdv AS
SELECT DISTINCT u.*
FROM stg_usinas u
WHERE EXISTS (
    SELECT 1 FROM stg_detail_cdv d
    WHERE UPPER(d.nom_conjunto_spe) = UPPER(u.nom_conjunto)
);
"""

SQL_JOINED = """
CREATE OR REPLACE VIEW stg_joined AS
SELECT
    d.ceg_nucleo,
    d.ceg_completo,
    d.id_ons                                AS id_ons_spe,
    d.nom_usina                             AS nom_spe,
    d.nom_conjunto_spe,
    d.spe,
    d.projeto,
    d.din_referencia,

    d.val_velocidadeVento,
    d.flg_dadoInvalido,
    d.val_geracaoEstimada,
    d.val_geracaoVerificada,

    u.id_ons_conjunto,
    u.nom_conjunto,

    u.val_geracao,
    u.val_geracaoLimitada,
    u.val_disponibilidade,
    u.val_geracaoReferencia,
    u.cod_razaorestricao,
    u.nom_razaorestricao,
    u.cod_origemrestricao,
    u.nom_origemrestricao

FROM stg_detail_cdv d
LEFT JOIN stg_usinas_cdv u
    ON  u.din_referencia = d.din_referencia
    AND UPPER(d.nom_conjunto_spe) = UPPER(u.nom_conjunto)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.ceg_completo, d.din_referencia
    ORDER BY u.id_ons_conjunto NULLS LAST
) = 1;
"""

# Star Schema DDL

DDL_DIM_TEMPO = """
CREATE TABLE IF NOT EXISTS dim_tempo (
    sk_tempo        BIGINT PRIMARY KEY,    -- YYYYMMDDHHMM
    din_referencia  TIMESTAMP NOT NULL,
    dat_data        DATE,
    ano             INTEGER,
    mes             INTEGER,
    dia             INTEGER,
    hora            INTEGER,
    minuto          INTEGER,
    semana_ano      INTEGER,
    dia_semana      INTEGER,               -- 0=Dom … 6=Sab
    is_fim_semana   BOOLEAN,
    UNIQUE (din_referencia)
);
"""

DDL_DIM_SPE = """
CREATE TABLE IF NOT EXISTS dim_spe (
    sk_spe          INTEGER PRIMARY KEY,
    id_ons_spe      VARCHAR NOT NULL,
    spe             VARCHAR NOT NULL,       -- código interno CdV
    projeto         VARCHAR NOT NULL,
    ceg_nucleo      VARCHAR NOT NULL,
    ceg_completo    VARCHAR,
    nom_spe         VARCHAR,
    UNIQUE (id_ons_spe)
);
"""

DDL_DIM_CONJUNTO = """
CREATE TABLE IF NOT EXISTS dim_conjunto (
    sk_conjunto     INTEGER PRIMARY KEY,
    id_ons_conjunto VARCHAR NOT NULL,
    nom_conjunto    VARCHAR,
    UNIQUE (id_ons_conjunto)
);
"""

DDL_DIM_RESTRICAO = """
CREATE TABLE IF NOT EXISTS dim_restricao (
    sk_restricao        INTEGER PRIMARY KEY,
    cod_razaorestricao  VARCHAR NOT NULL,
    nom_razaorestricao  VARCHAR,
    cod_origemrestricao VARCHAR,
    nom_origemrestricao VARCHAR,
    UNIQUE (cod_razaorestricao, cod_origemrestricao)
);
"""

DDL_FATO = """
CREATE TABLE IF NOT EXISTS fato_geracao_spe (
    -- Chaves surrogate
    sk_tempo            BIGINT NOT NULL REFERENCES dim_tempo(sk_tempo),
    sk_spe              INTEGER NOT NULL REFERENCES dim_spe(sk_spe),
    sk_conjunto         INTEGER REFERENCES dim_conjunto(sk_conjunto),   -- NULL se sem match
    sk_restricao        INTEGER REFERENCES dim_restricao(sk_restricao),

    -- Métricas SPE
    val_velocidadeVento     DOUBLE,
    flg_dadoInvalido        INTEGER,
    val_geracaoEstimada     DOUBLE,
    val_geracaoVerificada   DOUBLE,

    -- Métricas conjunto (denormalizadas na fato)
    val_geracao             DOUBLE,
    val_geracaoLimitada     DOUBLE,
    val_disponibilidade     DOUBLE,
    val_geracaoReferencia   DOUBLE,

    PRIMARY KEY (sk_tempo, sk_spe)
);
"""

# Populate Dimensions and Fact

SQL_POP_DIM_TEMPO = """
INSERT OR IGNORE INTO dim_tempo
SELECT
    CAST(strftime(din_referencia, '%Y%m%d%H%M') AS BIGINT) AS sk_tempo,
    din_referencia,
    CAST(din_referencia AS DATE)            AS dat_data,
    YEAR(din_referencia)                    AS ano,
    MONTH(din_referencia)                   AS mes,
    DAY(din_referencia)                     AS dia,
    HOUR(din_referencia)                    AS hora,
    MINUTE(din_referencia)                  AS minuto,
    WEEK(din_referencia)                    AS semana_ano,
    DAYOFWEEK(din_referencia)               AS dia_semana,
    DAYOFWEEK(din_referencia) IN (0, 6)     AS is_fim_semana
FROM (SELECT DISTINCT din_referencia FROM stg_joined WHERE din_referencia IS NOT NULL);
"""

SQL_POP_DIM_SPE = """
INSERT OR IGNORE INTO dim_spe
SELECT
    ROW_NUMBER() OVER (ORDER BY id_ons_spe) AS sk_spe,
    id_ons_spe,
    spe,
    projeto,
    ceg_nucleo,
    ceg_completo,
    nom_spe
FROM (
    SELECT DISTINCT id_ons_spe, spe, projeto, ceg_nucleo, ceg_completo, nom_spe
    FROM stg_joined
    WHERE id_ons_spe IS NOT NULL
);
"""

SQL_POP_DIM_CONJUNTO = """
INSERT OR IGNORE INTO dim_conjunto
SELECT
    ROW_NUMBER() OVER (ORDER BY id_ons_conjunto) AS sk_conjunto,
    id_ons_conjunto,
    nom_conjunto
FROM (
    SELECT DISTINCT id_ons_conjunto, nom_conjunto
    FROM stg_joined
    WHERE id_ons_conjunto IS NOT NULL
);
"""

SQL_POP_DIM_RESTRICAO = """
INSERT OR IGNORE INTO dim_restricao
SELECT
    ROW_NUMBER() OVER (ORDER BY cod_razaorestricao, cod_origemrestricao) AS sk_restricao,
    cod_razaorestricao,
    nom_razaorestricao,
    cod_origemrestricao,
    nom_origemrestricao
FROM (
    SELECT DISTINCT cod_razaorestricao, nom_razaorestricao,
                    cod_origemrestricao, nom_origemrestricao
    FROM stg_joined
    WHERE cod_razaorestricao IS NOT NULL
);
"""

SQL_POP_FATO = """
INSERT OR IGNORE INTO fato_geracao_spe
SELECT
    t.sk_tempo,
    s.sk_spe,
    c.sk_conjunto,
    r.sk_restricao,
    j.val_velocidadeVento,
    j.flg_dadoInvalido,
    j.val_geracaoEstimada,
    j.val_geracaoVerificada,
    j.val_geracao,
    j.val_geracaoLimitada,
    j.val_disponibilidade,
    j.val_geracaoReferencia
FROM stg_joined j
JOIN  dim_tempo    t ON t.din_referencia  = j.din_referencia
JOIN  dim_spe      s ON s.id_ons_spe     = j.id_ons_spe
LEFT JOIN dim_conjunto c ON c.id_ons_conjunto = j.id_ons_conjunto
LEFT JOIN dim_restricao r
    ON  r.cod_razaorestricao  = j.cod_razaorestricao
    AND r.cod_origemrestricao = j.cod_origemrestricao
WHERE j.din_referencia IS NOT NULL
  AND j.id_ons_spe     IS NOT NULL;
"""


# Interface Pública

def load_spes_reference(conn: duckdb.DuckDBPyConnection, spes_csv: Path) -> None:
    """Carrega o arquivo de referência das SPEs Casa dos Ventos."""
    conn.execute(SQL_SPES_TABLE)
    conn.execute("DELETE FROM ref_spes")
    conn.execute(f"""
        INSERT INTO ref_spes
        SELECT projeto, spe, ceg
        FROM read_csv_auto('{spes_csv}', header=true)
    """)
    count = conn.execute("SELECT COUNT(*) FROM ref_spes").fetchone()[0]
    logger.info("Referência SPEs carregada: %d registros", count)


def run_transform(conn: duckdb.DuckDBPyConnection, config: PipelineConfig) -> None:
    """
    Entry-point da etapa Transform.
    Executa as views de staging, filtragem, join e popula o star schema.
    """
    logger.info("Iniciando Transform…")

    # 1. Carrega referência das SPEs
    load_spes_reference(conn, config.spes_csv)

    # 2. Views de staging
    logger.info("Criando views de staging…")
    conn.execute(SQL_STAGING_DETAIL)
    conn.execute(SQL_STAGING_USINAS)
    conn.execute(SQL_FILTERED_DETAIL)
    conn.execute(SQL_FILTERED_USINAS)
    conn.execute(SQL_JOINED)

    # 3. Relatório de join
    total_spe = conn.execute("SELECT COUNT(*) FROM stg_detail_cdv").fetchone()[0]
    matched   = conn.execute("SELECT COUNT(*) FROM stg_joined WHERE id_ons_conjunto IS NOT NULL").fetchone()[0]
    unmatched = total_spe - matched
    logger.info(
        "JOIN SPE<->Conjunto: %d/%d com correspondencia (%.1f%%) | %d sem match (NULL conjunto)",
        matched, total_spe,
        100 * matched / total_spe if total_spe else 0,
        unmatched,
    )
    if unmatched > 0:
        logger.warning(
            "Linhas sem correspondência no dataset de usinas serão inseridas na fato "
            "com sk_conjunto=NULL — isso é esperado quando o conjunto opera sem restrição."
        )

    # 4. DDL do star schema
    logger.info("Criando tabelas do star schema…")
    for ddl in [DDL_DIM_TEMPO, DDL_DIM_SPE, DDL_DIM_CONJUNTO, DDL_DIM_RESTRICAO, DDL_FATO]:
        conn.execute(ddl)

    # 5. Popula dimensões
    logger.info("Populando dimensões…")
    conn.execute(SQL_POP_DIM_TEMPO)
    conn.execute(SQL_POP_DIM_SPE)
    conn.execute(SQL_POP_DIM_CONJUNTO)
    conn.execute(SQL_POP_DIM_RESTRICAO)

    # 6. Popula fato
    logger.info("Populando fato_geracao_spe…")
    conn.execute(SQL_POP_FATO)

    fato_rows = conn.execute("SELECT COUNT(*) FROM fato_geracao_spe").fetchone()[0]
    logger.info("Transform concluído. fato_geracao_spe: %d linhas", fato_rows)

    # 7. Exporta para Parquet
    _export_parquet(conn, config.parquet_dir)


def _export_parquet(conn: duckdb.DuckDBPyConnection, parquet_dir: Path) -> None:
    """
    Exporta a fato + dimensões para Parquet.
    Particionamento escolhido: projeto / ano_mes
    """
    logger.info("Exportando para Parquet em %s…", parquet_dir)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Fato principal
    conn.execute(f"""
        COPY (
            SELECT
                f.*,
                s.projeto,
                s.spe,
                t.ano  || '-' || LPAD(CAST(t.mes AS VARCHAR), 2, '0') AS ano_mes
            FROM fato_geracao_spe f
            JOIN dim_spe    s ON s.sk_spe   = f.sk_spe
            JOIN dim_tempo  t ON t.sk_tempo = f.sk_tempo
        )
        TO '{parquet_dir}/fato_geracao_spe'
        (FORMAT PARQUET, PARTITION_BY (projeto, ano_mes), OVERWRITE_OR_IGNORE TRUE)
    """)

    # Dimensões
    for dim in ["dim_tempo", "dim_spe", "dim_conjunto", "dim_restricao"]:
        conn.execute(f"""
            COPY {dim}
            TO '{parquet_dir}/{dim}.parquet'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """)

    logger.info("Exportação Parquet concluída.")
