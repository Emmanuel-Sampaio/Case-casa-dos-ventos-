"""
Testes unitários do pipeline.
Execute com: pytest tests/ -v
"""

import sys
from pathlib import Path
import pytest
import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.extract.downloader import _month_range, _build_url
from config.settings import DATASET_USINAS, FILE_PREFIX_USINAS



def test_month_range_basic():
    result = _month_range("2025-10", "2026-03")
    assert result == ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03"]


def test_month_range_single():
    result = _month_range("2026-01", "2026-01")
    assert result == ["2026-01"]


def test_month_range_cross_year():
    result = _month_range("2025-11", "2026-02")
    assert len(result) == 4


def test_build_url():
    url = _build_url(DATASET_USINAS, FILE_PREFIX_USINAS, "2025-10")
    assert "2025-10" in url
    assert DATASET_USINAS in url
    assert url.startswith("https://")



@pytest.fixture
def in_memory_db():
    """Cria um DuckDB em memória com dados sintéticos para testes."""
    conn = duckdb.connect(":memory:")

    # Tabela raw_detail sintética
    conn.execute("""
        CREATE TABLE raw_detail AS
        SELECT
            'MAEDT1'                    AS id_ons,
            'MORRO ESTREITO 1'          AS nom_usina,
            'EOL.CV.BA.037102-5.01'     AS ceg,
            TIMESTAMP '2025-10-01 00:00:00' AS din_referencia,
            8.5                         AS val_velocidadeVento,
            0                           AS flg_dadoInvalido,
            10.2                        AS val_geracaoEstimada,
            9.8                         AS val_geracaoVerificada,
            '2025-10'                   AS _year_month,
            'test_file.csv'             AS _source_file
        UNION ALL
        SELECT
            'MAEDT1', 'MORRO ESTREITO 1', 'EOL.CV.BA.037102-5.01',
            TIMESTAMP '2025-10-01 00:30:00',
            -1.0,   -- vento negativo → violação de regra de negócio
            1,      -- dado inválido
            5.0, 0.0,
            '2025-10', 'test_file.csv'
    """)

    # Tabela raw_usinas sintética
    conn.execute("""
        CREATE TABLE raw_usinas AS
        SELECT
            'CJU_MROESTR'               AS id_ons,
            'CONJ. MORRO ESTREITO'      AS nom_usina,
            'EOL.CV.BA.037102-5'        AS ceg,
            TIMESTAMP '2025-10-01 00:00:00' AS din_referencia,
            9.0                         AS val_geracao,
            8.0                         AS val_geracaoLimitada,
            100.0                       AS val_disponibilidade,
            10.0                        AS val_geracaoReferencia,
            'TRANS'                     AS cod_razaorestricao,
            'Restrição de Transmissão'  AS nom_razaorestricao,
            'ONS'                       AS cod_origemrestricao,
            'Operador'                  AS nom_origemrestricao,
            '2025-10'                   AS _year_month,
            'test_file.csv'             AS _source_file
    """)

    # Controle de carga
    conn.execute("""
        CREATE TABLE _load_control (
            dataset VARCHAR, year_month VARCHAR,
            source_file VARCHAR, loaded_at TIMESTAMP, row_count INTEGER,
            PRIMARY KEY (dataset, year_month)
        )
    """)

    yield conn
    conn.close()


def test_ceg_nucleo_extraction(in_memory_db):
    """Testa extração do núcleo do CEG via regex."""
    result = in_memory_db.execute("""
        SELECT regexp_extract('EOL.CV.BA.037102-5.01', '(\d{6}-\d)', 1)
    """).fetchone()[0]
    assert result == "037102-5"


def test_dado_invalido_null(in_memory_db):
    """Vento deve ser NULL quando flg_dadoInvalido = 1."""
    result = in_memory_db.execute("""
        SELECT
            CASE WHEN flg_dadoInvalido = 1 THEN NULL
                 ELSE val_velocidadeVento END AS vento
        FROM raw_detail
        WHERE din_referencia = TIMESTAMP '2025-10-01 00:30:00'
    """).fetchone()[0]
    assert result is None


def test_business_rule_negative_wind(in_memory_db):
    """Detecta velocidade de vento negativa (violação de regra)."""
    count = in_memory_db.execute("""
        SELECT COUNT(*) FROM raw_detail
        WHERE val_velocidadeVento < 0
    """).fetchone()[0]
    assert count == 1


def test_business_rule_no_negative_generation(in_memory_db):
    """Geração verificada não deve ser negativa nos dados sintéticos válidos."""
    count = in_memory_db.execute("""
        SELECT COUNT(*) FROM raw_detail
        WHERE val_geracaoVerificada < 0
    """).fetchone()[0]
    assert count == 0


def test_idempotency_load_control(in_memory_db):
    """INSERT OR REPLACE não deve duplicar dados na tabela de controle."""
    in_memory_db.execute("""
        INSERT INTO _load_control VALUES ('test_ds', '2025-10', 'f.csv', NOW(), 100)
    """)
    in_memory_db.execute("""
        INSERT OR REPLACE INTO _load_control VALUES ('test_ds', '2025-10', 'f.csv', NOW(), 100)
    """)
    count = in_memory_db.execute(
        "SELECT COUNT(*) FROM _load_control WHERE dataset='test_ds'"
    ).fetchone()[0]
    assert count == 1



def test_api_health():
    """Testa endpoint /health sem banco de dados real."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "db_exists" in data
