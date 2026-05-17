"""
Produz relatório de qualidade dos dados brutos e valida
as regras de negócio do domínio de constrained-off eólico.
"""

from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timedelta
import json

import duckdb
import pandas as pd

from src.logger import get_logger
from config.settings import PipelineConfig

logger = get_logger(__name__)



EXPECTED_SCHEMA_RAW_DETAIL = {
    "id_ons": "VARCHAR",
    "nom_usina": "VARCHAR",
    "ceg": "VARCHAR",
    "din_referencia": "TIMESTAMP",
    "val_velocidadeVento": "DOUBLE",
    "flg_dadoInvalido": "INTEGER",
    "val_geracaoEstimada": "DOUBLE",
    "val_geracaoVerificada": "DOUBLE",
}

EXPECTED_SCHEMA_RAW_USINAS = {
    "id_ons": "VARCHAR",
    "nom_usina": "VARCHAR",
    "ceg": "VARCHAR",
    "din_referencia": "TIMESTAMP",
    "val_geracao": "DOUBLE",
    "val_geracaoLimitada": "DOUBLE",
    "val_geracaoReferencia": "DOUBLE",
    "cod_razaorestricao": "VARCHAR",
}


@dataclass
class QualityReport:
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    schema_issues: list[str] = field(default_factory=list)
    freshness: dict = field(default_factory=dict)
    null_stats: dict = field(default_factory=dict)          # {table: {col: pct_null}}
    duplicates: dict = field(default_factory=dict)          # {table: count}
    business_rule_violations: dict = field(default_factory=dict)
    completeness_by_project: dict = field(default_factory=dict)
    referential_integrity: dict = field(default_factory=dict)
    summary: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(self.__dict__, indent=2, ensure_ascii=False, default=str)

    def print_summary(self) -> None:
        logger.info("=" * 60)
        logger.info("RELATÓRIO DE QUALIDADE DE DADOS")
        logger.info("=" * 60)
        for k, v in self.summary.items():
            logger.info("  %-35s %s", k + ":", v)
        if self.schema_issues:
            logger.warning("Problemas de schema: %s", self.schema_issues)
        if self.business_rule_violations:
            for rule, count in self.business_rule_violations.items():
                lvl = logger.warning if count > 0 else logger.info
                lvl("  Violações [%s]: %d", rule, count)
        for proj, pct in self.completeness_by_project.items():
            lvl = logger.warning if pct < 0.95 else logger.info
            lvl("  Completude [%s]: %.1f%%", proj, pct * 100)
        logger.info("=" * 60)



def check_schema(conn: duckdb.DuckDBPyConnection, report: QualityReport) -> None:
    """Verifica se as colunas e tipos das tabelas raw batem com o esperado."""
    checks = {
        "raw_detail": EXPECTED_SCHEMA_RAW_DETAIL,
        "raw_usinas": EXPECTED_SCHEMA_RAW_USINAS,
    }
    for table, expected in checks.items():
        actual_cols = {
            row[0]: row[1]
            for row in conn.execute(f"DESCRIBE {table}").fetchall()
        }
        for col, dtype in expected.items():
            if col not in actual_cols:
                report.schema_issues.append(f"{table}.{col} ausente")
            elif not actual_cols[col].startswith(dtype.split("(")[0]):
                report.schema_issues.append(
                    f"{table}.{col}: esperado {dtype}, encontrado {actual_cols[col]}"
                )
    if not report.schema_issues:
        logger.info("Schema: OK")
    else:
        logger.warning("Schema issues: %s", report.schema_issues)


def check_freshness(
    conn: duckdb.DuckDBPyConnection,
    report: QualityReport,
    expected_latest: str,
) -> None:
    """Valida que o mês mais recente esperado está presente."""
    latest_detail = conn.execute(
        "SELECT MAX(_year_month) FROM raw_detail"
    ).fetchone()[0]
    latest_usinas = conn.execute(
        "SELECT MAX(_year_month) FROM raw_usinas"
    ).fetchone()[0]

    report.freshness = {
        "expected_latest": expected_latest,
        "raw_detail_latest": latest_detail,
        "raw_usinas_latest": latest_usinas,
        "detail_ok": latest_detail == expected_latest,
        "usinas_ok": latest_usinas == expected_latest,
    }

    if latest_detail != expected_latest:
        logger.warning(
            "Freshness: raw_detail mais recente é %s, esperado %s",
            latest_detail, expected_latest
        )
    else:
        logger.info("Freshness raw_detail: OK (%s)", latest_detail)


def check_nulls(conn: duckdb.DuckDBPyConnection, report: QualityReport) -> None:
    """Calcula percentual de nulos por coluna em cada tabela raw."""
    for table in ["raw_detail", "raw_usinas"]:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if total == 0:
            continue
        cols = [r[0] for r in conn.execute(f"DESCRIBE {table}").fetchall()]
        null_pct = {}
        for col in cols:
            if col.startswith("_"):
                continue
            n_null = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            pct = n_null / total
            null_pct[col] = round(pct, 4)
            if pct > 0.05:
                logger.warning("%s.%s: %.1f%% nulos", table, col, pct * 100)
        report.null_stats[table] = null_pct


def check_duplicates(conn: duckdb.DuckDBPyConnection, report: QualityReport) -> None:
    """Conta duplicatas exatas (ceg + din_referencia) em cada tabela."""
    for table, key in [
        ("raw_detail", "ceg, din_referencia"),
        ("raw_usinas", "ceg, din_referencia"),
    ]:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        distinct = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT {key} FROM {table})"
        ).fetchone()[0]
        dups = total - distinct
        report.duplicates[table] = dups
        if dups > 0:
            logger.warning("%s: %d duplicatas encontradas", table, dups)
        else:
            logger.info("%s: sem duplicatas", table)


def check_business_rules(conn: duckdb.DuckDBPyConnection, report: QualityReport) -> None:
    """Valida regras de negócio do domínio eólico."""
    rules = {
        "geracao_negativa_spe": """
            SELECT COUNT(*) FROM raw_detail
            WHERE val_geracaoVerificada < 0 OR val_geracaoEstimada < 0
        """,
        "geracao_negativa_conjunto": """
            SELECT COUNT(*) FROM raw_usinas
            WHERE val_geracao < 0 OR val_geracaoLimitada < 0
        """,
        "vento_fora_faixa": """
            SELECT COUNT(*) FROM raw_detail
            WHERE val_velocidadeVento IS NOT NULL
              AND (val_velocidadeVento < 0 OR val_velocidadeVento > 40)
        """,
        "limitada_maior_referencia": """
            SELECT COUNT(*) FROM raw_usinas
            WHERE val_geracaoLimitada IS NOT NULL
              AND val_geracaoReferencia IS NOT NULL
              AND val_geracaoLimitada > val_geracaoReferencia * 1.01  -- tolerância 1%
        """,
        "dado_invalido_com_geracao": """
            SELECT COUNT(*) FROM raw_detail
            WHERE flg_dadoInvalido = 1
              AND val_geracaoVerificada IS NOT NULL
              AND val_geracaoVerificada > 0
        """,
    }
    for rule, sql in rules.items():
        count = conn.execute(sql).fetchone()[0]
        report.business_rule_violations[rule] = count


def check_timestamp_continuity(
    conn: duckdb.DuckDBPyConnection,
    report: QualityReport,
    config: PipelineConfig,
) -> None:
    """
    Valida que os timestamps são semi-horários contínuos por projeto.
    Detecta gaps > 30 min e calcula completude real vs esperada.
    """
    # Timestamps esperados: a cada 30 min entre min e max por projeto
    query = r"""
    SELECT
        s.projeto,
        MIN(d.din_referencia)    AS ts_min,
        MAX(d.din_referencia)    AS ts_max,
        COUNT(DISTINCT d.din_referencia) AS ts_count
    FROM raw_detail d
    JOIN ref_spes s ON regexp_extract(d.ceg, '(\d{6}-\d)', 1) = s.ceg
    GROUP BY s.projeto
    """
    try:
        rows = conn.execute(query).fetchall()
    except Exception as exc:
        logger.warning("Não foi possível verificar continuidade de timestamps: %s", exc)
        return

    for projeto, ts_min, ts_max, ts_count in rows:
        if ts_min is None or ts_max is None:
            continue
        expected = int((ts_max - ts_min).total_seconds() / 1800) + 1
        pct = ts_count / expected if expected > 0 else 0
        report.completeness_by_project[projeto] = round(pct, 4)

        if pct < config.completeness_threshold:
            logger.warning(
                "Projeto %s: completude %.1f%% (abaixo do threshold %.0f%%)",
                projeto, pct * 100, config.completeness_threshold * 100
            )


def check_referential_integrity(
    conn: duckdb.DuckDBPyConnection, report: QualityReport
) -> None:
    """Verifica integridade referencial entre fato e dimensões."""
    try:
        checks = {
            "fato->dim_tempo": """
                SELECT COUNT(*) FROM fato_geracao_spe f
                LEFT JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
                WHERE t.sk_tempo IS NULL
            """,
            "fato->dim_spe": """
                SELECT COUNT(*) FROM fato_geracao_spe f
                LEFT JOIN dim_spe s ON s.sk_spe = f.sk_spe
                WHERE s.sk_spe IS NULL
            """,
            "fato->dim_conjunto": """
                SELECT COUNT(*) FROM fato_geracao_spe f
                LEFT JOIN dim_conjunto c ON c.sk_conjunto = f.sk_conjunto
                WHERE f.sk_conjunto IS NOT NULL AND c.sk_conjunto IS NULL
            """,
            "fato->dim_restricao": """
                SELECT COUNT(*) FROM fato_geracao_spe f
                LEFT JOIN dim_restricao r ON r.sk_restricao = f.sk_restricao
                WHERE f.sk_restricao IS NOT NULL AND r.sk_restricao IS NULL
            """,
        }
        for check, sql in checks.items():
            orphans = conn.execute(sql).fetchone()[0]
            report.referential_integrity[check] = orphans
            if orphans > 0:
                logger.error("Integridade referencial: %d órfãos em %s", orphans, check)
            else:
                logger.info("Integridade referencial OK: %s", check)
    except Exception as exc:
        logger.warning("Tabelas do modelo ainda não criadas: %s", exc)



def run_quality_checks(
    conn: duckdb.DuckDBPyConnection,
    config: PipelineConfig,
    save_report: bool = True,
) -> QualityReport:
    """
    Executa todas as validações e retorna/salva o relatório.
    """
    logger.info("Iniciando validação de qualidade de dados…")
    report = QualityReport()

    check_schema(conn, report)
    check_freshness(conn, report, expected_latest=config.end_year_month)
    check_nulls(conn, report)
    check_duplicates(conn, report)
    check_business_rules(conn, report)
    check_timestamp_continuity(conn, report, config)
    check_referential_integrity(conn, report)

    # Resumo executivo
    total_detail = conn.execute("SELECT COUNT(*) FROM raw_detail").fetchone()[0]
    total_usinas = conn.execute("SELECT COUNT(*) FROM raw_usinas").fetchone()[0]
    report.summary = {
        "total_linhas_raw_detail": total_detail,
        "total_linhas_raw_usinas": total_usinas,
        "schema_ok": len(report.schema_issues) == 0,
        "freshness_detail_ok": report.freshness.get("detail_ok", False),
        "total_violacoes_negras": sum(report.business_rule_violations.values()),
        "projetos_abaixo_completude": sum(
            1 for v in report.completeness_by_project.values()
            if v < config.completeness_threshold
        ),
    }

    report.print_summary()

    if save_report:
        report_path = config.processed_dir / "quality_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report.to_json(), encoding="utf-8")
        logger.info("Relatório salvo em: %s", report_path)

    return report
