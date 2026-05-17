"""
Entry-point do pipeline ELT.
Executa as etapas Extract → Load → Transform → Validate em sequência.

Uso:
  python -m pipeline                         # usa settings padrão
  START_YEAR_MONTH=2025-10 python -m pipeline

Ou via CLI:
  python pipeline.py --start 2025-10 --end 2026-03
"""

import argparse
import sys
import os
from pathlib import Path

# Garante que o root do projeto está no sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import get_config, PipelineConfig
from src.logger import setup_logging, get_logger
from src.extract.downloader import run_extract
from src.load.loader import init_database, run_load
from src.transform.transformer import run_transform
from src.validate.quality import run_quality_checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ELT — Constrained-off Eólico (Casa dos Ventos)"
    )
    parser.add_argument("--start", help="Mês inicial YYYY-MM (override START_YEAR_MONTH)")
    parser.add_argument("--end",   help="Mês final YYYY-MM (override END_YEAR_MONTH)")
    parser.add_argument("--skip-extract",   action="store_true", help="Pula etapa Extract (usa arquivos locais)")
    parser.add_argument("--skip-transform", action="store_true", help="Pula etapa Transform")
    parser.add_argument("--skip-validate",  action="store_true", help="Pula etapa Validate")
    parser.add_argument("--log-level", default=None, help="DEBUG | INFO | WARNING | ERROR")
    return parser.parse_args()


def run_pipeline(config: PipelineConfig) -> None:
    logger = get_logger(__name__)
    logger.info("Pipeline iniciado")
    logger.info("Período: %s → %s", config.start_year_month, config.end_year_month)
    logger.info("DB: %s", config.db_path)

    # EXTRACT
    args = parse_args()

    if not args.skip_extract:
        logger.info("▶ Etapa 1/4: EXTRACT")
        results_usinas, results_detail = run_extract(config)
    else:
        logger.info("⏭ Extract pulado — assumindo arquivos locais existentes")
        # Reconstrói o dict de resultados a partir dos arquivos locais
        from config.settings import DATASET_USINAS, DATASET_DETAIL, FILE_PREFIX_USINAS, FILE_PREFIX_DETAIL
        from src.extract.downloader import _month_range
        months = _month_range(config.start_year_month, config.end_year_month)
        results_usinas = {
            ym: (config.raw_dir / DATASET_USINAS / f"{FILE_PREFIX_USINAS}_{ym}.csv")
            for ym in months
        }
        results_detail = {
            ym: (config.raw_dir / DATASET_DETAIL / f"{FILE_PREFIX_DETAIL}_{ym}.csv")
            for ym in months
        }
        # Marca None para arquivos que não existem
        results_usinas = {k: v if v.exists() else None for k, v in results_usinas.items()}
        results_detail = {k: v if v.exists() else None for k, v in results_detail.items()}

    # LOAD
    logger.info("▶ Etapa 2/4: LOAD")
    conn = init_database(config.db_path)
    run_load(conn, results_usinas, results_detail)

    # TRANSFORM
    if not args.skip_transform:
        logger.info("▶ Etapa 3/4: TRANSFORM")
        run_transform(conn, config)
    else:
        logger.info("⏭ Transform pulado")

    # VALIDATE
    if not args.skip_validate:
        logger.info("▶ Etapa 4/4: VALIDATE")
        report = run_quality_checks(conn, config)
        if report.summary.get("total_violacoes_negras", 0) > 0:
            logger.warning("Pipeline concluído com violações de regras de negócio — revisar relatório.")
    else:
        logger.info("⏭ Validate pulado")

    conn.close()
    logger.info("Pipeline concluído com sucesso")


if __name__ == "__main__":
    args = parse_args()

    # Override de env vars via CLI
    if args.start:
        os.environ["START_YEAR_MONTH"] = args.start
    if args.end:
        os.environ["END_YEAR_MONTH"] = args.end
    if args.log_level:
        os.environ["LOG_LEVEL"] = args.log_level

    config = get_config()
    setup_logging(log_level=config.log_level, log_dir=config.log_dir)

    run_pipeline(config)
