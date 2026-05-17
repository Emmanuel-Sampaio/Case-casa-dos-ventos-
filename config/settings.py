"""
Configurações centrais do pipeline.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# S3 ONS Public Bucket
ONS_S3_BASE = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"

DATASET_USINAS = "restricao_coff_eolica_tm"
DATASET_DETAIL = "restricao_coff_eolica_detail_tm"

FILE_PREFIX_USINAS = "RESTRICAO_COFF_EOLICA"
FILE_PREFIX_DETAIL = "RESTRICAO_COFF_EOLICA_DETAIL"


@dataclass
class PipelineConfig:
    # Período de coleta
    start_year_month: str = field(
        default_factory=lambda: os.getenv("START_YEAR_MONTH", "2025-10")
    )
    end_year_month: str = field(
        default_factory=lambda: os.getenv("END_YEAR_MONTH", "2026-03")
    )

    # Diretórios
    raw_dir: Path = field(
        default_factory=lambda: Path(os.getenv("RAW_DIR", str(PROJECT_ROOT / "data" / "raw")))
    )
    processed_dir: Path = field(
        default_factory=lambda: Path(os.getenv("PROCESSED_DIR", str(PROJECT_ROOT / "data" / "processed")))
    )
    parquet_dir: Path = field(
        default_factory=lambda: Path(os.getenv("PARQUET_DIR", str(PROJECT_ROOT / "data" / "parquet")))
    )
    db_path: Path = field(
        default_factory=lambda: Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "warehouse.duckdb")))
    )
    spes_csv: Path = field(
        default_factory=lambda: Path(os.getenv("SPES_CSV", str(PROJECT_ROOT / "data" / "spes_casa_dos_ventos.csv")))
    )
    log_dir: Path = field(
        default_factory=lambda: Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
    )

    # Download
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("MAX_RETRIES", "3"))
    )
    retry_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_DELAY", "5.0"))
    )
    request_timeout: int = field(
        default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT", "120"))
    )

    # Qualidade
    completeness_threshold: float = field(
        default_factory=lambda: float(os.getenv("COMPLETENESS_THRESHOLD", "0.95"))
    )

    # Log
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )

    def __post_init__(self):
        # Garante que todos os diretórios existam
        for d in [self.raw_dir, self.processed_dir, self.parquet_dir, self.log_dir,
                  self.raw_dir / DATASET_USINAS, self.raw_dir / DATASET_DETAIL]:
            d.mkdir(parents=True, exist_ok=True)


def get_config() -> PipelineConfig:
    """Factory que retorna configuração singleton."""
    return PipelineConfig()
