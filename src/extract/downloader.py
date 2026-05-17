"""
Responsável por baixar os arquivos CSV mensais dos datasets ONS
do S3 público, com retry e idempotência.
"""

import time
from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta

import requests

from src.logger import get_logger
from config.settings import (
    PipelineConfig,
    ONS_S3_BASE,
    DATASET_USINAS,
    DATASET_DETAIL,
    FILE_PREFIX_USINAS,
    FILE_PREFIX_DETAIL,
)

logger = get_logger(__name__)



def _month_range(start_ym: str, end_ym: str) -> list[str]:
    """
    Gera lista de strings 'AAAA-MM' entre start_ym e end_ym (inclusive).
    Ex: _month_range('2025-10', '2026-03') →
        ['2025-10', '2025-11', '2025-12', '2026-01', '2026-02', '2026-03']
    """
    start = date.fromisoformat(f"{start_ym}-01")
    end   = date.fromisoformat(f"{end_ym}-01")
    months = []
    cur = start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        cur += relativedelta(months=1)
    return months


def _build_url(dataset: str, prefix: str, year_month: str) -> str:
    """Constrói a URL S3 para um arquivo mensal."""
    ym_underscore = year_month.replace("-", "_")
    filename = f"{prefix}_{ym_underscore}.csv"
    return f"{ONS_S3_BASE}/{dataset}/{filename}"


def _download_file(
    url: str,
    dest: Path,
    max_retries: int,
    retry_delay: float,
    timeout: int,
) -> bool:
    """
    Baixa url → dest com retry exponencial.
    Retorna True em sucesso, False se o arquivo não existir no servidor.
    Levanta RuntimeError em erros inesperados após esgotar retries.
    """
    if dest.exists():
        logger.info("Já existe localmente, pulando download: %s", dest.name)
        return True

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Baixando (tentativa %d/%d): %s", attempt, max_retries, url)
            resp = requests.get(url, timeout=timeout, stream=True)

            if resp.status_code == 404:
                logger.warning("Arquivo não encontrado no servidor (404): %s", url)
                return False

            resp.raise_for_status()

            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)

            logger.info("Download concluído: %s (%.1f KB)", dest.name, dest.stat().st_size / 1024)
            return True

        except requests.exceptions.Timeout:
            logger.warning("Timeout na tentativa %d para %s", attempt, url)
        except requests.exceptions.ConnectionError as exc:
            logger.warning("Erro de conexão na tentativa %d: %s", attempt, exc)
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error %s para %s", exc.response.status_code, url)
            raise

        if attempt < max_retries:
            wait = retry_delay * (2 ** (attempt - 1))
            logger.info("Aguardando %.1f s antes de nova tentativa…", wait)
            time.sleep(wait)

    raise RuntimeError(f"Falha ao baixar {url} após {max_retries} tentativas")



def extract_dataset(
    dataset: str,
    prefix: str,
    months: list[str],
    raw_dir: Path,
    config: PipelineConfig,
) -> dict[str, Path | None]:
    """
    Baixa todos os meses de um dataset.
    Retorna dict {year_month → Path | None}
      - Path: arquivo baixado com sucesso
      - None: arquivo não disponível no servidor (fallback registrado)
    """
    results: dict[str, Path | None] = {}
    dataset_dir = raw_dir / dataset

    for ym in months:
        url  = _build_url(dataset, prefix, ym)
        ym_underscore = ym.replace("-", "_")
        dest = dataset_dir / f"{prefix}_{ym_underscore}.csv"

        try:
            ok = _download_file(
                url=url,
                dest=dest,
                max_retries=config.max_retries,
                retry_delay=config.retry_delay,
                timeout=config.request_timeout,
            )
            results[ym] = dest if ok else None
            if not ok:
                logger.warning("Mês %s indisponível para dataset '%s' — registrando fallback.", ym, dataset)
        except RuntimeError as exc:
            logger.error("Erro irrecuperável para %s/%s: %s", dataset, ym, exc)
            results[ym] = None

    available = sum(1 for v in results.values() if v is not None)
    logger.info(
        "Dataset '%s': %d/%d meses baixados com sucesso.",
        dataset, available, len(months)
    )
    return results


def run_extract(config: PipelineConfig) -> tuple[dict, dict]:
    """
    Entry-point da etapa Extract.
    Retorna (results_usinas, results_detail) — dicts {ym → Path|None}.
    """
    months = _month_range(config.start_year_month, config.end_year_month)
    logger.info("Iniciando Extract para %d meses: %s -> %s", len(months), months[0], months[-1])

    results_usinas = extract_dataset(
        dataset=DATASET_USINAS,
        prefix=FILE_PREFIX_USINAS,
        months=months,
        raw_dir=config.raw_dir,
        config=config,
    )
    results_detail = extract_dataset(
        dataset=DATASET_DETAIL,
        prefix=FILE_PREFIX_DETAIL,
        months=months,
        raw_dir=config.raw_dir,
        config=config,
    )

    return results_usinas, results_detail
