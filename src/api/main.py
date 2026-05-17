"""
FastAPI servindo os dados processados do warehouse DuckDB.

Endpoints:
  GET /health                    — health check
  GET /projects                  — lista projetos disponíveis
  GET /generation/{project_id}   — geração agregada de um projeto
  GET /restrictions/summary      — resumo de restrições por razão

Documentação automática: http://localhost:8000/docs
"""

import os
from pathlib import Path
from datetime import date
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "warehouse.duckdb")))


def get_conn() -> duckdb.DuckDBPyConnection:
    """Abre uma conexão read-only ao DuckDB (thread-safe para FastAPI)."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Warehouse não encontrado em {DB_PATH}. Execute o pipeline primeiro."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)



app = FastAPI(
    title="Casa dos Ventos — API de Geração Eólica",
    description=(
        "API REST para consulta de dados de geração e constrained-off "
        "das SPEs da Casa dos Ventos, processados a partir dos dados públicos do ONS."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)



class HealthResponse(BaseModel):
    status: str
    db_path: str
    db_exists: bool


class ProjectMetadata(BaseModel):
    projeto: str
    n_spes: int
    estado: Optional[str]
    subsistema: Optional[str]
    primeira_data: Optional[str]
    ultima_data: Optional[str]


class GenerationPoint(BaseModel):
    periodo: str
    geracao_verificada_mwh: Optional[float]
    geracao_estimada_mwh: Optional[float]
    geracao_limitada_mwh: Optional[float]
    velocidade_vento_media: Optional[float]


class RestrictionSummaryItem(BaseModel):
    cod_razaorestricao: str
    nom_razaorestricao: Optional[str]
    projeto: Optional[str]
    horas_com_restricao: float
    mwh_restringidos: Optional[float]
    n_ocorrencias: int



@app.get("/health", response_model=HealthResponse, tags=["Sistema"])
def health_check():
    """Health check. Verifica se o warehouse está acessível."""
    return HealthResponse(
        status="ok" if DB_PATH.exists() else "degraded",
        db_path=str(DB_PATH),
        db_exists=DB_PATH.exists(),
    )


@app.get("/projects", response_model=list[ProjectMetadata], tags=["Projetos"])
def list_projects():
    """
    Lista todos os projetos disponíveis com metadados básicos.
    Um projeto agrupa um conjunto de SPEs da Casa dos Ventos.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                s.projeto,
                COUNT(DISTINCT s.sk_spe)              AS n_spes,
                MODE(SPLIT_PART(s.ceg_completo, '.', 3)) AS estado,
                MIN(t.din_referencia)::VARCHAR          AS primeira_data,
                MAX(t.din_referencia)::VARCHAR          AS ultima_data
            FROM fato_geracao_spe f
            JOIN dim_spe   s ON s.sk_spe   = f.sk_spe
            JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
            GROUP BY s.projeto
            ORDER BY s.projeto
        """).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar projetos: {exc}")
    finally:
        conn.close()

    subsistema_map = {
        "BA": "Nordeste", "CE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste",
        "PE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", "AL": "Nordeste",
        "SE": "Nordeste", "RS": "Sul", "SC": "Sul", "PR": "Sul",
    }

    return [
        ProjectMetadata(
            projeto=r[0], n_spes=r[1], estado=r[2],
            subsistema=subsistema_map.get(r[2], "Desconhecido"),
            primeira_data=r[3], ultima_data=r[4],
        )
        for r in rows
    ]


@app.get(
    "/generation/{project_id}",
    response_model=list[GenerationPoint],
    tags=["Geração"],
)
def get_generation(
    project_id: str,
    granularidade: str = Query(
        "diario",
        description="Agrupamento temporal: 'diario' ou 'mensal'",
        pattern="^(diario|mensal)$",
    ),
    data_inicio: Optional[date] = Query(None, description="Data início (YYYY-MM-DD)"),
    data_fim: Optional[date] = Query(None, description="Data fim (YYYY-MM-DD)"),
):
    """
    Retorna dados de geração agregados de um projeto.
    As métricas são somadas sobre todas as SPEs do projeto.
    """
    conn = get_conn()

    # Valida projeto
    exists = conn.execute(
        "SELECT 1 FROM dim_spe WHERE projeto = ? LIMIT 1", [project_id]
    ).fetchone()
    if not exists:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Projeto '{project_id}' não encontrado.")

    period_expr = (
        "strftime(t.din_referencia, '%Y-%m-%d')"
        if granularidade == "diario"
        else "strftime(t.din_referencia, '%Y-%m')"
    )

    filters = ["s.projeto = ?"]
    params: list = [project_id]

    if data_inicio:
        filters.append("t.dat_data >= ?")
        params.append(str(data_inicio))
    if data_fim:
        filters.append("t.dat_data <= ?")
        params.append(str(data_fim))

    where = " AND ".join(filters)

    try:
        rows = conn.execute(f"""
            SELECT
                {period_expr}                                  AS periodo,
                SUM(f.val_geracaoVerificada)  / 2.0            AS geracao_verificada_mwh,
                SUM(f.val_geracaoEstimada)    / 2.0            AS geracao_estimada_mwh,
                SUM(f.val_geracaoLimitada)    / 2.0            AS geracao_limitada_mwh,
                AVG(f.val_velocidadeVento)                     AS velocidade_vento_media
            FROM fato_geracao_spe f
            JOIN dim_spe   s ON s.sk_spe   = f.sk_spe
            JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
            WHERE {where}
            GROUP BY {period_expr}
            ORDER BY periodo
        """, params).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

    return [
        GenerationPoint(
            periodo=r[0],
            geracao_verificada_mwh=r[1],
            geracao_estimada_mwh=r[2],
            geracao_limitada_mwh=r[3],
            velocidade_vento_media=r[4],
        )
        for r in rows
    ]


@app.get(
    "/restrictions/summary",
    response_model=list[RestrictionSummaryItem],
    tags=["Restrições"],
)
def get_restrictions_summary(
    projeto: Optional[str] = Query(None, description="Filtrar por projeto"),
    data_inicio: Optional[date] = Query(None),
    data_fim: Optional[date] = Query(None),
):
    """
    Resumo de constrained-off: total de horas e MWh restringidos,
    agrupados por razão de restrição.
    """
    conn = get_conn()

    filters = ["r.cod_razaorestricao != 'SEM_REST'"]
    params: list = []

    if projeto:
        filters.append("s.projeto = ?")
        params.append(projeto)
    if data_inicio:
        filters.append("t.dat_data >= ?")
        params.append(str(data_inicio))
    if data_fim:
        filters.append("t.dat_data <= ?")
        params.append(str(data_fim))

    where = " AND ".join(filters) if filters else "1=1"

    try:
        rows = conn.execute(f"""
            SELECT
                r.cod_razaorestricao,
                r.nom_razaorestricao,
                s.projeto,
                -- cada registro = 30 min = 0.5 h
                COUNT(*) * 0.5                                  AS horas_com_restricao,
                -- MWh = MW × 0.5h, calculado como diferença entre estimada e verificada
                SUM(f.val_geracaoEstimada - f.val_geracaoVerificada) / 2.0
                                                                AS mwh_restringidos,
                COUNT(*)                                        AS n_ocorrencias
            FROM fato_geracao_spe f
            JOIN dim_spe      s ON s.sk_spe      = f.sk_spe
            JOIN dim_tempo    t ON t.sk_tempo    = f.sk_tempo
            JOIN dim_restricao r ON r.sk_restricao = f.sk_restricao
            WHERE {where}
            GROUP BY r.cod_razaorestricao, r.nom_razaorestricao, s.projeto
            ORDER BY horas_com_restricao DESC
        """, params).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

    return [
        RestrictionSummaryItem(
            cod_razaorestricao=r[0],
            nom_razaorestricao=r[1],
            projeto=r[2],
            horas_com_restricao=r[3],
            mwh_restringidos=r[4],
            n_ocorrencias=r[5],
        )
        for r in rows
    ]
