#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from inventario_sharepoint_local_mcmv import classify_text, infer_source, primary_front


def load_env(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def norm_text(value: Any) -> str:
    value = "" if pd.isna(value) else str(value)
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def env_bool(value: str, default: bool = True) -> bool:
    if value == "":
        return default
    return value.lower() in {"1", "true", "yes", "sim"}


def superset_session(env: dict[str, str]) -> tuple[requests.Session, str]:
    required = ["SUPERSET_URL", "SUPERSET_USERNAME", "SUPERSET_PASSWORD"]
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise SystemExit("Variaveis Superset ausentes no local.env: " + ", ".join(missing))

    base_url = env["SUPERSET_URL"].rstrip("/")
    session = requests.Session()
    session.verify = env_bool(env.get("SUPERSET_VERIFY_SSL", "true"), default=True)
    try:
        login = session.post(
            f"{base_url}/api/v1/security/login",
            json={
                "username": env["SUPERSET_USERNAME"],
                "password": env["SUPERSET_PASSWORD"],
                "provider": env.get("SUPERSET_PROVIDER", "db"),
                "refresh": True,
            },
            timeout=60,
        )
    except requests.exceptions.ConnectionError as exc:
        raise SystemExit(f"Nao foi possivel conectar ao Superset em {base_url}: {exc}") from exc
    if login.status_code >= 400:
        raise SystemExit(f"Falha login Superset HTTP {login.status_code}: {login.text[:500]}")
    token = login.json().get("access_token") or login.json().get("result", {}).get("access_token")
    if not token:
        raise SystemExit("Superset nao retornou access_token.")
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session, base_url


def fetch_collection(session: requests.Session, base_url: str, endpoint: str, page_size: int = 100) -> list[dict]:
    rows: list[dict] = []
    page = 0
    while True:
        response = session.get(
            f"{base_url}/api/v1/{endpoint}/",
            params={"q": f"(page:{page},page_size:{page_size})"},
            timeout=60,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Superset {endpoint} HTTP {response.status_code}: {response.text[:500]}")
        payload = response.json()
        result = payload.get("result", [])
        if isinstance(result, dict):
            result_rows = result.get("data") or result.get("result") or []
            count = result.get("count") or payload.get("count")
        else:
            result_rows = result
            count = payload.get("count")
        rows.extend(result_rows)
        if not result_rows or len(result_rows) < page_size:
            break
        if count is not None and len(rows) >= int(count):
            break
        page += 1
    return rows


def fetch_detail(session: requests.Session, base_url: str, endpoint: str, item_id: Any) -> dict[str, Any]:
    response = session.get(f"{base_url}/api/v1/{endpoint}/{item_id}", timeout=60)
    if response.status_code >= 400:
        return {"_detail_error": f"HTTP {response.status_code}: {response.text[:300]}"}
    result = response.json().get("result", {})
    return result if isinstance(result, dict) else {"result": result}


def flatten_database(value: Any) -> str:
    if isinstance(value, dict):
        return value.get("database_name") or value.get("name") or str(value.get("id", ""))
    return "" if pd.isna(value) else str(value)


def dataset_rows(session: requests.Session, base_url: str) -> pd.DataFrame:
    rows = []
    for item in fetch_collection(session, base_url, "dataset"):
        detail = fetch_detail(session, base_url, "dataset", item.get("id"))
        merged = {**item, **{f"detail_{k}": v for k, v in detail.items() if k not in item}}
        table = item.get("table_name") or detail.get("table_name") or ""
        schema = item.get("schema") or detail.get("schema") or ""
        database = flatten_database(item.get("database") or detail.get("database"))
        text = " ".join(str(v) for v in [database, schema, table, item.get("datasource_name"), detail.get("description")] if v)
        fronts = classify_text(text)
        columns = detail.get("columns") if isinstance(detail.get("columns"), list) else []
        metrics = detail.get("metrics") if isinstance(detail.get("metrics"), list) else []
        rows.append(
            {
                "dataset_id": item.get("id"),
                "database": database,
                "schema": schema,
                "table_name": table,
                "datasource_name": item.get("datasource_name") or detail.get("datasource_name") or "",
                "kind": item.get("kind") or detail.get("kind") or "",
                "changed_on": item.get("changed_on") or detail.get("changed_on") or "",
                "changed_by": str(item.get("changed_by") or detail.get("changed_by") or ""),
                "frente_inferida": fronts,
                "frente_primaria": primary_front(fronts),
                "fonte_inferida": infer_source(text),
                "colunas_qtd": len(columns),
                "metricas_qtd": len(metrics),
                "colunas_amostra": "; ".join(str(col.get("name", "")) for col in columns[:30] if isinstance(col, dict)),
                "metricas_amostra": "; ".join(str(metric.get("metric_name", "")) for metric in metrics[:30] if isinstance(metric, dict)),
                "table_norm": norm_text(table),
                "schema_table_norm": norm_text(f"{schema}_{table}"),
                "raw_json": json.dumps(merged, ensure_ascii=False, default=str)[:5000],
            }
        )
    return pd.DataFrame(rows)


def chart_rows(session: requests.Session, base_url: str) -> pd.DataFrame:
    rows = []
    for item in fetch_collection(session, base_url, "chart"):
        text = " ".join(str(item.get(k, "")) for k in ["slice_name", "datasource_name", "viz_type", "description"])
        fronts = classify_text(text)
        rows.append(
            {
                "chart_id": item.get("id"),
                "slice_name": item.get("slice_name") or "",
                "viz_type": item.get("viz_type") or "",
                "datasource_id": item.get("datasource_id") or "",
                "datasource_name": item.get("datasource_name") or "",
                "changed_on": item.get("changed_on") or "",
                "frente_inferida": fronts,
                "frente_primaria": primary_front(fronts),
                "raw_json": json.dumps(item, ensure_ascii=False, default=str)[:5000],
            }
        )
    return pd.DataFrame(rows)


def dashboard_rows(session: requests.Session, base_url: str) -> pd.DataFrame:
    rows = []
    for item in fetch_collection(session, base_url, "dashboard"):
        text = " ".join(str(item.get(k, "")) for k in ["dashboard_title", "slug", "published"])
        fronts = classify_text(text)
        rows.append(
            {
                "dashboard_id": item.get("id"),
                "dashboard_title": item.get("dashboard_title") or "",
                "slug": item.get("slug") or "",
                "published": item.get("published") or "",
                "changed_on": item.get("changed_on") or "",
                "frente_inferida": fronts,
                "frente_primaria": primary_front(fronts),
                "raw_json": json.dumps(item, ensure_ascii=False, default=str)[:5000],
            }
        )
    return pd.DataFrame(rows)


def read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def build_match_sets(df: pd.DataFrame, name_cols: list[str]) -> set[str]:
    values: set[str] = set()
    if df.empty:
        return values
    for col in name_cols:
        if col in df.columns:
            values.update(df[col].dropna().map(norm_text).tolist())
            values.update(df[col].dropna().map(lambda v: norm_text(Path(str(v)).stem)).tolist())
    return {v for v in values if v}


def semantic_counts(df: pd.DataFrame) -> set[str]:
    if df.empty or "frente_primaria" not in df.columns:
        return set()
    return set(df["frente_primaria"].dropna().astype(str))


def compare_datasets(datasets: pd.DataFrame, sharepoint: pd.DataFrame, minio_all: pd.DataFrame, minio_staging: pd.DataFrame) -> pd.DataFrame:
    sp_names = build_match_sets(sharepoint, ["nome", "caminho_relativo", "arquivo_origem"])
    mi_names = build_match_sets(minio_all, ["filename", "object_name"])
    stg_names = build_match_sets(minio_staging, ["filename", "object_name"])
    sp_fronts = semantic_counts(sharepoint)
    mi_fronts = semantic_counts(minio_all)
    stg_fronts = semantic_counts(minio_staging)

    rows = []
    for _, row in datasets.iterrows():
        candidates = {row.get("table_norm", ""), row.get("schema_table_norm", "")}
        candidates = {c for c in candidates if c}
        front = str(row.get("frente_primaria", "nao_classificado"))
        rows.append(
            {
                "dataset_id": row.get("dataset_id"),
                "database": row.get("database"),
                "schema": row.get("schema"),
                "table_name": row.get("table_name"),
                "frente_primaria": front,
                "sharepoint_match_nome": bool(candidates & sp_names),
                "sharepoint_tem_frente": front in sp_fronts,
                "minio_bucket_match_nome": bool(candidates & mi_names),
                "minio_bucket_tem_frente": front in mi_fronts,
                "minio_staging_match_nome": bool(candidates & stg_names),
                "minio_staging_tem_frente": front in stg_fronts,
                "colunas_qtd": row.get("colunas_qtd"),
                "metricas_qtd": row.get("metricas_qtd"),
                "colunas_amostra": row.get("colunas_amostra"),
            }
        )
    return pd.DataFrame(rows)


def write_report(out_dir: Path, datasets: pd.DataFrame, charts: pd.DataFrame, dashboards: pd.DataFrame, comparison: pd.DataFrame) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    front_summary = (
        datasets.groupby("frente_primaria", dropna=False)
        .size()
        .reset_index(name="datasets")
        .sort_values(["datasets", "frente_primaria"], ascending=[False, True])
    )
    comp_summary = (
        comparison.groupby(["frente_primaria", "sharepoint_tem_frente", "minio_bucket_tem_frente", "minio_staging_tem_frente"], dropna=False)
        .size()
        .reset_index(name="datasets")
        .sort_values(["datasets", "frente_primaria"], ascending=[False, True])
    )
    front_summary.to_csv(out_dir / "superset_resumo_datasets_por_frente.csv", index=False)
    comp_summary.to_csv(out_dir / "superset_resumo_existencia_por_frente.csv", index=False)

    lines = [
        "# Inventario Superset x SharePoint x MinIO - MCMV",
        "",
        f"- Gerado em: `{now}`",
        f"- Datasets Superset: `{len(datasets)}`",
        f"- Charts Superset: `{len(charts)}`",
        f"- Dashboards Superset: `{len(dashboards)}`",
        "",
        "## Resumo de datasets por frente inferida",
        "",
        "| Frente | Datasets |",
        "|---|---:|",
    ]
    for _, row in front_summary.iterrows():
        lines.append(f"| {row['frente_primaria']} | {row['datasets']} |")
    lines.extend(
        [
            "",
            "## Existencia das frentes em SharePoint/MinIO",
            "",
            "| Frente | SharePoint tem frente | MinIO bucket tem frente | MinIO staging tem frente | Datasets |",
            "|---|---|---|---|---:|",
        ]
    )
    for _, row in comp_summary.iterrows():
        lines.append(
            f"| {row['frente_primaria']} | {row['sharepoint_tem_frente']} | {row['minio_bucket_tem_frente']} | {row['minio_staging_tem_frente']} | {row['datasets']} |"
        )
    lines.extend(
        [
            "",
            "## Evidencias geradas",
            "",
            "- `superset_datasets.csv`: datasets publicados no Superset.",
            "- `superset_charts.csv`: graficos publicados no Superset.",
            "- `superset_dashboards.csv`: dashboards publicados no Superset.",
            "- `superset_datasets_vs_sharepoint_minio.csv`: batimento dataset a dataset.",
            "- `superset_resumo_datasets_por_frente.csv`: resumo por frente inferida.",
            "- `superset_resumo_existencia_por_frente.csv`: resumo de existencia por frente.",
            "",
            "Observacao: match por nome exige nomes fisicos semelhantes. Quando nao houver match por nome, use `*_tem_frente` como sinal de cobertura semantica, nao como prova de equivalencia arquivo-tabela.",
        ]
    )
    (out_dir / "superset_inventario_mcmv.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default="local.env")
    parser.add_argument(
        "--output-dir",
        default="data-science/dados-historicos-tratamento/docs/evidencias/superset-mcmv",
    )
    parser.add_argument(
        "--sharepoint-csv",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-mcmv-local/sharepoint_local_candidatos_mcmv.csv",
    )
    parser.add_argument(
        "--minio-all-csv",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-minio-diff-all/minio_inventory_prefixos_consultados.csv",
    )
    parser.add_argument(
        "--minio-staging-csv",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-minio-diff/minio_inventory_prefixos_consultados.csv",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env = load_env(Path(args.env_file))
    session, base_url = superset_session(env)
    print(f"Conectado ao Superset: {base_url}", flush=True)

    datasets = dataset_rows(session, base_url)
    charts = chart_rows(session, base_url)
    dashboards = dashboard_rows(session, base_url)
    sharepoint = read_optional_csv(Path(args.sharepoint_csv))
    minio_all = read_optional_csv(Path(args.minio_all_csv))
    minio_staging = read_optional_csv(Path(args.minio_staging_csv))
    comparison = compare_datasets(datasets, sharepoint, minio_all, minio_staging)

    datasets.to_csv(out_dir / "superset_datasets.csv", index=False)
    charts.to_csv(out_dir / "superset_charts.csv", index=False)
    dashboards.to_csv(out_dir / "superset_dashboards.csv", index=False)
    comparison.to_csv(out_dir / "superset_datasets_vs_sharepoint_minio.csv", index=False)
    write_report(out_dir, datasets, charts, dashboards, comparison)

    print(f"Inventario Superset salvo em: {out_dir}")
    print(f"Datasets: {len(datasets)} | Charts: {len(charts)} | Dashboards: {len(dashboards)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
