#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


MOJIBAKE_RE = re.compile(r"(Ã|Â|â€|�)")
DATE_NAME_RE = re.compile(r"(^|_)(dat|dt|data)(_|\b)|date", re.I)
VALUE_NAME_RE = re.compile(r"(vlr|valor|total|preco|saldo|r\$)", re.I)
ID_NAME_RE = re.compile(r"(cnpj|apf|contrato|cod_|codigo|^nr_|^nu_)", re.I)


def read_csv_any(path: Path, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, **kwargs).fillna("")
    except UnicodeDecodeError:
        return pd.read_csv(
            path, dtype=str, keep_default_na=False, encoding="latin1", **kwargs
        ).fillna("")


def latest(root: Path, patterns: list[str]) -> Path | None:
    files: list[Path] = []
    for pattern in patterns:
        files.extend(root.glob(pattern))
    return sorted(files)[-1] if files else None


def parse_number(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({"": pd.NA, "-": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def inspect_treated(
    path: Path, table_name: str, sample_rows: int
) -> tuple[dict[str, object], str]:
    try:
        df = read_csv_any(path, sep="\t")
    except Exception:
        df = read_csv_any(path)

    total_cells = max(int(df.shape[0] * df.shape[1]), 1)
    text_df = df.astype(str)
    empty_rows = int(
        text_df.apply(
            lambda row: all(v.strip() in ("", "-") for v in row), axis=1
        ).sum()
    )
    empty_cols = int(
        sum(text_df[c].map(lambda v: v.strip() in ("", "-")).all() for c in df.columns)
    )
    duplicate_rows = int(df.duplicated().sum()) if len(df) else 0
    duplicate_cols = int(pd.Index(df.columns).duplicated().sum())
    unnamed_cols = int(
        sum(
            str(c).lower().startswith("unnamed") or str(c).strip() == ""
            for c in df.columns
        )
    )
    mojibake_cells = int(
        text_df.apply(lambda s: s.str.contains(MOJIBAKE_RE, regex=True, na=False))
        .sum()
        .sum()
    )

    date_cols = [c for c in df.columns if DATE_NAME_RE.search(str(c))]
    date_checks = []
    for col in date_cols[:12]:
        raw = df[col].astype(str).replace({"": pd.NA, "-": pd.NA}).dropna()
        parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)
        denom = max(len(raw), 1)
        date_checks.append(f"{col}:{round(float(parsed.notna().sum() / denom), 3)}")

    value_cols = [c for c in df.columns if VALUE_NAME_RE.search(str(c))]
    value_checks = []
    for col in value_cols[:12]:
        raw = df[col].astype(str).replace({"": pd.NA, "-": pd.NA}).dropna()
        parsed = parse_number(raw)
        denom = max(len(raw), 1)
        value_checks.append(f"{col}:{round(float(parsed.notna().sum() / denom), 3)}")

    id_cols = [c for c in df.columns if ID_NAME_RE.search(str(c))]
    id_null_rates = []
    leading_zero_risk = 0
    for col in id_cols[:12]:
        raw = df[col].astype(str)
        denom = max(len(raw), 1)
        null_rate = float(
            raw.map(lambda v: v.strip() in ("", "-", "nan", "None")).sum() / denom
        )
        id_null_rates.append(f"{col}:{round(null_rate, 3)}")
        leading_zero_risk += int(raw.str.match(r"^0+\d+", na=False).sum())

    sample = df.head(sample_rows).to_string(index=False)
    return (
        {
            "table_name": table_name,
            "treated_file": str(path),
            "treated_exists": True,
            "treated_n_rows": int(df.shape[0]),
            "treated_n_cols": int(df.shape[1]),
            "empty_row_ratio": round(empty_rows / max(len(df), 1), 6),
            "empty_cols": empty_cols,
            "duplicate_row_ratio": round(duplicate_rows / max(len(df), 1), 6),
            "duplicate_cols": duplicate_cols,
            "unnamed_cols": unnamed_cols,
            "mojibake_cells": mojibake_cells,
            "date_parse_rates": ";".join(date_checks),
            "value_parse_rates": ";".join(value_checks),
            "id_null_rates": ";".join(id_null_rates),
            "leading_zero_id_values": leading_zero_risk,
        },
        sample,
    )


def build_sftp(root: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    repo_root = root.parents[1] if len(root.parents) > 1 else root
    for path in sorted(repo_root.glob("diff_sftp_minio*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            rows.append({"artefato": path.name, "erro": str(exc)})
            continue
        if isinstance(data, list):
            rows.append(
                {
                    "artefato": path.name,
                    "tipo": "lista",
                    "registros": len(data),
                    "same_size_false": sum(
                        1
                        for item in data
                        if isinstance(item, dict) and item.get("same_size") is False
                    ),
                    "container_mapped": sum(
                        1
                        for item in data
                        if isinstance(item, dict) and item.get("container_path")
                    ),
                }
            )
        elif isinstance(data, dict):
            rows.append(
                {
                    "artefato": path.name,
                    "tipo": "diff",
                    "registros": "",
                    "added": len(data.get("added", [])),
                    "removed": len(data.get("removed", [])),
                    "modified": len(data.get("modified", [])),
                    "unchanged": len(data.get("unchanged", [])),
                }
            )
    columns = [
        "artefato",
        "tipo",
        "registros",
        "added",
        "removed",
        "modified",
        "unchanged",
        "same_size_false",
        "container_mapped",
        "erro",
    ]
    return pd.DataFrame(rows, columns=columns)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", default=".", help="dados-historicos-tratamento directory"
    )
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--sample-rows", type=int, default=8)
    parser.add_argument(
        "--limit-tables", type=int, default=0, help="0 means all treated files found"
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    out = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else root / "docs" / "evidencias" / "revisao-tratamento-dados"
    )
    out.mkdir(parents=True, exist_ok=True)

    quality_path = latest(
        root,
        [
            "*_qualidade_*.csv",
            "data/*_qualidade_*.csv",
            "data/treated_tables/_quality_report.csv",
        ],
    )
    dedup_path = latest(
        root,
        [
            "*_dedup_map_*.csv",
            "data/*_dedup_map_*.csv",
            "data/treated_tables/_dedup_map.csv",
        ],
    )
    class_path = latest(
        root,
        [
            "*_classificacao_*.csv",
            "data/*_classificacao_*.csv",
            "data/classificacao_formacao.csv",
        ],
    )
    inventory_path = root / "data" / "inventario_dados.csv"

    if not quality_path:
        raise SystemExit(f"No quality report found under {root}")

    quality = read_csv_any(quality_path)
    audit = quality.copy()
    audit["evidence_quality_file"] = str(quality_path)

    if class_path:
        cls = read_csv_any(class_path).rename(
            columns={
                "formacao": "classificacao_formacao",
                "confidence": "classificacao_confidence",
            }
        )
        audit = audit.merge(
            cls[
                [
                    c
                    for c in [
                        "table_name",
                        "classificacao_formacao",
                        "classificacao_confidence",
                    ]
                    if c in cls.columns
                ]
            ],
            on="table_name",
            how="left",
        )

    if inventory_path.exists():
        inv = pd.read_csv(
            inventory_path, sep="\t", dtype=str, keep_default_na=False
        ).fillna("")
        keep = [
            c
            for c in [
                "table_name",
                "n_linhas",
                "n_colunas",
                "status_tratamento",
                "perfil",
                "score_utilidade_preditiva",
            ]
            if c in inv.columns
        ]
        audit = audit.merge(
            inv[keep], on="table_name", how="left", suffixes=("", "_inventario")
        )

    if dedup_path:
        dedup = read_csv_any(dedup_path).rename(columns={"source_table": "table_name"})
        keep = [
            c
            for c in [
                "table_name",
                "canonical_table",
                "content_hash",
                "is_duplicate",
                "file_size",
            ]
            if c in dedup.columns
        ]
        audit = audit.merge(dedup[keep], on="table_name", how="left")

    treated_dir = root / "data" / "treated_tables"
    treated_rows: list[dict[str, object]] = []
    sample_rows: list[dict[str, object]] = []
    files = sorted(treated_dir.glob("*_tratado.csv")) if treated_dir.exists() else []
    if args.limit_tables:
        files = files[: args.limit_tables]
    for path in files:
        table_name = path.name.removesuffix("_tratado.csv")
        row, sample = inspect_treated(path, table_name, args.sample_rows)
        treated_rows.append(row)
        sample_rows.append({"table_name": table_name, "sample_print": sample})

    treated_audit = pd.DataFrame(treated_rows)
    if not treated_audit.empty:
        audit = audit.merge(treated_audit, on="table_name", how="left")
    else:
        audit["treated_exists"] = False

    audit["flag_revisao"] = ""
    audit.loc[audit.get("status", "").astype(str).eq("error"), "flag_revisao"] += (
        "status_error;"
    )
    audit.loc[audit.get("status", "").astype(str).eq("discarded"), "flag_revisao"] += (
        "status_discarded;"
    )
    audit.loc[
        pd.to_numeric(audit.get("n_rows", 0), errors="coerce").fillna(0).eq(0),
        "flag_revisao",
    ] += "zero_rows;"
    audit.loc[
        pd.to_numeric(audit.get("n_cols", 0), errors="coerce").fillna(0).eq(0),
        "flag_revisao",
    ] += "zero_cols;"
    audit.loc[
        pd.to_numeric(audit.get("missing_pct", 0), errors="coerce").fillna(0).gt(30),
        "flag_revisao",
    ] += "missing_gt_30;"
    audit.loc[
        audit.get("institution", "").astype(str).isin(["", "unknown"]), "flag_revisao"
    ] += "institution_unknown;"
    audit.loc[audit.get("report_date", "").astype(str).eq(""), "flag_revisao"] += (
        "report_date_missing;"
    )
    for col, flag in [
        ("encoding_issues", "encoding_warning"),
        ("date_parse_errors", "date_parse_warning"),
        ("type_coercion_warnings", "type_warning"),
    ]:
        if col in audit.columns:
            audit.loc[
                pd.to_numeric(audit[col], errors="coerce").fillna(0).gt(0),
                "flag_revisao",
            ] += f"{flag};"
    for col, flag in [
        ("mojibake_cells", "mojibake_detected"),
        ("duplicate_cols", "duplicate_columns"),
        ("empty_cols", "empty_columns"),
    ]:
        if col in audit.columns:
            audit.loc[
                pd.to_numeric(audit[col], errors="coerce").fillna(0).gt(0),
                "flag_revisao",
            ] += f"{flag};"

    flags = audit[audit["flag_revisao"].astype(str).ne("")].copy()
    sftp = build_sftp(root)
    samples = pd.DataFrame(sample_rows, columns=["table_name", "sample_print"])
    if samples.empty:
        samples = pd.DataFrame(
            [
                {
                    "table_name": "",
                    "sample_print": "Nenhum CSV tratado encontrado em data/treated_tables/.",
                }
            ]
        )

    dedup_summary = pd.DataFrame()
    if dedup_path:
        dedup_raw = read_csv_any(dedup_path)
        dedup_summary = pd.DataFrame(
            [
                {
                    "dedup_file": str(dedup_path),
                    "linhas": len(dedup_raw),
                    "duplicatas": int(
                        dedup_raw.get("is_duplicate", pd.Series(dtype=str))
                        .astype(str)
                        .str.lower()
                        .isin(["true", "1", "yes"])
                        .sum()
                    ),
                    "hashes_unicos": int(
                        dedup_raw.get("content_hash", pd.Series(dtype=str)).nunique()
                    )
                    if "content_hash" in dedup_raw.columns
                    else "",
                }
            ]
        )

    summary = {
        "quality_file": str(quality_path),
        "dedup_file": str(dedup_path) if dedup_path else "",
        "classification_file": str(class_path) if class_path else "",
        "tables_in_quality": int(len(quality)),
        "treated_files_found": int(len(files)),
        "flags": int(len(flags)),
        "status_error": int((audit.get("status", "").astype(str) == "error").sum()),
        "status_discarded": int(
            (audit.get("status", "").astype(str) == "discarded").sum()
        ),
    }

    audit.to_csv(out / "auditoria_tratamento_pandas.csv", index=False)
    flags.to_csv(out / "flags_tratamento_pandas.csv", index=False)
    samples.to_csv(out / "amostras_tratadas_pandas.csv", index=False)
    sftp.to_csv(out / "impacto_sftp_tratamento_pandas.csv", index=False)
    dedup_summary.to_csv(out / "deduplicacao_tratamento_pandas.csv", index=False)
    (out / "summary_tratamento.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
