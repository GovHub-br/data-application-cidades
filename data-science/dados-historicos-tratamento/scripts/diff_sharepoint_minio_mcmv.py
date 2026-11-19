#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from minio import Minio

from inventario_sharepoint_local_mcmv import classify_text, extract_period, file_category, infer_source, primary_front


DATA_EXTENSIONS = {
    ".csv",
    ".txt",
    ".xlsx",
    ".xls",
    ".xlsm",
    ".parquet",
    ".json",
    ".zip",
    ".pbix",
    ".pdf",
    ".docx",
    ".pptx",
}

MINIO_COLUMNS = [
    "bucket",
    "prefixo_consultado",
    "object_name",
    "top_prefix",
    "filename",
    "extension",
    "category",
    "size_bytes",
    "last_modified",
    "periodo_inferido",
    "fonte_inferida",
    "frente_inferida",
    "frente_primaria",
    "filename_norm",
    "stem_norm",
    "object_norm",
]


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


def norm_stem(value: Any) -> str:
    return norm_text(Path(str(value)).stem)


def client_from_env(env_file: Path) -> tuple[Minio, str]:
    env = load_env(env_file)
    required = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"]
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise SystemExit("Variaveis MinIO ausentes: " + ", ".join(missing))
    secure = env.get("MINIO_SECURE", "false").lower() in {"1", "true", "yes", "sim"}
    client = Minio(
        env["MINIO_ENDPOINT"],
        access_key=env["MINIO_ACCESS_KEY"],
        secret_key=env["MINIO_SECRET_KEY"],
        secure=secure,
    )
    return client, env["MINIO_BUCKET"]


def inventory_minio(client: Minio, bucket: str, prefixes: list[str]) -> pd.DataFrame:
    rows = []
    for prefix in prefixes:
        print(f"Listando MinIO: bucket={bucket} prefix={prefix or '<todos>'}", flush=True)
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            name = obj.object_name
            filename = Path(name).name
            ext = Path(filename).suffix.lower()
            front_tags = classify_text(name)
            rows.append(
                {
                    "bucket": bucket,
                    "prefixo_consultado": prefix,
                    "object_name": name,
                    "top_prefix": name.split("/", 1)[0] if "/" in name else "",
                    "filename": filename,
                    "extension": ext,
                    "category": file_category(ext),
                    "size_bytes": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else "",
                    "periodo_inferido": extract_period(name),
                    "fonte_inferida": infer_source(name),
                    "frente_inferida": front_tags,
                    "frente_primaria": primary_front(front_tags),
                    "filename_norm": norm_text(filename),
                    "stem_norm": norm_stem(filename),
                    "object_norm": norm_text(name),
                }
            )
    return pd.DataFrame(rows, columns=MINIO_COLUMNS)


def prepare_sharepoint(sp_path: Path) -> pd.DataFrame:
    sp = pd.read_csv(sp_path)
    sp = sp[sp["tipo"].eq("arquivo") & sp["categoria"].ne("outro")].copy()
    sp["filename_norm"] = sp["nome"].map(norm_text)
    sp["stem_norm"] = sp["nome"].map(norm_stem)
    sp["path_norm"] = (sp["arquivo_origem"].fillna("") + "/" + sp["caminho_relativo"].fillna("")).map(norm_text)
    return sp


def examples(rows: pd.DataFrame, limit: int = 5) -> str:
    if rows.empty or "object_name" not in rows.columns:
        return ""
    return " | ".join(rows["object_name"].head(limit).astype(str).tolist())


def diff_sharepoint_minio(sp: pd.DataFrame, mi: pd.DataFrame) -> pd.DataFrame:
    by_filename = defaultdict(list)
    by_stem = defaultdict(list)
    by_semantic = defaultdict(list)
    for idx, row in mi.iterrows():
        by_filename[row["filename_norm"]].append(idx)
        by_stem[row["stem_norm"]].append(idx)
        sem_key = (row["frente_primaria"], row["periodo_inferido"], row["fonte_inferida"])
        by_semantic[sem_key].append(idx)

    rows = []
    for _, row in sp.iterrows():
        exact_idx = by_filename.get(row["filename_norm"], [])
        stem_idx = by_stem.get(row["stem_norm"], [])
        sem_key = (row["frente_primaria"], row.get("periodo_inferido", ""), row.get("fonte_inferida", ""))
        semantic_idx = by_semantic.get(sem_key, [])
        if exact_idx:
            match_type = "existe_nome_exato"
            match_idx = exact_idx
        elif stem_idx:
            match_type = "existe_mesmo_stem_extensao_diferente"
            match_idx = stem_idx
        elif semantic_idx:
            match_type = "pista_semantica_mesma_frente_periodo_fonte"
            match_idx = semantic_idx
        else:
            match_type = "nao_encontrado_no_minio_consultado"
            match_idx = []
        matches = mi.loc[match_idx] if match_idx else mi.iloc[0:0]
        rows.append(
            {
                "sharepoint_nome": row["nome"],
                "sharepoint_origem": row.get("arquivo_origem", ""),
                "sharepoint_caminho": row.get("caminho_relativo", ""),
                "sharepoint_extensao": row.get("extensao", ""),
                "sharepoint_categoria": row.get("categoria", ""),
                "sharepoint_frente_primaria": row.get("frente_primaria", ""),
                "sharepoint_fonte_inferida": row.get("fonte_inferida", ""),
                "sharepoint_periodo_inferido": row.get("periodo_inferido", ""),
                "minio_match_type": match_type,
                "minio_match_count": len(match_idx),
                "minio_match_examples": examples(matches),
            }
        )
    return pd.DataFrame(rows)


def minio_only(mi: pd.DataFrame, sp: pd.DataFrame) -> pd.DataFrame:
    if mi.empty:
        return pd.DataFrame(columns=mi.columns)
    sp_names = set(sp["filename_norm"])
    sp_stems = set(sp["stem_norm"])
    result = mi[~mi["filename_norm"].isin(sp_names) & ~mi["stem_norm"].isin(sp_stems)].copy()
    return result


def write_outputs(sp: pd.DataFrame, mi: pd.DataFrame, diff: pd.DataFrame, out_dir: Path, prefixes: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    mi.to_csv(out_dir / "minio_inventory_prefixos_consultados.csv", index=False)
    diff.to_csv(out_dir / "sharepoint_vs_minio_diff.csv", index=False)
    only = minio_only(mi, sp)
    only.to_csv(out_dir / "minio_only_sem_match_sharepoint.csv", index=False)

    summary = (
        diff.groupby(["sharepoint_frente_primaria", "minio_match_type"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["sharepoint_frente_primaria", "quantidade"], ascending=[True, False])
    )
    mi_summary = (
        mi.groupby(["top_prefix", "frente_primaria", "fonte_inferida", "extension"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["quantidade", "top_prefix"], ascending=[False, True])
    )
    summary.to_csv(out_dir / "sharepoint_vs_minio_resumo_por_frente.csv", index=False)
    mi_summary.to_csv(out_dir / "minio_resumo_por_prefixo_frente.csv", index=False)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    prefix_label = ", ".join(prefix or "<todos>" for prefix in prefixes)
    total = len(diff)
    strong = diff["minio_match_type"].isin(["existe_nome_exato", "existe_mesmo_stem_extensao_diferente"]).sum()
    weak = diff["minio_match_type"].eq("pista_semantica_mesma_frente_periodo_fonte").sum()
    missing = diff["minio_match_type"].eq("nao_encontrado_no_minio_consultado").sum()
    lines = [
        "# Diff SharePoint local x MinIO",
        "",
        f"- Gerado em: `{now}`",
        f"- Prefixos MinIO consultados: `{prefix_label}`",
        f"- Arquivos candidatos no SharePoint local: `{total}`",
        f"- Objetos MinIO inventariados: `{len(mi)}`",
        f"- Match forte por nome/stem: `{strong}`",
        f"- Pista semantica sem match de nome: `{weak}`",
        f"- Nao encontrados no MinIO consultado: `{missing}`",
        f"- Objetos MinIO sem match no SharePoint: `{len(only)}`",
        "",
        "## Como interpretar",
        "",
        "- `existe_nome_exato`: arquivo do SharePoint aparece no MinIO com o mesmo nome.",
        "- `existe_mesmo_stem_extensao_diferente`: parece o mesmo artefato convertido, por exemplo CSV/TXT no SharePoint e Parquet no MinIO.",
        "- `pista_semantica_mesma_frente_periodo_fonte`: existe algo no MinIO com mesma frente/periodo/fonte, mas o nome nao comprova equivalencia.",
        "- `nao_encontrado_no_minio_consultado`: o SharePoint complementa o MinIO nos prefixos consultados, ou o arquivo foi renomeado fora das regras de match.",
        "",
        "## Resultado por frente",
        "",
        "| Frente | Match | Quantidade |",
        "|---|---|---:|",
    ]
    for _, row in summary.iterrows():
        lines.append(f"| {row['sharepoint_frente_primaria']} | {row['minio_match_type']} | {row['quantidade']} |")
    lines.extend(["", "## MinIO por prefixo/frente", "", "| Prefixo | Frente | Fonte | Extensao | Quantidade |", "|---|---|---|---|---:|"])
    for _, row in mi_summary.head(60).iterrows():
        lines.append(
            f"| {row['top_prefix']} | {row['frente_primaria']} | {row['fonte_inferida']} | {row['extension']} | {row['quantidade']} |"
        )
    lines.extend(
        [
            "",
            "## Evidencias geradas",
            "",
            "- `minio_inventory_prefixos_consultados.csv`: inventario dos objetos MinIO nos prefixos consultados.",
            "- `sharepoint_vs_minio_diff.csv`: diff arquivo a arquivo do SharePoint local contra MinIO.",
            "- `sharepoint_vs_minio_resumo_por_frente.csv`: resumo do diff por frente.",
            "- `minio_only_sem_match_sharepoint.csv`: objetos no MinIO sem nome/stem equivalente no SharePoint local.",
            "- `minio_resumo_por_prefixo_frente.csv`: resumo do MinIO por prefixo/frente/fonte/extensao.",
        ]
    )
    (out_dir / "sharepoint_vs_minio_diff.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default="local.env")
    parser.add_argument(
        "--sharepoint-csv",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-mcmv-local/sharepoint_local_candidatos_mcmv.csv",
    )
    parser.add_argument("--prefix", action="append", default=None)
    parser.add_argument(
        "--output-dir",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-minio-diff",
    )
    args = parser.parse_args()

    client, bucket = client_from_env(Path(args.env_file))
    sp = prepare_sharepoint(Path(args.sharepoint_csv))
    prefixes = args.prefix or ["raw/sharepoint/"]
    mi = inventory_minio(client, bucket, prefixes)
    diff = diff_sharepoint_minio(sp, mi)
    write_outputs(sp, mi, diff, Path(args.output_dir), prefixes)
    print(f"Diff salvo em: {args.output_dir}")
    print(f"SharePoint candidatos: {len(sp)}")
    print(f"MinIO objetos: {len(mi)}")
    print(diff["minio_match_type"].value_counts().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
