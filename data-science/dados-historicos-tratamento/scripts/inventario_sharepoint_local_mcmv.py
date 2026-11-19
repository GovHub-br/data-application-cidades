#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DATA_EXTENSIONS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".xlsm",
    ".ods",
    ".parquet",
    ".json",
    ".txt",
    ".zip",
    ".pbix",
}

DOCUMENT_EXTENSIONS = {".pdf", ".docx", ".doc", ".pptx", ".ppt", ".md"}

FRONT_PATTERNS = {
    "mcmv_geral": [r"(?<![A-Za-z0-9])mcmv(?![A-Za-z0-9])", r"minha\s+casa\s+minha\s+vida"],
    "ogu_subsidiado": [r"(?<![A-Za-z0-9])ogu(?![A-Za-z0-9])"],
    "fgts_financiado": [
        r"(?<![A-Za-z0-9])fgts(?![A-Za-z0-9])",
        r"(?<![A-Za-z0-9])geavo(?![A-Za-z0-9])",
        r"(?<![A-Za-z0-9])sbpe(?![A-Za-z0-9])",
    ],
    "fgts_entregas": [
        r"(?<![A-Za-z0-9])fgts(?![A-Za-z0-9]).*entregas?",
        r"entregas?.*(?<![A-Za-z0-9])fgts(?![A-Za-z0-9])",
    ],
    "far": [r"(?<![A-Za-z0-9])far(?![A-Za-z0-9])", r"arrendamento"],
    "rural": [r"(?<![A-Za-z0-9])rural(?![A-Za-z0-9])", r"(?<![A-Za-z0-9])pnhr(?![A-Za-z0-9])"],
    "entidades": [r"entidades?", r"(?<![A-Za-z0-9])fds(?![A-Za-z0-9])"],
    "sub50_fnhis": [r"sub\s*50", r"sub50", r"fnhis"],
    "pro_moradia": [r"pro[\s_-]*moradia", r"promoradia"],
    "reforma": [r"reforma\s+casa\s+brasil", r"\brcb\b"],
    "cidades": [r"\bcidades\b", r"periferia"],
    "conjuntura": [r"conjuntura", r"ipea", r"ibge", r"caged", r"fgv", r"fipe"],
    "casa_civil": [r"casa\s+civil"],
    "metadados": [r"metadad[oa]s?"],
}

FRONT_PRIORITY = [
    "far",
    "entidades",
    "rural",
    "sub50_fnhis",
    "pro_moradia",
    "reforma",
    "cidades",
    "fgts_entregas",
    "fgts_financiado",
    "ogu_subsidiado",
    "conjuntura",
    "metadados",
    "mcmv_geral",
    "casa_civil",
    "nao_classificado",
]


def classify_text(text: str) -> str:
    hits = []
    for label, patterns in FRONT_PATTERNS.items():
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            hits.append(label)
    return ";".join(hits) if hits else "nao_classificado"


def primary_front(labels: str) -> str:
    label_set = set(str(labels).split(";"))
    for label in FRONT_PRIORITY:
        if label in label_set:
            return label
    return "nao_classificado"


def file_category(ext: str) -> str:
    ext = ext.lower()
    if ext == ".zip":
        return "arquivo_compactado"
    if ext in {".csv", ".xlsx", ".xls", ".xlsm", ".ods", ".parquet", ".json", ".txt"}:
        return "dado_tabular_ou_serializado"
    if ext == ".pbix":
        return "dashboard_power_bi"
    if ext in DOCUMENT_EXTENSIONS:
        return "documento"
    return "outro"


def extract_period(text: str) -> str:
    parts = [
        part
        for part in re.split(r"[\\/]", text)
        if not re.match(r"^OneDrive(?:[_\s-]|\b)", Path(part).name, flags=re.IGNORECASE)
    ]
    text = "/".join(parts)
    patterns = [
        r"(20\d{2})\s*[_\-]\s*([01]?\d)",
        r"(20\d{2})([01]\d)(?:[0-3]\d)?",
        r"(20\d{2})[_\-\s]+(?:0?)([1-9])(?:\D|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            year, month = match.group(1), match.group(2).zfill(2)
            if "01" <= month <= "12":
                return f"{year}-{month}"
    return ""


def infer_source(text: str) -> str:
    lower = text.lower()
    labels = []
    if "ogu" in lower:
        labels.append("ogu")
    if "fgts" in lower:
        labels.append("fgts")
    if "entrega" in lower:
        labels.append("entregas")
    if "metadad" in lower:
        labels.append("metadados")
    if "casa civil" in lower:
        labels.append("casa_civil")
    return ";".join(labels) if labels else "nao_identificada"


def mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def zip_dt_iso(info: zipfile.ZipInfo) -> str:
    try:
        return datetime(*info.date_time, tzinfo=timezone.utc).isoformat()
    except Exception:
        return ""


def excel_summary_from_bytes(data: bytes) -> str:
    try:
        book = pd.ExcelFile(io.BytesIO(data))
    except Exception as exc:
        return f"erro_excel:{str(exc)[:120]}"
    return "; ".join(book.sheet_names[:20])


def excel_summary_from_path(path: Path) -> str:
    try:
        book = pd.ExcelFile(path)
    except Exception as exc:
        return f"erro_excel:{str(exc)[:120]}"
    return "; ".join(book.sheet_names[:20])


def filesystem_rows(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        text = f"{rel} {path.name}"
        fronts = classify_text(text)
        if path.is_dir():
            rows.append(
                {
                    "escopo": "filesystem",
                    "tipo": "pasta",
                    "arquivo_origem": "",
                    "caminho_relativo": rel,
                    "nome": path.name,
                    "extensao": "",
                    "categoria": "pasta",
                    "tamanho_bytes": "",
                    "modificado_em": mtime_iso(path),
                    "periodo_inferido": extract_period(text),
                    "fonte_inferida": infer_source(text),
                    "frente_inferida": fronts,
                    "frente_primaria": primary_front(fronts),
                    "detalhe_estrutura": "",
                    "erro": "",
                }
            )
            continue
        ext = path.suffix.lower()
        detail = ""
        if ext in {".xlsx", ".xlsm", ".xls"}:
            detail = excel_summary_from_path(path)
        rows.append(
            {
                "escopo": "filesystem",
                "tipo": "arquivo",
                "arquivo_origem": "",
                "caminho_relativo": rel,
                "nome": path.name,
                "extensao": ext,
                "categoria": file_category(ext),
                "tamanho_bytes": path.stat().st_size,
                "modificado_em": mtime_iso(path),
                "periodo_inferido": extract_period(text),
                "fonte_inferida": infer_source(text),
                "frente_inferida": fronts,
                "frente_primaria": primary_front(fronts),
                "detalhe_estrutura": detail,
                "erro": "",
            }
        )
    return rows


def zip_rows(root: Path) -> list[dict[str, Any]]:
    rows = []
    for zip_path in sorted(root.rglob("*.zip")):
        rel_zip = zip_path.relative_to(root).as_posix()
        try:
            with zipfile.ZipFile(zip_path) as archive:
                for info in archive.infolist():
                    rel = info.filename
                    name = Path(rel).name
                    ext = Path(name).suffix.lower()
                    text = f"{rel_zip} {rel} {name}"
                    fronts = classify_text(text)
                    detail = ""
                    if not info.is_dir() and ext in {".xlsx", ".xlsm", ".xls"}:
                        try:
                            detail = excel_summary_from_bytes(archive.read(info))
                        except Exception as exc:
                            detail = f"erro_excel_zip:{str(exc)[:120]}"
                    rows.append(
                        {
                            "escopo": "zip",
                            "tipo": "pasta" if info.is_dir() else "arquivo",
                            "arquivo_origem": rel_zip,
                            "caminho_relativo": rel,
                            "nome": name,
                            "extensao": "" if info.is_dir() else ext,
                            "categoria": "pasta" if info.is_dir() else file_category(ext),
                            "tamanho_bytes": "" if info.is_dir() else info.file_size,
                            "modificado_em": zip_dt_iso(info),
                            "periodo_inferido": extract_period(text),
                            "fonte_inferida": infer_source(text),
                            "frente_inferida": fronts,
                            "frente_primaria": primary_front(fronts),
                            "detalhe_estrutura": detail,
                            "erro": "",
                        }
                    )
        except Exception as exc:
            rows.append(
                {
                    "escopo": "zip",
                    "tipo": "erro_zip",
                    "arquivo_origem": rel_zip,
                    "caminho_relativo": "",
                    "nome": zip_path.name,
                    "extensao": ".zip",
                    "categoria": "arquivo_compactado",
                    "tamanho_bytes": zip_path.stat().st_size,
                    "modificado_em": mtime_iso(zip_path),
                    "periodo_inferido": extract_period(rel_zip),
                    "fonte_inferida": infer_source(rel_zip),
                    "frente_inferida": classify_text(rel_zip),
                    "frente_primaria": primary_front(classify_text(rel_zip)),
                    "detalhe_estrutura": "",
                    "erro": str(exc)[:500],
                }
            )
    return rows


def explode_fronts(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    candidates = df[df["tipo"].eq("arquivo") & df["categoria"].isin(
        ["dado_tabular_ou_serializado", "dashboard_power_bi", "documento", "arquivo_compactado"]
    )]
    for _, row in candidates.iterrows():
        for front in str(row["frente_inferida"]).split(";"):
            rows.append({**row.to_dict(), "frente": front})
    return pd.DataFrame(rows)


def write_outputs(df: pd.DataFrame, out_dir: Path, root: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_df = df[df["escopo"].eq("zip")].copy()
    files_df = df[df["escopo"].eq("filesystem")].copy()
    candidates = df[df["tipo"].eq("arquivo") & df["categoria"].ne("outro")].copy()
    fronts = explode_fronts(df)

    files_df.to_csv(out_dir / "sharepoint_local_arquivos.csv", index=False)
    zip_df.to_csv(out_dir / "sharepoint_local_zip_conteudo.csv", index=False)
    candidates.to_csv(out_dir / "sharepoint_local_candidatos_mcmv.csv", index=False)

    by_front = (
        fronts.groupby(["frente", "fonte_inferida", "categoria"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["quantidade", "frente"], ascending=[False, True])
    )
    by_primary = (
        candidates.groupby(["frente_primaria", "fonte_inferida", "categoria"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["quantidade", "frente_primaria"], ascending=[False, True])
    )
    by_ext = (
        candidates.assign(extensao=candidates["extensao"].replace("", "sem_extensao"))
        .groupby(["extensao", "categoria"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["quantidade", "extensao"], ascending=[False, True])
    )
    by_period = (
        candidates[candidates["periodo_inferido"].ne("")]
        .groupby(["periodo_inferido", "fonte_inferida"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["periodo_inferido", "fonte_inferida"])
    )
    by_package = (
        candidates.assign(pacote=candidates["arquivo_origem"].fillna("").replace("", "filesystem"))
        .groupby(["pacote", "frente_primaria", "fonte_inferida"], dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(["quantidade", "pacote"], ascending=[False, True])
    )
    by_primary.to_csv(out_dir / "sharepoint_local_resumo_por_frente_primaria.csv", index=False)
    by_front.to_csv(out_dir / "sharepoint_local_resumo_por_frente.csv", index=False)
    by_ext.to_csv(out_dir / "sharepoint_local_resumo_por_extensao.csv", index=False)
    by_period.to_csv(out_dir / "sharepoint_local_resumo_por_periodo.csv", index=False)
    by_package.to_csv(out_dir / "sharepoint_local_resumo_por_pacote.csv", index=False)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    md = [
        "# Inventario local SharePoint - MCMV",
        "",
        f"- Gerado em: `{now}`",
        f"- Pasta inventariada: `{root}`",
        f"- Itens no filesystem: `{len(files_df)}`",
        f"- Itens dentro de ZIPs: `{len(zip_df)}`",
        f"- Candidatos a dado/documento/dashboard: `{len(candidates)}`",
        "",
        "## Leitura executiva",
        "",
        "- O pacote local baixado combina bases Casa Civil do MCMV OGU/FGTS, pacotes de dados do Novo MCMV e materiais de catalogo/metadados.",
        "- Os nomes indicam bases mensais de contratacoes/dados OGU, contratacoes/dados FGTS, entregas FGTS e arquivos especificos de FAR/Entidades/Rural.",
        "- Ha fichas de metadados em XLSX para OGU, FGTS e entregas FGTS, que devem orientar o mapeamento semantico antes de transformar em silver.",
        "- A inferencia por nome/caminho identifica a fonte macro, mas nao substitui leitura de colunas para separar FAR, Entidades, Rural, SUB50/FNHIS e demais modalidades quando elas estiverem dentro de arquivos OGU.",
        "",
        "## Resumo por frente primaria",
        "",
        "| Frente primaria | Fonte | Categoria | Quantidade |",
        "|---|---|---|---:|",
    ]
    for _, row in by_primary.head(40).iterrows():
        md.append(f"| {row['frente_primaria']} | {row['fonte_inferida']} | {row['categoria']} | {row['quantidade']} |")
    md.extend(
        [
            "",
            "## Resumo por todas as tags de frente/fonte inferidas",
        "",
        "| Frente | Fonte | Categoria | Quantidade |",
        "|---|---|---|---:|",
        ]
    )
    for _, row in by_front.head(40).iterrows():
        md.append(f"| {row['frente']} | {row['fonte_inferida']} | {row['categoria']} | {row['quantidade']} |")
    md.extend(["", "## Resumo por extensao", "", "| Extensao | Categoria | Quantidade |", "|---|---|---:|"])
    for _, row in by_ext.iterrows():
        md.append(f"| {row['extensao']} | {row['categoria']} | {row['quantidade']} |")
    md.extend(["", "## Resumo por periodo/fonte", "", "| Periodo | Fonte | Quantidade |", "|---|---|---:|"])
    for _, row in by_period.iterrows():
        md.append(f"| {row['periodo_inferido']} | {row['fonte_inferida']} | {row['quantidade']} |")
    md.extend(["", "## Principais pacotes", "", "| Pacote | Frente primaria | Fonte | Quantidade |", "|---|---|---|---:|"])
    for _, row in by_package.head(30).iterrows():
        md.append(f"| {row['pacote']} | {row['frente_primaria']} | {row['fonte_inferida']} | {row['quantidade']} |")
    md.extend(
        [
            "",
            "## Evidencias CSV",
            "",
            "- `sharepoint_local_arquivos.csv`: arquivos e pastas locais baixados.",
            "- `sharepoint_local_zip_conteudo.csv`: conteudo interno dos arquivos ZIP.",
            "- `sharepoint_local_candidatos_mcmv.csv`: arquivos com extensoes relevantes para dados, dashboards ou documentacao.",
            "- `sharepoint_local_resumo_por_frente_primaria.csv`: contagem pela frente mais especifica inferida.",
            "- `sharepoint_local_resumo_por_frente.csv`: contagem por frente/fonte/categoria inferida.",
            "- `sharepoint_local_resumo_por_extensao.csv`: contagem por extensao.",
            "- `sharepoint_local_resumo_por_periodo.csv`: contagem por periodo mensal inferido.",
            "- `sharepoint_local_resumo_por_pacote.csv`: contagem por ZIP/pacote de origem.",
            "",
            "## Proximo uso na arquitetura",
            "",
            "Para respeitar a arquitetura definida, estes arquivos devem ser tratados como fonte de inventario/descoberta. A carga silver produtiva deve consumir apenas arquivos publicados na camada `staging/` do MinIO via DuckDB/dbt.",
        ]
    )
    (out_dir / "sharepoint_local_inventario_mcmv.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/home/juan-pablo/CIDADES/sharepoint")
    parser.add_argument(
        "--output-dir",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-mcmv-local",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Pasta nao encontrada: {root}")
    out_dir = Path(args.output_dir)
    df = pd.DataFrame(filesystem_rows(root) + zip_rows(root))
    write_outputs(df, out_dir, root)
    print(f"Inventario salvo em: {out_dir}")
    print(f"Itens inventariados: {len(df)}")
    print(f"Arquivos candidatos: {len(df[df['tipo'].eq('arquivo') & df['categoria'].ne('outro')])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
