#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path("data-science/dados-historicos-tratamento/docs/evidencias")
OUT = ROOT / "relatorios-fontes-mcmv"


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path).fillna("") if path.exists() else pd.DataFrame()


def md_table(df: pd.DataFrame, cols: list[str], headers: list[str] | None = None, limit: int = 30) -> list[str]:
    if df.empty:
        return ["_Sem registros._"]
    view = df.loc[:, cols].head(limit).astype(str)
    headers = headers or cols
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "/") for col in cols) + " |")
    return lines


def front_periods(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["periodo_inferido"].astype(str).str.len().gt(0)].copy()
    if d.empty:
        return pd.DataFrame()
    return (
        d.groupby("frente_primaria")["periodo_inferido"]
        .agg(["min", "max", "count"])
        .reset_index()
        .rename(columns={"frente_primaria": "frente", "min": "periodo_min", "max": "periodo_max", "count": "arquivos_com_periodo"})
        .sort_values("frente")
    )


def examples(df: pd.DataFrame, front: str, col: str = "object_name", limit: int = 12) -> list[str]:
    if df.empty or col not in df.columns:
        return []
    sub = df[df["frente_primaria"].astype(str).eq(front)]
    return [f"- `{value}`" for value in sub[col].drop_duplicates().head(limit).astype(str)]


def write(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def report_sharepoint(sp: pd.DataFrame, files: pd.DataFrame, zips: pd.DataFrame) -> None:
    counts = sp.groupby(["frente_primaria", "categoria"], dropna=False).size().reset_index(name="qtd")
    exts = sp.groupby("extensao", dropna=False).size().reset_index(name="qtd").sort_values("qtd", ascending=False)
    packages = sp.groupby("arquivo_origem", dropna=False).size().reset_index(name="qtd").sort_values("qtd", ascending=False)
    lines = [
        "# Relatorio 01 - Inventario SharePoint MCMV",
        "",
        f"- Gerado em: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- Pasta local inventariada: `/home/juan-pablo/CIDADES/sharepoint`",
        f"- Arquivos/pastas no filesystem: `{len(files)}`",
        f"- Itens encontrados dentro de ZIPs: `{len(zips)}`",
        f"- Candidatos MCMV inventariados: `{len(sp)}`",
        "",
        "## Resumo por frente",
        "",
        *md_table(counts.sort_values(["frente_primaria", "qtd"], ascending=[True, False]), ["frente_primaria", "categoria", "qtd"], ["Frente", "Categoria", "Qtd"]),
        "",
        "## Janela temporal inferida",
        "",
        *md_table(front_periods(sp), ["frente", "periodo_min", "periodo_max", "arquivos_com_periodo"], ["Frente", "Inicio", "Fim", "Arquivos"]),
        "",
        "## Extensoes principais",
        "",
        *md_table(exts, ["extensao", "qtd"], ["Extensao", "Qtd"], limit=20),
        "",
        "## Pacotes mais relevantes",
        "",
        *md_table(packages[packages["arquivo_origem"].astype(str).str.len().gt(0)], ["arquivo_origem", "qtd"], ["Pacote", "Itens"], limit=15),
        "",
        "## Conclusao",
        "",
        "- O SharePoint local contem massa forte para FAR, Entidades/FDS, Rural, FGTS financiado, OGU subsidiado e entregas FGTS.",
        "- Nao apareceram pacotes explicitos e volumosos de Pro-Moradia ou Reforma Casa Brasil no acervo local inventariado.",
        "- O acervo local e util como trilha de auditoria e fonte complementar, mas a producao deve consumir o que foi publicado no MinIO em `raw/sharepoint/`.",
    ]
    write(OUT / "01-inventario-sharepoint.md", lines)


def report_minio(mi_raw: pd.DataFrame, mi_sp: pd.DataFrame, diff_raw: pd.DataFrame, diff_sp: pd.DataFrame) -> None:
    raw = mi_raw.copy()
    raw["segundo_prefixo"] = raw["object_name"].astype(str).str.split("/").str[1]
    by_area = raw.groupby(["segundo_prefixo", "frente_primaria"], dropna=False).size().reset_index(name="qtd").sort_values("qtd", ascending=False)
    by_sp = mi_sp.groupby(["frente_primaria", "extension"], dropna=False).size().reset_index(name="qtd").sort_values("qtd", ascending=False)
    match_raw = diff_raw["minio_match_type"].value_counts().reset_index()
    match_raw.columns = ["match", "qtd"]
    match_sp = diff_sp["minio_match_type"].value_counts().reset_index()
    match_sp.columns = ["match", "qtd"]
    lines = [
        "# Relatorio 02 - MinIO raw e reorganizacao SFTP/SharePoint",
        "",
        f"- Gerado em: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        "- Bucket analisado: `data-lake-mcid`.",
        "- Prefixos relidos nesta rodada: `raw/` e `raw/sharepoint/`.",
        f"- Objetos em `raw/`: `{len(mi_raw)}`",
        f"- Objetos em `raw/sharepoint/`: `{len(mi_sp)}`",
        "",
        "## Distribuicao no raw",
        "",
        *md_table(by_area, ["segundo_prefixo", "frente_primaria", "qtd"], ["Area", "Frente", "Qtd"], limit=40),
        "",
        "## Distribuicao em raw/sharepoint",
        "",
        *md_table(by_sp, ["frente_primaria", "extension", "qtd"], ["Frente", "Extensao", "Qtd"], limit=30),
        "",
        "## Diff SharePoint local x MinIO raw",
        "",
        *md_table(match_raw, ["match", "qtd"], ["Tipo de match", "Qtd"]),
        "",
        "## Diff SharePoint local x MinIO raw/sharepoint",
        "",
        *md_table(match_sp, ["match", "qtd"], ["Tipo de match", "Qtd"]),
        "",
        "## Leitura",
        "",
        "- Nao ha indicio de perda dos dados SFTP: `raw/sftp/` segue presente e concentra a maior massa por FAR, Rural, Entidades e FGTS.",
        "- O que mudou foi organizacao/publicacao: parte do acervo SharePoint aparece como CSVs consolidados em `raw/sharepoint/`.",
        "- O diff por nome exato entre SharePoint local e `raw/sharepoint/` nao bate porque os objetos do MinIO foram renomeados para nomes canonicos, como `novo_mcmv_far_consolidado.csv`.",
        "- Para dbt/silver, o caminho pratico e consumir `raw/sharepoint/` e manter `raw/sftp/` como fonte bruta historica/auditavel.",
    ]
    write(OUT / "02-minio-sftp-reorganizacao.md", lines)


def report_superset(sources: pd.DataFrame, datasets: pd.DataFrame, profiles: pd.DataFrame, charts: pd.DataFrame, hits: pd.DataFrame) -> None:
    chart_counts = charts.groupby(["dashboard_title", "dashboard_front"], dropna=False).size().reset_index(name="charts")
    merged = datasets.merge(profiles[["dataset_id", "row_count_postgres"]], on="dataset_id", how="left")
    use = charts.groupby(["dashboard_title", "dashboard_front", "dataset_id"], dropna=False).size().reset_index(name="charts_usando")
    use = use.merge(merged, on="dataset_id", how="left")
    mismatches = use[use["dashboard_front"].ne(use["dataset_front"])].copy()
    content = hits.groupby(["schema", "table_name"], dropna=False)["sharepoint_content_match"].any().reset_index(name="match_conteudo_sharepoint")
    lines = [
        "# Relatorio 03 - Superset FAR/RURAL/Entidades x MinIO/SharePoint",
        "",
        f"- Gerado em: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- Dashboards: `{charts['dashboard_id'].nunique()}`",
        f"- Charts: `{len(charts)}`",
        f"- Datasets distintos: `{len(datasets)}`",
        f"- Datasets com fonte candidata no SharePoint local: `{int(sources['sharepoint_tem_frente'].sum())}/{len(sources)}`",
        f"- Datasets com fonte candidata no MinIO raw: `{int(sources['minio_bucket_tem_frente'].sum())}/{len(sources)}`",
        f"- Datasets com fonte candidata no MinIO `raw/sharepoint/`: `{int(sources['minio_raw_sharepoint_tem_frente'].sum())}/{len(sources)}`",
        f"- Datasets com valor real amostrado encontrado no SharePoint: `{int(content['match_conteudo_sharepoint'].sum())}/{len(content)}`",
        "",
        "## Charts por dashboard",
        "",
        *md_table(chart_counts, ["dashboard_title", "dashboard_front", "charts"], ["Dashboard", "Frente", "Charts"]),
        "",
        "## Datasets que alimentam os dashboards",
        "",
        *md_table(use.sort_values(["dashboard_title", "dataset_id"]), ["dashboard_title", "schema", "table_name", "dataset_front", "charts_usando", "row_count_postgres"], ["Dashboard", "Schema", "Tabela", "Frente dataset", "Charts", "Linhas"], limit=20),
        "",
        "## Fonte por dataset",
        "",
        *md_table(sources, ["schema", "table_name", "sharepoint_tem_frente", "minio_bucket_tem_frente", "minio_raw_sharepoint_tem_frente"], ["Schema", "Tabela", "SharePoint", "MinIO raw", "MinIO raw/sharepoint"], limit=20),
        "",
        "## Alerta encontrado",
        "",
    ]
    if mismatches.empty:
        lines.append("- Nenhum chart usa dataset de outra frente.")
    else:
        lines.extend(md_table(mismatches, ["dashboard_title", "schema", "table_name", "dashboard_front", "dataset_front", "charts_usando"], ["Dashboard", "Schema", "Tabela", "Frente dashboard", "Frente dataset", "Charts"]))
    lines.extend([
        "",
        "## Conclusao",
        "",
        "- Os dados que alimentam os dashboards FAR, RURAL e Entidades existem no PostgreSQL e possuem fonte candidata no SharePoint e no MinIO.",
        "- O match direto por nome nao deve ser usado como unico criterio, porque o Superset consome tabelas analiticas/agregadas e as fontes tem nomes operacionais.",
        "- A correcao imediata e revisar o chart `Estado` de Entidades, que hoje aponta para `empreendimento_far.panorama_estadual`.",
    ])
    write(OUT / "03-superset-far-rural-entidades-fontes.md", lines)


def report_new_data(sp: pd.DataFrame, mi_sp: pd.DataFrame) -> None:
    fronts = sp.groupby("frente_primaria", dropna=False).size().reset_index(name="qtd_sharepoint_local").sort_values("qtd_sharepoint_local", ascending=False)
    mi_fronts = mi_sp.groupby("frente_primaria", dropna=False).size().reset_index(name="qtd_minio_raw_sharepoint")
    fronts = fronts.merge(mi_fronts, on="frente_primaria", how="left").fillna(0)
    lines = [
        "# Relatorio 04 - Dados novos uteis do SharePoint para relogio e alertas",
        "",
        f"- Gerado em: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        "- Foco: selecionar dados que ajudam metas de UH, desempenho, gargalos e alertas preditivos.",
        "",
        "## Cobertura por frente",
        "",
        *md_table(fronts, ["frente_primaria", "qtd_sharepoint_local", "qtd_minio_raw_sharepoint"], ["Frente", "SharePoint local", "MinIO raw/sharepoint"], limit=30),
        "",
        "## Arquivos uteis ja publicados em raw/sharepoint",
        "",
        "### FAR",
        *examples(mi_sp, "far"),
        "",
        "### Rural",
        *examples(mi_sp, "rural"),
        "",
        "### Entidades/FDS",
        *examples(mi_sp, "entidades"),
        "",
        "### FGTS financiado",
        *examples(mi_sp, "fgts_financiado", limit=20),
        "",
        "### OGU/Sub50/Conjuntura",
        *examples(mi_sp, "ogu_subsidiado"),
        *examples(mi_sp, "sub50_fnhis"),
        *examples(mi_sp, "conjuntura"),
        "",
        "## Uso recomendado",
        "",
        "- Relogio de meta: usar bases de contratacao/consolidado por frente, com UH, empreendimento, periodo e status.",
        "- Alertas de atraso: usar bases de obra, previsao/conclusao, percentual fisico e status simplificado.",
        "- Alertas financeiros: usar bases de financeiro/desembolso, liberado x desembolsado e execucao financeira.",
        "- Gargalos territoriais: usar municipio, UF, geoespacial, IBGE e regioes.",
        "- Responsaveis/atores: usar entidades, construtoras, agente financeiro, tomador/proponente quando disponivel.",
        "",
        "## Lacunas",
        "",
        "- Pro-Moradia e Reforma Casa Brasil nao apareceram como frentes robustas no SharePoint local/`raw/sharepoint/` inventariado.",
        "- SUB50/FNHIS apareceu com propostas apresentadas e selecionadas, mas ainda precisa mart/silver propria para entrar no relogio.",
    ]
    write(OUT / "04-dados-novos-sharepoint-uteis.md", lines)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    sp = read_csv(ROOT / "sharepoint-mcmv-local/sharepoint_local_candidatos_mcmv.csv")
    files = read_csv(ROOT / "sharepoint-mcmv-local/sharepoint_local_arquivos.csv")
    zips = read_csv(ROOT / "sharepoint-mcmv-local/sharepoint_local_zip_conteudo.csv")
    mi_raw = read_csv(ROOT / "sharepoint-minio-diff-raw/minio_inventory_prefixos_consultados.csv")
    mi_sp = read_csv(ROOT / "sharepoint-minio-diff-raw-sharepoint/minio_inventory_prefixos_consultados.csv")
    diff_raw = read_csv(ROOT / "sharepoint-minio-diff-raw/sharepoint_vs_minio_diff.csv")
    diff_sp = read_csv(ROOT / "sharepoint-minio-diff-raw-sharepoint/sharepoint_vs_minio_diff.csv")
    sources = read_csv(ROOT / "superset-mcmv/superset_dashboard_dataset_sources.csv")
    datasets = read_csv(ROOT / "superset-mcmv/superset_dashboard_datasets_alvo.csv")
    profiles = read_csv(ROOT / "superset-mcmv/superset_dashboard_table_profiles.csv")
    charts = read_csv(ROOT / "superset-mcmv/superset_dashboard_charts_alvo.csv")
    hits = read_csv(ROOT / "superset-mcmv/superset_dashboard_sample_sharepoint_hits.csv")

    report_sharepoint(sp, files, zips)
    report_minio(mi_raw, mi_sp, diff_raw, diff_sp)
    report_superset(sources, datasets, profiles, charts, hits)
    report_new_data(sp, mi_sp)
    print(f"Relatorios salvos em: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
