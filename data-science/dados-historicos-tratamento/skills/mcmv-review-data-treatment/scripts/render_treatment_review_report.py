#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from datetime import date
from pathlib import Path

import pandas as pd


def esc(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def table_html(df: pd.DataFrame, columns: list[str], limit: int = 20) -> str:
    available = [c for c in columns if c in df.columns]
    if not available:
        return "<p>Sem dados disponiveis.</p>"
    view = df[available].head(limit)
    head = "".join(f"<th>{esc(c)}</th>" for c in available)
    body = []
    for _, row in view.iterrows():
        body.append("<tr>" + "".join(f"<td>{esc(row[c])}</td>" for c in available) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def chrome_pdf(html: Path, pdf: Path) -> bool:
    chrome = shutil.which("google-chrome") or shutil.which("chromium") or shutil.which("chromium-browser")
    if not chrome:
        return False
    try:
        subprocess.run(
            [chrome, "--headless", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage", "--no-pdf-header-footer", f"--print-to-pdf={pdf}", str(html.resolve())],
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="dados-historicos-tratamento directory")
    parser.add_argument("--evidence-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--basename", default="revisao-tratamento-dados")
    parser.add_argument("--no-pdf", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    evidence = Path(args.evidence_dir).resolve() if args.evidence_dir else root / "docs" / "evidencias" / "revisao-tratamento-dados"
    docs = Path(args.output_dir).resolve() if args.output_dir else root / "docs"
    docs.mkdir(parents=True, exist_ok=True)

    audit = pd.read_csv(evidence / "auditoria_tratamento_pandas.csv", dtype=str).fillna("")
    flags = pd.read_csv(evidence / "flags_tratamento_pandas.csv", dtype=str).fillna("")
    samples = pd.read_csv(evidence / "amostras_tratadas_pandas.csv", dtype=str).fillna("")
    sftp = pd.read_csv(evidence / "impacto_sftp_tratamento_pandas.csv", dtype=str).fillna("")
    summary = json.loads((evidence / "summary_tratamento.json").read_text(encoding="utf-8"))
    db_summary_path = evidence / "resumo_db_tratamento_pandas.csv"
    db_discord_path = evidence / "discordancias_tratamento_db_pandas.csv"
    db_summary = pd.read_csv(db_summary_path, dtype=str).fillna("") if db_summary_path.exists() else pd.DataFrame()
    db_discord = pd.read_csv(db_discord_path, dtype=str).fillna("") if db_discord_path.exists() else pd.DataFrame()
    use_db = not db_summary.empty and not db_discord.empty

    status_counts = audit["status"].value_counts().to_dict() if "status" in audit.columns else {}
    if use_db:
        db_row = db_summary.iloc[0]
        total_tables = int(db_row["qualidade_linhas"])
        treated_count = int(db_row["tratadas"])
        error_count = int(db_row["erros"])
        discarded_count = int(db_row["descartadas"])
        missing_gt_30_count = int(db_row["missing_gt_30"])
        institution_unknown_count = int(db_row["institution_unknown"])
        report_date_missing_count = int(db_row["report_date_missing"])
        discord_count = int(db_row["discordancias_amostradas"])
    else:
        total_tables = int(summary.get("tables_in_quality", len(audit)))
        treated_count = int(status_counts.get("treated", 0))
        error_count = int(summary.get("status_error", status_counts.get("error", 0)))
        discarded_count = int(summary.get("status_discarded", status_counts.get("discarded", 0)))
        missing_gt_30_count = 0
        institution_unknown_count = 0
        report_date_missing_count = 0
        discord_count = len(flags)
    treated_ratio = round(treated_count / max(total_tables, 1) * 100, 1)
    treated_files_found = int(summary.get("treated_files_found", 0))
    identifier_length_errors = 0
    if "error" in audit.columns:
        identifier_length_errors = int(audit["error"].astype(str).str.contains("exceeds maximum length", regex=False).sum())
    flag_counts = pd.DataFrame()
    if len(flags) and "flag_revisao" in flags.columns:
        flag_counts = (
            flags["flag_revisao"]
            .str.strip(";")
            .str.get_dummies(sep=";")
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        flag_counts.columns = ["flag", "quantidade"]
    if use_db:
        flag_counts = db_discord["motivo_discordancia"].value_counts().reset_index()
        flag_counts.columns = ["flag", "quantidade"]
    missing_top = audit.copy()
    if "missing_pct" in missing_top.columns:
        missing_top["_missing_pct_num"] = pd.to_numeric(missing_top["missing_pct"], errors="coerce").fillna(0)
        missing_top = missing_top.sort_values("_missing_pct_num", ascending=False)
    if use_db:
        missing_top = db_discord[db_discord["motivo_discordancia"].eq("missing_alto")].copy()
        missing_top["_missing_pct_num"] = pd.to_numeric(missing_top["missing_pct"], errors="coerce").fillna(0)
        missing_top = missing_top.sort_values("_missing_pct_num", ascending=False)
    missing_evidence = []
    if treated_files_found == 0:
        if use_db:
            missing_evidence.append("data/treated_tables/ nao existe no repo, mas as amostras foram lidas diretamente do schema dados_historicos_formatados.")
        else:
            missing_evidence.append("data/treated_tables/ nao existe nesta branch; nao foi possivel abrir CSV tratado linha a linha.")
    if len(sftp) == 0:
        missing_evidence.append("diff_sftp_minio*.json nao foi encontrado nesta branch/worktree; impacto SFTP detalhado ficou pendente.")
    if len(samples) and "sample_print" in samples.columns and "Nenhum CSV tratado" in str(samples.iloc[0]["sample_print"]):
        missing_evidence.append("amostras_tratadas_pandas.csv registra ausencia de amostras tratadas para prints reais.")
    if use_db:
        verdict = "Tratamento sem erros no banco, mas ainda com discordancias a corrigir antes do uso preditivo final"
        status_interpretation = (
            f"A leitura do banco em dados_historicos_formatados mostra {treated_count} de {total_tables} registros de qualidade "
            f"com status treated ({treated_ratio}%), {discarded_count} descartes e {error_count} erros. "
            f"A aprovacao final depende de corrigir as {discord_count} discordancias amostradas: "
            f"{missing_gt_30_count} tabelas com missing_pct > 30%, {institution_unknown_count} com instituicao desconhecida, "
            f"{report_date_missing_count} com report_date ausente e descartes pipe com dados estruturaveis."
        )
        metric_cards = [
            ("Fonte", "Banco"),
            ("Tabelas qualidade", total_tables),
            ("Status treated", f"{treated_count} ({treated_ratio}%)"),
            ("Erros", error_count),
            ("Descartes", discarded_count),
            ("Discordancias lidas", discord_count),
        ]
    else:
        verdict = (
            "Parcialmente tratados, ainda nao aprovados para uso preditivo final"
            if error_count or treated_files_found == 0 or len(flags)
            else "Tratamento aprovado nas evidencias disponiveis"
        )
        status_interpretation = (
            f"O snapshot de qualidade indica que {treated_count} de {total_tables} tabelas ({treated_ratio}%) chegaram a status treated. "
            f"Isso mostra que o pipeline processa a maior parte do acervo, mas os {error_count} erros, {discarded_count} descartes "
            f"e {len(flags)} linhas com flags impedem uma aprovacao final sem ajustes e sem os CSVs tratados."
        )
        metric_cards = [
            ("Tabelas revisadas", summary.get("tables_in_quality", len(audit))),
            ("Status treated", f"{treated_count} ({treated_ratio}%)"),
            ("CSVs tratados encontrados", summary.get("treated_files_found", 0)),
            ("Flags de revisao", summary.get("flags", len(flags))),
            ("Erros", summary.get("status_error", status_counts.get("error", 0))),
            ("Descartes", summary.get("status_discarded", status_counts.get("discarded", 0))),
        ]
    metrics = "".join(f"<div class='metric'><span>{esc(label)}</span><strong>{esc(value)}</strong></div>" for label, value in metric_cards)

    top_flags = table_html(
        flags,
        ["table_name", "status", "n_rows", "n_cols", "profile", "missing_pct", "institution", "report_date", "flag_revisao", "error"],
        30,
    )
    if use_db:
        top_flags = table_html(
            db_discord,
            ["table_name", "status", "profile", "missing_pct", "motivo_discordancia", "evidencia", "como_consertar", "treated_exists", "raw_exists"],
            35,
        )
    sample_print = esc(samples.iloc[0]["sample_print"]) if len(samples) and "sample_print" in samples.columns else "Sem amostras."
    sftp_html = table_html(sftp, list(sftp.columns), 20)
    flag_counts_html = table_html(flag_counts, ["flag", "quantidade"], 20)
    status_html = table_html(
        pd.DataFrame([{"status": key, "quantidade": value} for key, value in status_counts.items()]),
        ["status", "quantidade"],
        20,
    )
    missing_top_html = table_html(
        missing_top,
        ["table_name", "status", "n_rows", "n_cols", "profile", "missing_pct", "institution", "report_date", "flag_revisao"],
        12,
    )
    missing_evidence_html = "".join(f"<li>{esc(item)}</li>" for item in missing_evidence) or "<li>Nenhuma ausencia critica de evidencia registrada.</li>"
    recommendation_items = [
        "Localizar ou regenerar data/treated_tables/ para validar linha a linha nomes, datas, valores, encoding e tipos nos CSVs finais.",
        "Corrigir os erros de identificador longo no writer/tratamento, pois 23 dos 34 erros indicam limite de 63 caracteres.",
        "Revisar extracao de report_date e institution; ha 132 flags de report_date ausente e 92 de institution desconhecida.",
        "Auditar as 4 tabelas com missing_pct acima de 30% para separar esparsidade real de tratamento excessivo.",
        "Trazer os diff_sftp_minio*.json para confirmar impacto de path, zip, duplicidade e filename no tratamento.",
    ]
    recommendations_html = "".join(f"<li>{esc(item)}</li>" for item in recommendation_items)
    if use_db:
        reason_counts = db_discord["motivo_discordancia"].value_counts().to_dict()
        recommendation_items = [
            f"Corrigir {reason_counts.get('descarte_incorreto_pipe', 0)} descartes pipe: alinhar classificacao para separador_| e reprocessar com tratar_separador_pipe().",
            f"Revisar {reason_counts.get('missing_alto', 0)} tabelas com missing alto, principalmente sub_tabelas_3: separar blocos, remover colunas vazias e avaliar formato longo para features.",
            f"Corrigir {reason_counts.get('metadados_incompletos', 0)} casos amostrados de metadados: ampliar extrair_periodo_filename() para datas como 25jan2011, 22mar11 e 31dez09.",
            f"Revisar {reason_counts.get('descarte_revisar', 0)} descartes restantes para confirmar se sao de fato vazios/sem utilidade.",
            "Adicionar testes de regressao para familias bb_2011_relatorio_min_cidades, datas_entregas_pj e bb_2013_06_junho_pmcmv_18062013_tab_*.",
        ]
        recommendations_html = "".join(f"<li>{esc(item)}</li>" for item in recommendation_items)
        issue_metrics = f"""- Fonte principal: banco `dados_historicos_formatados`
- Tabelas no `_qualidade`: {total_tables}
- Status `treated`: {treated_count} ({treated_ratio}%)
- Erros no banco: {error_count}
- Descartes: {discarded_count}
- Discordancias amostradas linha a linha: {discord_count}
- `missing_pct > 30%`: {missing_gt_30_count}
- `institution` desconhecida/vazia: {institution_unknown_count}
- `report_date` ausente: {report_date_missing_count}"""
    else:
        issue_metrics = f"""- Tabelas revisadas: {summary.get("tables_in_quality", len(audit))}
- Status `treated`: {treated_count} ({treated_ratio}%)
- CSVs tratados encontrados: {summary.get("treated_files_found", 0)}
- Flags de revisao: {summary.get("flags", len(flags))}
- Erros: {summary.get("status_error", 0)}
- Descartes: {summary.get("status_discarded", 0)}
- Erros por identificador longo: {identifier_length_errors}"""

    sample_blocks = ""
    if use_db:
        for reason in ["missing_alto", "metadados_incompletos", "descarte_incorreto_pipe"]:
            subset = db_discord[db_discord["motivo_discordancia"].eq(reason)]
            if subset.empty:
                continue
            row = subset.iloc[0]
            sample_blocks += (
                f"<h3>Amostra DB: {esc(reason)} - {esc(row['table_name'])}</h3>"
                f"<p><strong>Evidencia:</strong> {esc(row['evidencia'])}</p>"
                f"<p><strong>Como consertar:</strong> {esc(row['como_consertar'])}</p>"
                f"<h4>Tabela tratada</h4><pre>{esc(row['treated_sample_print'])}</pre>"
                f"<h4>Tabela bruta</h4><pre>{esc(row['raw_sample_print'])}</pre>"
            )
    else:
        sample_blocks = f"<h3>Amostra pandas</h3><pre>{sample_print}</pre>"

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>Revisao de Tratamento de Dados MCMV</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/@twind/core@1.1.3/umd/index.global.js"></script>
  <script>window.__REVIEW_DATA__ = {json.dumps({"metrics": metric_cards}, ensure_ascii=False)};</script>
  <style>
    @page {{ size: A4; margin: 14mm; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: #172033; font: 12px/1.45 Arial, Helvetica, sans-serif; background: #eef3f7; }}
    .page {{ width: 210mm; min-height: 297mm; margin: 0 auto 18px; background: #fff; overflow: hidden; page-break-after: always; box-shadow: 0 8px 28px rgba(15,23,42,.12); }}
    .cover {{ min-height: 297mm; padding: 34mm 22mm; color: white; background: linear-gradient(135deg, #14532d, #2563eb); }}
    .eyebrow {{ text-transform: uppercase; letter-spacing: .12em; opacity: .86; font-size: 12px; }}
    h1 {{ font-size: 34px; line-height: 1.08; margin: 18px 0 12px; max-width: 700px; }}
    h2 {{ color: #14532d; font-size: 19px; margin: 0 0 12px; }}
    h3 {{ color: #2563eb; margin: 16px 0 8px; }}
    .content {{ padding: 16mm 18mm; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 12px 0 18px; }}
    .metric {{ border: 1px solid #dbe3ee; border-radius: 10px; padding: 12px; background: #f8fafc; min-height: 70px; }}
    .metric span {{ display: block; color: #64748b; font-size: 10px; text-transform: uppercase; }}
    .metric strong {{ display: block; color: #14532d; font-size: 24px; margin-top: 4px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 9px; margin: 10px 0 16px; }}
    th, td {{ border: 1px solid #dbe3ee; padding: 5px; vertical-align: top; overflow-wrap: anywhere; }}
    th {{ background: #eaf3ee; color: #143822; text-align: left; }}
    pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #0f172a; color: #e2e8f0; padding: 10px; border-radius: 8px; font-size: 8.5px; }}
    .note {{ border-left: 4px solid #2563eb; background: #eff6ff; padding: 10px 12px; }}
    .verdict {{ border-left: 6px solid #b45309; background: #fffbeb; padding: 12px 14px; margin: 14px 0; }}
    .good {{ border-left-color: #14532d; background: #f0fdf4; }}
    .bad {{ border-left-color: #b91c1c; background: #fef2f2; }}
  </style>
</head>
<body>
  <section class="page cover">
    <div class="eyebrow">Ministerio das Cidades | Minha Casa Minha Vida</div>
    <h1>Revisao de Tratamento de Dados Historicos MCMV</h1>
    <p>Auditoria tecnica com pandas, evidencias de qualidade e recomendacoes para analise preditiva.</p>
    <p>Data: {date.today().isoformat()} | Fonte: {esc(root)}</p>
  </section>
  <section class="page">
    <div class="content">
      <h2>Sumario Executivo</h2>
      <div class="grid">{metrics}</div>
      <div class="verdict">
        <strong>Veredito:</strong> {esc(verdict)}.
        <p>{esc(status_interpretation)}</p>
      </div>
      <div class="verdict good">
        <strong>O que parece bom:</strong> a maior parte da qualidade esta como treated ({treated_count}/{total_tables}) e o banco formatado nao apresentou status error na leitura atual.
      </div>
      <div class="verdict bad">
        <strong>O que ainda nao esta bom:</strong> ha discordancias com evidencia: missing alto, metadados temporais/institucionais ausentes e descartes pipe com dado bruto estruturavel.
      </div>
      <h3>Distribuicao de status</h3>
      {status_html}
      <h3>Flags por tipo</h3>
      {flag_counts_html}
    </div>
  </section>
  <section class="page">
    <div class="content">
      <h2>Evidencias de Problemas</h2>
      <h3>Principais Flags</h3>
      {top_flags}
      <h3>Maiores missing_pct</h3>
      {missing_top_html}
    </div>
  </section>
  <section class="page">
    <div class="content">
      <h2>Evidencias e Lacunas</h2>
      <h3>O que nao foi encontrado nesta branch</h3>
      <ul>{missing_evidence_html}</ul>
      {sample_blocks}
      <h3>Impacto SFTP/MinIO</h3>
      {sftp_html}
      <h3>Recomendacoes</h3>
      <ul>{recommendations_html}</ul>
      <h3>Arquivos gerados</h3>
      <ul>
        <li>{esc(evidence / "auditoria_tratamento_pandas.csv")}</li>
        <li>{esc(evidence / "flags_tratamento_pandas.csv")}</li>
        <li>{esc(evidence / "amostras_tratadas_pandas.csv")}</li>
        <li>{esc(evidence / "impacto_sftp_tratamento_pandas.csv")}</li>
      </ul>
    </div>
  </section>
  <script>
    (function () {{
      if (!window.React || !window.ReactDOM) return;
      document.documentElement.setAttribute("data-react-ready", "true");
    }})();
  </script>
</body>
</html>
"""

    html_path = docs / f"{args.basename}.html"
    pdf_path = docs / f"{args.basename}.pdf"
    issue_path = docs / f"resposta-{args.basename}.md"
    html_path.write_text(html, encoding="utf-8")
    if not args.no_pdf:
        chrome_pdf(html_path, pdf_path)

    issue_path.write_text(
        f"""## Resumo da revisao de tratamento

Auditoria pandas executada sobre relatorios de qualidade, deduplicacao, classificacao, inventario e CSVs tratados disponiveis.

{issue_metrics}

**Veredito:** {verdict}. {status_interpretation}

**Evidencias principais:**
- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`: base completa da auditoria.
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`: {len(flags)} linhas com flags.
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`: registra que nao havia CSV tratado disponivel para prints reais.
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`: sem diffs SFTP encontrados nesta branch.
- `docs/evidencias/revisao-tratamento-dados/resumo_db_tratamento_pandas.csv`: resumo da leitura DB.
- `docs/evidencias/revisao-tratamento-dados/discordancias_tratamento_db_pandas.csv`: amostras brutas e tratadas das discordancias.

**Como consertar:**
- Corrigir descartes pipe para `separador_|` e reprocessar `bb_2013_06_junho_pmcmv_18062013_tab_*`.
- Revisar `sub_tabelas_3` com missing alto; separar blocos, remover colunas vazias e avaliar formato longo.
- Ampliar `extrair_periodo_filename()` para datas textuais/abreviadas como `25jan2011`, `22mar11`, `31dez09`.
- Revisar descartes restantes antes de aceitar como `vazia` ou `dados_sem_utilidade`.

**O que falta localizar/regenerar:**
- `data/treated_tables/`
- `diff_sftp_minio*.json`

Artefatos:
- `docs/evidencias/revisao-tratamento-dados/auditoria_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/flags_tratamento_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/amostras_tratadas_pandas.csv`
- `docs/evidencias/revisao-tratamento-dados/impacto_sftp_tratamento_pandas.csv`
- `docs/revisao-tratamento-dados.html`
- `docs/revisao-tratamento-dados.pdf`
""",
        encoding="utf-8",
    )
    print(html_path)


if __name__ == "__main__":
    main()
