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

    status_counts = audit["status"].value_counts().to_dict() if "status" in audit.columns else {}
    metric_cards = [
        ("Tabelas revisadas", summary.get("tables_in_quality", len(audit))),
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
    sample_print = esc(samples.iloc[0]["sample_print"]) if len(samples) and "sample_print" in samples.columns else "Sem amostras."
    sftp_html = table_html(sftp, list(sftp.columns), 20)

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
    .grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; margin: 12px 0 18px; }}
    .metric {{ border: 1px solid #dbe3ee; border-radius: 10px; padding: 12px; background: #f8fafc; min-height: 70px; }}
    .metric span {{ display: block; color: #64748b; font-size: 10px; text-transform: uppercase; }}
    .metric strong {{ display: block; color: #14532d; font-size: 24px; margin-top: 4px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 9px; margin: 10px 0 16px; }}
    th, td {{ border: 1px solid #dbe3ee; padding: 5px; vertical-align: top; overflow-wrap: anywhere; }}
    th {{ background: #eaf3ee; color: #143822; text-align: left; }}
    pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #0f172a; color: #e2e8f0; padding: 10px; border-radius: 8px; font-size: 8.5px; }}
    .note {{ border-left: 4px solid #2563eb; background: #eff6ff; padding: 10px 12px; }}
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
      <p class="note">Este relatorio prioriza evidencias reproduziveis: CSVs de auditoria, flags, amostras pandas, deduplicacao e impacto SFTP/MinIO.</p>
      <h3>Principais Flags</h3>
      {top_flags}
    </div>
  </section>
  <section class="page">
    <div class="content">
      <h2>Evidencias</h2>
      <h3>Amostra pandas</h3>
      <pre>{sample_print}</pre>
      <h3>Impacto SFTP/MinIO</h3>
      {sftp_html}
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

- Tabelas revisadas: {summary.get("tables_in_quality", len(audit))}
- CSVs tratados encontrados: {summary.get("treated_files_found", 0)}
- Flags de revisao: {summary.get("flags", len(flags))}
- Erros: {summary.get("status_error", 0)}
- Descartes: {summary.get("status_discarded", 0)}

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
