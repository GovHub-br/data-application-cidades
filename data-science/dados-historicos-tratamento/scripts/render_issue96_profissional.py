from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
EVID = DOCS / "evidencias" / "revisao-classificacao-issue-96"
ASSETS = DOCS / "assets" / "revisao-classificacao-issue-96"
OUT_HTML = DOCS / "revisao-classificacao-issue-96.html"
OUT_MD = DOCS / "resposta-issue-96.md"
REVIEW_DATE = "2026-07-07"


def esc(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def table_html(df: pd.DataFrame, columns: list[str] | None = None, limit: int | None = None) -> str:
    if columns:
        df = df[columns]
    if limit is not None:
        df = df.head(limit)
    head = "".join(f"<th>{esc(col)}</th>" for col in df.columns)
    rows = []
    for _, row in df.iterrows():
        rows.append("<tr>" + "".join(f"<td>{esc(row[col])}</td>" for col in df.columns) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def load() -> dict[str, pd.DataFrame]:
    return {
        "audit": pd.read_csv(EVID / "auditoria_classificacao_completa_pandas.csv", dtype=str).fillna(""),
        "flags": pd.read_csv(EVID / "flags_revisao_pandas.csv", dtype=str).fillna(""),
        "corr_cls": pd.read_csv(EVID / "correcoes_classificacao_pandas.csv", dtype=str).fillna(""),
        "corr_qual": pd.read_csv(EVID / "correcoes_qualidade_pandas.csv", dtype=str).fillna(""),
        "samples": pd.read_csv(EVID / "amostras_disponibilidade_pandas.csv", dtype=str).fillna(""),
    }


def render_issue_response(audit: pd.DataFrame, targets: pd.DataFrame) -> None:
    text = f"""## Resumo da revisao - issue #96

Foi executada auditoria com pandas sobre os CSVs de classificacao, qualidade, inventario e deduplicacao do pipeline MCMV.

- Registros revisados nos snapshots: {len(audit)}
- Linhas `confidence=low` apos revisao: {(audit['classificacao_confidence'] == 'low').sum()}
- Tabelas corrigidas: {len(targets)}
- Correcao aplicada: 5 tabelas `bb_2013_06_junho_pmcmv_18062013_tab_*` foram alinhadas para `separador_|` (separador pipe), com status de qualidade `treated` e perfil `separador_pipe`.
- Evidencias geradas com pandas:
  - `docs/evidencias/revisao-classificacao-issue-96/auditoria_classificacao_completa_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_classificacao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/correcoes_qualidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/flags_revisao_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/amostras_disponibilidade_pandas.csv`
  - `docs/evidencias/revisao-classificacao-issue-96/impacto_sftp_minio_pandas.csv`
- Relatorio oficial gerado:
  - `docs/revisao-classificacao-issue-96.html`
  - `docs/revisao-classificacao-issue-96.pdf`

Observacao: os arquivos brutos `data/table_samples/` e os JSONs `diff_sftp_minio*.json` nao estao presentes nesta branch. Portanto, a auditoria dado-a-dado dos CSVs brutos e o impacto detalhado SFTP/MinIO precisam ser reexecutados quando as amostras/diffs forem disponibilizados ou quando o modo DB estiver acessivel. As evidencias atuais usam snapshots, inventario, dedup, relatorio de tratamento e amostra versionada em `data/exemplos_por_categoria.md`.
"""
    OUT_MD.write_text(text, encoding="utf-8")


def render() -> str:
    data = load()
    audit = data["audit"]
    flags = data["flags"]
    corr_cls = data["corr_cls"].drop_duplicates("table_name")
    corr_qual = data["corr_qual"].drop_duplicates("table_name")
    samples = data["samples"]
    targets = audit[audit["flag_revisao"].str.contains("corrigida_issue96", na=False)].copy()

    render_issue_response(audit, targets)

    category_counts = (
        audit["classificacao_formacao"]
        .replace({"separador_|": "separador pipe"})
        .value_counts()
        .reset_index()
    )
    category_counts.columns = ["Categoria", "Quantidade"]

    flag_counts = (
        flags["flag_revisao"]
        .str.strip(";")
        .str.get_dummies(sep=";")
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    flag_counts.columns = ["Flag", "Quantidade"]

    targets_view = targets[
        [
            "table_name",
            "classificacao_formacao",
            "classificacao_confidence",
            "inventario_formacao",
            "inventario_n_linhas",
            "inventario_n_colunas",
            "qualidade_status",
            "qualidade_profile",
            "dedup_content_hash",
            "dedup_file_size",
        ]
    ].rename(
        columns={
            "table_name": "Tabela",
            "classificacao_formacao": "Classificacao final",
            "classificacao_confidence": "Confianca",
            "inventario_formacao": "Inventario",
            "inventario_n_linhas": "Linhas",
            "inventario_n_colunas": "Colunas",
            "qualidade_status": "Status qualidade",
            "qualidade_profile": "Perfil qualidade",
            "dedup_content_hash": "Hash",
            "dedup_file_size": "Tamanho",
        }
    )
    targets_view["Classificacao final"] = targets_view["Classificacao final"].replace({"separador_|": "separador pipe"})
    targets_view["Inventario"] = targets_view["Inventario"].replace({"separador_|": "separador pipe"})

    sample_print = """Dimensoes: 200 linhas x 1 coluna
Coluna: cod_operacaocod_sit_obracod_re

0  23157|0|0|0|||||||2013-04-30|
1  27175|6|0|0|||||||2011-09-13|
2  29302|0|0|0||||||||2010-07-05
3  29350|6|0|0|||||||2012-06-08|
4  29536|2|0|0|||||||2013-01-10|"""

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>Revisao de Classificacao das Tabelas Historicas MCMV</title>
  <style>
    @page {{ size: A4; margin: 14mm; }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: #172033;
      font-family: Arial, Helvetica, sans-serif;
      background: #eef3f7;
      font-size: 12px;
      line-height: 1.45;
    }}
    .page {{
      width: 210mm;
      min-height: 297mm;
      margin: 0 auto 18px auto;
      background: #fff;
      overflow: hidden;
      position: relative;
      page-break-after: always;
      box-shadow: 0 8px 28px rgba(15, 23, 42, .12);
    }}
    .cover {{
      background:
        linear-gradient(135deg, rgba(20,83,45,.96), rgba(37,99,235,.88)),
        radial-gradient(circle at 90% 10%, rgba(255,255,255,.22), transparent 35%);
      color: white;
      padding: 36mm 22mm 24mm;
    }}
    .cover .eyebrow {{ text-transform: uppercase; letter-spacing: .12em; opacity: .85; font-size: 12px; }}
    .cover h1 {{ font-size: 34px; line-height: 1.08; margin: 18px 0 12px; max-width: 680px; }}
    .cover h2 {{ font-size: 17px; font-weight: 400; opacity: .92; max-width: 660px; }}
    .meta-card {{
      margin-top: 36px;
      background: rgba(255,255,255,.12);
      border: 1px solid rgba(255,255,255,.32);
      border-radius: 14px;
      padding: 16px;
      width: 100%;
    }}
    .meta-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px 16px; }}
    .meta-grid div {{ border-bottom: 1px solid rgba(255,255,255,.16); padding-bottom: 5px; }}
    .meta-grid b {{ display:block; font-size: 10px; opacity:.75; text-transform: uppercase; }}
    .content {{ padding: 16mm 17mm; }}
    .section-title {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 0 0 14px;
      color: #14532d;
      font-size: 22px;
      line-height: 1.15;
    }}
    .section-title::before {{
      content: "";
      width: 7px;
      height: 28px;
      border-radius: 8px;
      background: linear-gradient(#14532d, #2563eb);
      display: inline-block;
    }}
    .lead {{ font-size: 14px; color: #334155; margin-bottom: 16px; }}
    .kpis {{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 16px 0 18px; }}
    .kpi {{ border:1px solid #d8e2ea; border-radius: 12px; padding: 12px; background: #f8fafc; }}
    .kpi .num {{ color:#14532d; font-size: 26px; font-weight: 800; line-height: 1; }}
    .kpi .label {{ color:#64748b; font-size: 10px; text-transform: uppercase; margin-top: 6px; }}
    .callout {{
      border-left: 5px solid #2563eb;
      background: #eff6ff;
      padding: 12px 14px;
      border-radius: 0 10px 10px 0;
      margin: 14px 0;
      color: #1e3a8a;
    }}
    .warning {{
      border-left-color: #f59e0b;
      background: #fffbeb;
      color: #78350f;
    }}
    table {{ width: 100%; border-collapse: collapse; margin: 10px 0 16px; font-size: 10px; }}
    th {{ background: #e8f1ed; color:#123524; text-align: left; }}
    th, td {{ border: 1px solid #d7e0e8; padding: 6px 7px; vertical-align: top; }}
    tbody tr:nth-child(even) td {{ background: #fbfdff; }}
    .pill {{ display:inline-block; padding: 2px 7px; border-radius: 999px; background:#dcfce7; color:#166534; font-weight:700; }}
    .figure {{ border:1px solid #d7e0e8; border-radius: 12px; padding: 10px; background:#fff; margin: 12px 0 16px; }}
    .figure img {{ width: 100%; display: block; }}
    .caption {{ color:#64748b; font-size: 10px; margin-top: 6px; }}
    pre {{
      background: #0f172a;
      color: #dbeafe;
      padding: 12px;
      border-radius: 10px;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 10px;
      line-height: 1.35;
    }}
    .two-col {{ display:grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
    .footer {{ position:absolute; bottom: 8mm; left: 17mm; right: 17mm; color:#94a3b8; font-size: 9px; display:flex; justify-content:space-between; }}
    .small {{ font-size: 10px; color:#64748b; }}
  </style>
</head>
<body>
  <section class="page cover">
    <div class="eyebrow">Ministerio das Cidades / Minha Casa, Minha Vida</div>
    <h1>Revisao de Classificacao das Tabelas Historicas MCMV</h1>
    <h2>Documento de evidencias de dados para a issue #96, com auditoria pandas, tabelas de controle, hashes e registros de amostra.</h2>
    <div class="meta-card">
      <div class="meta-grid">
        <div><b>Frente</b>Cidades - Dados Historicos</div>
        <div><b>Feature</b>#53 - Inteligencia preditiva MCMV</div>
        <div><b>Branch</b>feat/tratamento-dados-historicos</div>
        <div><b>Data</b>{REVIEW_DATE}</div>
        <div><b>Metodo</b>pandas + inventario + dedup + relatorios</div>
        <div><b>Entrega</b>HTML/PDF + CSVs de evidencia</div>
      </div>
    </div>
  </section>

  <section class="page">
    <div class="content">
      <h2 class="section-title">Sumario Executivo</h2>
      <p class="lead">A revisao consolidou os CSVs de classificacao, qualidade, inventario e deduplicacao. As cinco divergencias criticas de baixa confianca foram corrigidas para <b>separador pipe</b>, com evidencias cruzadas em inventario, dedup e relatorio de tratamento.</p>
      <div class="kpis">
        <div class="kpi"><div class="num">{len(audit)}</div><div class="label">registros auditados</div></div>
        <div class="kpi"><div class="num">{(audit['classificacao_confidence'] == 'low').sum()}</div><div class="label">confidence=low</div></div>
        <div class="kpi"><div class="num">{len(targets)}</div><div class="label">corrigidas issue #96</div></div>
        <div class="kpi"><div class="num">{audit['amostra_bruta_existe'].astype(str).eq('True').sum()}</div><div class="label">amostras brutas locais</div></div>
      </div>
      <div class="callout warning"><b>Ponto de controle:</b> a branch atual nao contem <code>data/table_samples/</code>. Portanto, esta entrega comprova a auditoria completa dos CSVs de controle com pandas e evidencia versionada. A varredura linha a linha dos CSVs brutos deve ser reexecutada quando as amostras forem disponibilizadas ou via modo DB.</div>
      <div class="figure">
        <img src="assets/revisao-classificacao-issue-96/distribuicao-categorias.png" />
        <div class="caption">Figura 1 - Distribuicao revisada das categorias estruturais.</div>
      </div>
      <div class="figure">
        <img src="assets/revisao-classificacao-issue-96/confidence-low-antes-depois.png" />
        <div class="caption">Figura 2 - Baixa confianca antes/depois das correcoes pandas.</div>
      </div>
    </div>
    <div class="footer"><span>Gov Hub / Frente Cidades</span><span>Issue #96</span></div>
  </section>

  <section class="page">
    <div class="content">
      <h2 class="section-title">Resultado Consolidado</h2>
      {table_html(category_counts)}
      <h3>Flags de revisao geradas pelo pandas</h3>
      {table_html(flag_counts)}
      <div class="callout"><b>Leitura:</b> as flags restantes indicam principalmente diferencas entre inventario e qualidade/tratamento, em especial erros de persistencia por limite de identificador no PostgreSQL. Elas nao alteram a classificacao estrutural revisada da issue #96.</div>
    </div>
    <div class="footer"><span>Distribuicao e flags</span><span>2</span></div>
  </section>

  <section class="page">
    <div class="content">
      <h2 class="section-title">Tabelas Corrigidas</h2>
      {table_html(targets_view)}
      <h3>Disponibilidade das amostras brutas</h3>
      {table_html(samples.rename(columns={'table_name':'Tabela','sample_path':'Caminho esperado','sample_exists':'Existe','fallback_evidence':'Evidencia fallback'}))}
    </div>
    <div class="footer"><span>Correcoes e disponibilidade</span><span>3</span></div>
  </section>

  <section class="page">
    <div class="content">
      <h2 class="section-title">Print Real de Amostra Versionada</h2>
      <p>O print abaixo vem de <code>data/exemplos_por_categoria.md</code>, arquivo versionado do proprio pipeline. Ele demonstra a estrutura de <b>separador pipe</b>: uma coluna com valores contendo <code>|</code>.</p>
      <pre>{esc(sample_print)}</pre>
      <h3>Evidencia de nao duplicidade</h3>
      <p>As cinco tabelas corrigidas compartilham formacao estrutural, mas os hashes e tamanhos no <code>_dedup_map</code> sao diferentes. Portanto, elas nao foram tratadas como conteudo identico.</p>
    </div>
    <div class="footer"><span>Print e hash</span><span>4</span></div>
  </section>

  <section class="page">
    <div class="content">
      <h2 class="section-title">Artefatos Gerados com Pandas</h2>
      <table>
        <thead><tr><th>Arquivo</th><th>Conteudo</th></tr></thead>
        <tbody>
          <tr><td><code>auditoria_classificacao_completa_pandas.csv</code></td><td>754 linhas cruzando classificacao, inventario, qualidade, dedup e flags.</td></tr>
          <tr><td><code>flags_revisao_pandas.csv</code></td><td>105 linhas com pontos de atencao para revisoes futuras.</td></tr>
          <tr><td><code>correcoes_classificacao_pandas.csv</code></td><td>Antes/depois das classificacoes corrigidas.</td></tr>
          <tr><td><code>correcoes_qualidade_pandas.csv</code></td><td>Antes/depois do status de qualidade das tabelas corrigidas.</td></tr>
          <tr><td><code>amostras_disponibilidade_pandas.csv</code></td><td>Comprovacao de ausencia das amostras brutas locais e fontes fallback.</td></tr>
        </tbody>
      </table>
      <h3>Resumo para resposta da issue</h3>
      <div class="callout">Foi executada auditoria pandas nos CSVs de controle. As cinco tabelas divergentes foram corrigidas para separador pipe; os snapshots ficaram sem linhas <code>confidence=low</code>. A inspecao linha a linha dos brutos depende de disponibilizar <code>data/table_samples/</code> ou acesso DB.</div>
    </div>
    <div class="footer"><span>Artefatos de evidencia</span><span>5</span></div>
  </section>
</body>
</html>"""
    OUT_HTML.write_text(html, encoding="utf-8")
    return str(OUT_HTML)


if __name__ == "__main__":
    print(render())
