#!/usr/bin/env python3
"""Gera HTML pronto para impressão do levantamento CadÚnico x RCB."""

from collections import Counter
from datetime import date
from html import escape
from pathlib import Path
import importlib.util


HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
SOURCE = HERE / "gerar_levantamento_cadunico_rcb.py"
OUT = PROJECT / "docs/levantamento_cadunico_reforma_casa_brasil.html"

spec = importlib.util.spec_from_file_location("levantamento", SOURCE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def e(value):
    return escape(str(value or ""))


def table(headers, rows, classes=""):
    th = "".join(f"<th>{e(x)}</th>" for x in headers)
    trs = []
    for row in rows:
        trs.append("<tr>" + "".join(f"<td>{e(x)}</td>" for x in row) + "</tr>")
    return f'<table class="{classes}"><thead><tr>{th}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def status_label(value):
    v = value.lower()
    if v.startswith("dispon") or "existe somente" in v:
        return "SIM / CONTROLADO"
    if v.startswith("parcial"):
        return "PARCIAL"
    return "NÃO AGORA"


def main():
    pessoa, familia = m.header(m.PESSOA), m.header(m.FAMILIA)
    themes = Counter(m.tema(c) for c in pessoa + familia)
    theme_rows = sorted(((k, v) for k, v in themes.items()), key=lambda x: (-x[1], x[0]))

    qrows = []
    for pergunta, situacao, fonte, campos, regra, limite in m.QUESTOES:
        qrows.append([status_label(situacao), pergunta, situacao, campos, regra, limite])

    cross_rows = [
        ["RCB → CadÚnico Pessoa", "CPF HMAC; NIS HMAC como fallback", "Cobertura CadÚnico, perfil pessoal e ocupação", "Pronto no modelo; depende de materialização"],
        ["Pessoa → Família", "CO_FAMILIAR_FAM", "Renda, despesas, composição e condições do domicílio", "Pronto no modelo"],
        ["Família → território", "CD_IBGE_CADASTRO; endereço completo restrito", "Município, geocodificação e análises territoriais", "Parcial"],
        ["Endereço → Comunidade Urbana", "latitude/longitude + polígono IBGE FCU", "Dentro, fora ou indeterminado", "Fonte geográfica ainda precisa ser incorporada"],
        ["RCB → histórico MCMV", "CPF/NIS HMAC compatível + data anterior", "Já contemplado, programa, modalidade e empreendimento", "Requer base nominal de beneficiários"],
        ["RCB → escopo da obra", "número do contrato/proposta", "Serviços, itens, custo e conclusão", "Requer orçamento, medição ou vistoria"],
        ["RCB → financiamento anterior do imóvel", "CPF/NIS/contrato do imóvel", "Aquisição financiada antes da reforma", "Requer base financeira/registral adicional"],
    ]

    cols_rows = []
    for origem, cols in (("Pessoa", pessoa), ("Família", familia)):
        for col in cols:
            cols_rows.append([origem, col, m.tema(col), m.uso(col), m.privacidade(col)])

    css = """
    @page { size: A4; margin: 15mm 13mm 16mm; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #17324d; font-size: 9.2px; line-height: 1.38; }
    h1, h2, h3 { color: #123d36; margin: 0 0 8px; }
    h1 { font-size: 30px; line-height: 1.08; max-width: 620px; }
    h2 { font-size: 18px; border-bottom: 2px solid #2e806f; padding-bottom: 5px; margin-top: 18px; }
    h3 { font-size: 12px; margin-top: 13px; }
    p { margin: 5px 0 9px; }
    .cover { min-height: 255mm; padding: 22mm 12mm; background: linear-gradient(145deg,#eef7f3 0%,#fff 53%); position: relative; page-break-after: always; }
    .eyebrow { color:#2e806f; font-size:11px; font-weight:bold; letter-spacing:1.4px; text-transform:uppercase; margin-bottom:16px; }
    .subtitle { font-size:16px; color:#49677f; max-width:580px; margin-top:18px; }
    .cover-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-top:36px; }
    .metric { background:#fff; border:1px solid #cfe1db; border-top:4px solid #2e806f; padding:14px; border-radius:4px; }
    .metric strong { display:block; font-size:25px; color:#123d36; }
    .note { padding:10px 12px; background:#fff8e8; border-left:4px solid #d69b22; margin:12px 0; }
    .ok { background:#e9f6ef; border-left-color:#2e806f; }
    .meta { position:absolute; bottom:25mm; left:12mm; color:#627789; }
    .page { page-break-before: always; }
    .keep { page-break-inside: avoid; }
    table { width:100%; border-collapse:collapse; margin:8px 0 15px; table-layout:fixed; }
    th { background:#173f5f; color:#fff; padding:6px 5px; text-align:left; font-size:8px; }
    td { border-bottom:1px solid #dbe5eb; padding:5px; vertical-align:top; overflow-wrap:anywhere; }
    tr:nth-child(even) td { background:#f6f9fa; }
    .questions th:nth-child(1) { width:10%; } .questions th:nth-child(2) { width:16%; }
    .questions th:nth-child(3) { width:15%; } .questions th:nth-child(4) { width:19%; }
    .questions th:nth-child(5) { width:20%; } .questions th:nth-child(6) { width:20%; }
    .columns { font-size:7.2px; line-height:1.25; }
    .columns th:nth-child(1) { width:7%; } .columns th:nth-child(2) { width:21%; }
    .columns th:nth-child(3) { width:15%; } .columns th:nth-child(4) { width:34%; } .columns th:nth-child(5) { width:23%; }
    .small { font-size:8px; color:#5e7181; }
    ul { margin:5px 0 10px 16px; padding:0; } li { margin:3px 0; }
    """

    html = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>CadÚnico × Reforma Casa Brasil</title><style>{css}</style></head><body>
    <section class="cover">
      <div class="eyebrow">Ministério das Cidades · Levantamento técnico</div>
      <h1>CadÚnico × Reforma Casa Brasil</h1>
      <p class="subtitle">Colunas disponíveis, cruzamentos possíveis e capacidade de resposta às perguntas prioritárias.</p>
      <div class="cover-grid">
        <div class="metric"><strong>198</strong>colunas inventariadas</div>
        <div class="metric"><strong>131</strong>colunas de Pessoa</div>
        <div class="metric"><strong>67</strong>colunas de Família</div>
        <div class="metric"><strong>2.199.185</strong>registros Pessoa locais</div>
        <div class="metric"><strong>9.304.758</strong>registros Família locais</div>
        <div class="metric"><strong>100.859</strong>contratos RCB na evidência disponível</div>
      </div>
      <div class="note" style="margin-top:28px"><b>Conclusão rápida.</b> O cruzamento já está modelado para medir vínculo com o CadÚnico, perfil e condições domiciliares. Serviços da obra, localização em comunidade urbana, contemplação MCMV anterior e financiamento preexistente exigem fontes adicionais.</div>
      <div class="meta">Data de referência do documento: 14/10/2026<br>Documento sem CPF, NIS, endereço ou qualquer registro individual.</div>
    </section>

    <main>
      <h2>1. Resposta às perguntas prioritárias</h2>
      <p>A classificação abaixo diferencia resposta disponível, resposta parcial e lacuna de fonte. Ausência de dado não deve ser apresentada como zero.</p>
      {table(["Resposta", "Pergunta", "Situação atual", "Campos/chave", "Como calcular", "Limitação"], qrows, "questions")}

      <div class="note"><b>Percentual no CadÚnico.</b> A regra é: 100 × contratos RCB vinculados por CPF HMAC ou NIS HMAC ÷ total de contratos RCB deduplicados. O número final não foi calculado neste documento porque o snapshot RCB materializado e o resultado do join não estão presentes no ambiente local. Há evidência de 100.859 contratos, mas não do numerador.</div>

      <h2>2. Cruzamentos possíveis</h2>
      {table(["Cruzamento", "Chave", "O que responde", "Estado"], cross_rows)}

      <h2>3. Grupos de colunas disponíveis</h2>
      <p>As 198 colunas originais estão listadas integralmente no anexo. A tabela resume os principais blocos.</p>
      {table(["Tema", "Quantidade de colunas"], theme_rows)}

      <h2>4. Colunas adicionais recomendadas</h2>
      {table(["Bloco", "Campos", "Resposta habilitada", "Prioridade"], m.ADICIONAIS)}

      <h2>5. Regras de segurança e qualidade</h2>
      <ul>
        <li>CPF e NIS devem ser comparados somente por HMAC determinístico, com a mesma chave e normalização.</li>
        <li>Endereço completo deve permanecer em ambiente restrito; resultados territoriais devem ser publicados apenas de forma agregada.</li>
        <li>Geocodificação deve guardar precisão e permitir a categoria “indeterminado”. Município ou CEP não comprovam localização em comunidade urbana.</li>
        <li>Contemplação anterior no MCMV exige benefício individual e data anterior ao RCB; proximidade de empreendimento não comprova benefício.</li>
        <li>Aplicar deduplicação por contrato e suprimir células pequenas para reduzir risco de reidentificação.</li>
      </ul>

      <section class="page">
        <h2>Anexo A — Todas as colunas do CadÚnico</h2>
        <p class="small">A descrição funcional foi inferida do nome técnico e deve ser validada contra o dicionário oficial de códigos. O inventário não contém valores individuais.</p>
        {table(["Origem", "Coluna original", "Tema", "Uso potencial", "Controle"], cols_rows, "columns")}
      </section>
    </main></body></html>"""
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
