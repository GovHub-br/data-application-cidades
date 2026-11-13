#!/usr/bin/env python3
"""Gera HTML/PDF-ready contendo somente o anexo de colunas do CadÚnico."""

from html import escape
from pathlib import Path
import importlib.util


HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
SOURCE = HERE / "gerar_levantamento_cadunico_rcb.py"
OUT = PROJECT / "docs/anexo_a_todas_colunas_cadunico.html"

spec = importlib.util.spec_from_file_location("levantamento", SOURCE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def e(value):
    return escape(str(value or ""))


def main():
    pessoa, familia = m.header(m.PESSOA), m.header(m.FAMILIA)
    rows = []
    numero = 1
    for origem, cols in (("Pessoa", pessoa), ("Família", familia)):
        for col in cols:
            rows.append(
                "<tr>"
                f"<td>{numero}</td><td>{e(origem)}</td><td>{e(col)}</td>"
                f"<td>{e(m.tema(col))}</td><td>{e(m.uso(col))}</td>"
                f"<td>{e(m.privacidade(col))}</td>"
                "</tr>"
            )
            numero += 1

    html = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
    <title>Anexo A — Todas as colunas do CadÚnico</title><style>
    @page {{ size:A4 landscape; margin:12mm 10mm 13mm; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Arial,Helvetica,sans-serif; color:#17324d; font-size:7.4px; line-height:1.25; }}
    h1 {{ margin:0 0 4px; color:#123d36; font-size:20px; }}
    .summary {{ margin:0 0 8px; font-size:9px; color:#536c7c; }}
    .source {{ background:#eef7f3; border-left:4px solid #2e806f; padding:7px 9px; margin:7px 0 10px; font-size:8px; }}
    table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
    thead {{ display:table-header-group; }}
    th {{ background:#173f5f; color:#fff; padding:5px 4px; text-align:left; font-size:7px; }}
    td {{ border-bottom:1px solid #dbe5eb; padding:3.5px 4px; vertical-align:top; overflow-wrap:anywhere; }}
    tr:nth-child(even) td {{ background:#f6f9fa; }}
    th:nth-child(1) {{ width:4%; }} th:nth-child(2) {{ width:7%; }} th:nth-child(3) {{ width:23%; }}
    th:nth-child(4) {{ width:15%; }} th:nth-child(5) {{ width:31%; }} th:nth-child(6) {{ width:20%; }}
    </style></head><body>
    <h1>Anexo A — Todas as colunas do CadÚnico</h1>
    <p class="summary"><b>198 colunas:</b> 131 da tabela Pessoa e 67 da tabela Família.</p>
    <div class="source"><b>Fontes do inventário:</b> Pessoa — arquivo proveniente do MinIO; Família — arquivo disponível no SharePoint e previsto no pipeline para ingestão no MinIO. A descrição funcional foi inferida do nome técnico e deve ser validada contra o dicionário oficial. Nenhum valor individual é reproduzido neste documento.</div>
    <table><thead><tr><th>#</th><th>Origem</th><th>Coluna original</th><th>Tema</th><th>Uso potencial</th><th>Controle</th></tr></thead>
    <tbody>{''.join(rows)}</tbody></table></body></html>"""
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
