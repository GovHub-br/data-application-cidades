"""Gera a documentação do pipeline bronze → silver → gold do conjuntura.

Por que gerado e não escrito à mão: documentação de pipeline escrita
manualmente desatualiza na primeira mudança de model, e aí passa a mentir —
o que é pior do que não existir. Aqui tudo sai do `manifest.json` e do
`catalog.json` que o próprio dbt produz, então o documento sempre reflete o
código que está rodando.

Cobre, para cada cadeia:
  - a fonte no lake (caminho do parquet e piso de linhas do contrato)
  - os models de cada camada, com o que cada um faz
  - materialização, nº de colunas e de linhas
  - os testes pendurados em cada model

Pré-requisito: `dbt docs generate` rodado em um diretório privado (é ele que
escreve os dois JSON). Os JSONs do dbt nunca são publicados: eles contêm SQL
compilado e o catálogo completo da bronze, que não fazem parte do catálogo de
metadados de consumo.

Uso:
    poetry run python scripts/conjuntura/gerar_doc_pipeline.py [saida.html] \\
        --target-dir /diretorio/privado/do/dbt
"""

from __future__ import annotations

import hashlib
import html
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
ALVO = RAIZ / "dbt" / "mcid" / "target"
PROJETO = "conjuntura_dbt"

#: agrupamento por origem, para o documento seguir o mundo real em vez da
#: ordem alfabética dos models
DOMINIOS = {
    "ibge": "IBGE — SIDRA",
    "bacen": "Banco Central",
    "abecip": "ABECIP",
    "geavo": "Caixa — sistema GEAVO",
    "fgv": "FGV",
    "fipezap": "FIPE",
    "infomoney": "B3 / Alpha Vantage",
    "novo_caged": "Novo CAGED",
    "siafi": "SIAFI / Tesouro",
    "manual": "Entrada manual",
    "qualidade": "Qualidade do dado",
}


def _rotulo(corpo: str) -> str | None:
    for chave, rotulo in DOMINIOS.items():
        if chave in corpo:
            return rotulo
    return None


def dominio_de(chave: str, modelos: dict, manifest: dict, _memo=None) -> str:
    """Domínio de um model, resolvido pela LINHAGEM e não pelo nome.

    O nome não serve: `gold_continuo_sinapi` não diz "ibge" em lugar nenhum,
    e a primeira versão jogava 29 dos 30 golds num balde "Outros" — o que
    esvaziava justamente a parte do documento que interessa. Subindo a
    linhagem até a fonte, o gold herda o domínio de quem o alimenta.
    """
    memo = _memo if _memo is not None else {}
    if chave in memo:
        return memo[chave]
    memo[chave] = "Outros"  # corta ciclo, se houver

    no = modelos.get(chave)
    if no is None:
        return "Outros"

    deps = no.get("depends_on", {}).get("nodes", [])

    # 1) fonte declarada é a resposta mais confiável
    for d in deps:
        if d in manifest.get("sources", {}):
            r = _rotulo(
                manifest["sources"][d].get("meta", {}).get("caminho", "")
            ) or _rotulo(manifest["sources"][d]["name"])
            if r:
                memo[chave] = r
                return r

    # 2) senão, herda do primeiro pai que souber responder
    for d in deps:
        if d in modelos:
            r = dominio_de(d, modelos, manifest, memo)
            if r != "Outros":
                memo[chave] = r
                return r

    # 3) por último, tenta pelo próprio nome (cobre as raízes manuais)
    r = _rotulo(no["name"])
    memo[chave] = r or "Outros"
    return memo[chave]


def camada_de(no: dict) -> str:
    caminho = no.get("path", "")
    for camada in ("bronze", "silver", "gold", "qualidade"):
        if f"/{camada}/" in caminho or caminho.startswith(f"{camada}/"):
            return "gold" if camada == "qualidade" else camada
    return "?"


#: identificadores de pessoa — mesma lista de `macros/coluna_sensivel.sql`.
#: Mantida em sincronia manualmente; se mudar lá, mudar aqui.
PADROES_SENSIVEIS = (
    "cpf",
    "cnpj",
    "mutuario",
    "nascimento",
    "logradouro",
    "endereco",
    "telefone",
    "celular",
    "email",
    "nis",
    "titular",
    "beneficiario",
)


def _e_sensivel(nome: str) -> bool:
    baixo = nome.lower()
    return baixo == "cep" or any(p in baixo for p in PADROES_SENSIVEIS)


DESCRICOES_SEMANTICAS = {
    "periodo": "Rótulo do período de referência da observação.",
    "data_referencia": "Data que identifica o período de referência da observação.",
    "data": "Data de referência da observação.",
    "ano": "Ano do período de referência.",
    "trimestre": "Trimestre do período de referência.",
    "mes": "Mês do período de referência.",
    "empresa": "Empresa ou grupo empresarial a que o indicador se refere.",
    "banco": "Instituição financeira a que o indicador se refere.",
    "indice": "Número-índice divulgado pela fonte para o período.",
    "dt_silver": "Momento de processamento da camada Silver.",
    "dt_referencia_extracao": "Data da extração de origem que fundamenta o snapshot.",
    "acao_governo_codigo": "Código da ação orçamentária na fonte governamental.",
    "acao_nome": "Denominação resumida da ação orçamentária.",
}


def descricao_semantica(nome: str) -> str:
    """Descrição segura por convenção para campos ainda sem YAML específico.

    Não infere valores nem consulta linhas: usa apenas o identificador técnico
    da coluna. Isso elimina campos mudos do catálogo RAG sem transformar o
    documento em uma amostra de dados.
    """
    if nome in DESCRICOES_SEMANTICAS:
        return DESCRICOES_SEMANTICAS[nome]
    if nome.startswith("var_") or "_var_" in nome:
        return "Variação percentual do indicador, conforme o recorte indicado no nome do campo."
    if nome.startswith("qtd_") or nome.startswith("quantidade_"):
        return "Quantidade do fenômeno medido no período de referência."
    if nome.startswith("valor_") or nome.startswith("vlr_"):
        return "Valor monetário do indicador no período de referência."
    if nome.startswith("financ_") or nome.startswith("financiamento_"):
        return "Medida de financiamento habitacional no recorte indicado pelo campo."
    if nome.startswith("ticket_"):
        return "Ticket médio de lançamentos no recorte indicado pelo campo."
    if nome.startswith("perc_") or nome.endswith("_perc") or "_percent" in nome:
        return "Participação percentual no recorte indicado pelo campo."
    if nome.startswith("total_") or nome.endswith("_total"):
        return "Total agregado do indicador no período de referência."
    if nome.startswith("saldo_"):
        return "Saldo do indicador no período de referência."
    if nome.startswith("taxa_"):
        return "Taxa associada ao indicador no período de referência."
    if nome.startswith("precos_") or nome.startswith("custo_"):
        return "Medida de preço ou custo no período de referência."
    return "Atributo técnico do indicador; consultar a descrição da tabela para o contexto e a fonte."


def sanitizar_artefatos_dbt(alvo: Path) -> int:
    """Mascara nomes de coluna com identificador de pessoa nos JSON do dbt.

    O `dbt docs generate` lê o catálogo do banco e escreve TODAS as colunas
    em `catalog.json`, inclusive as da bronze — que espelha a origem e tem
    `nu_cpf_cgc_mutuario`, `no_mutuario`, `dt_nascimento`. Servir ou publicar
    esse arquivo vazaria a estrutura do dado pessoal.

    Esta função roda depois do `dbt docs generate` e antes de qualquer
    publicação. É idempotente. A garantia não depende de a anonimização a
    montante ter funcionado: mesmo que o dado chegue cru na bronze, o nome
    não sai daqui.
    """
    trocas = 0
    for arquivo in ("catalog.json", "manifest.json"):
        caminho = alvo / arquivo
        if not caminho.exists():
            continue
        dados = json.loads(caminho.read_text())
        for grupo in ("nodes", "sources"):
            for no in (dados.get(grupo) or {}).values():
                colunas = no.get("columns")
                if not isinstance(colunas, dict):
                    continue
                renomeadas = {}
                for nome, corpo in colunas.items():
                    if _e_sensivel(nome):
                        novo = "sensivel_" + hashlib.md5(nome.encode()).hexdigest()[:8]
                        if isinstance(corpo, dict):
                            corpo = {
                                **corpo,
                                "name": novo,
                                "description": "Coluna com identificador de pessoa — nome omitido.",
                            }
                        renomeadas[novo] = corpo
                        trocas += 1
                    else:
                        renomeadas[nome] = corpo
                no["columns"] = renomeadas
        caminho.write_text(json.dumps(dados))
    return trocas


def carregar(alvo: Path):
    manifest = json.loads((alvo / "manifest.json").read_text())
    try:
        catalog = json.loads((alvo / "catalog.json").read_text())
    except FileNotFoundError:
        catalog = {"nodes": {}}
    return manifest, catalog


def montar(manifest: dict, catalog: dict):
    modelos = {
        k: v
        for k, v in manifest["nodes"].items()
        if v["resource_type"] == "model" and PROJETO in v.get("path", "")
    }
    testes = defaultdict(list)
    for no in manifest["nodes"].values():
        if no["resource_type"] != "test":
            continue
        for dep in no.get("depends_on", {}).get("nodes", []):
            if dep in modelos:
                testes[dep].append(no["name"])

    fontes = {
        k: v
        for k, v in manifest.get("sources", {}).items()
        if v.get("source_name") == "lake_staging"
    }

    # jusante: model -> quem depende dele
    jusante = defaultdict(list)
    for k, v in modelos.items():
        for dep in v.get("depends_on", {}).get("nodes", []):
            jusante[dep].append(k)

    return modelos, testes, fontes, jusante


def colunas_do_model(manifest: dict, catalog: dict, chave: str) -> list[dict]:
    """Metadados de coluna permitidos para publicação.

    O catálogo público é deliberadamente uma visão menor do artefato do dbt:
    só descreve a camada de consumo (silver/gold), nunca inclui valores,
    consultas compiladas, nem a bronze. Uma coluna de identificador pessoal
    nesta camada é uma falha de contrato e bloqueia a publicação, em vez de
    ser mascarada silenciosamente.
    """
    no_manifest = manifest["nodes"][chave]
    no_catalog = catalog.get("nodes", {}).get(chave, {})
    descricoes = no_manifest.get("columns", {})
    catalogo = no_catalog.get("columns", {})
    nomes = sorted(set(descricoes) | set(catalogo))
    saida = []
    for nome in nomes:
        if _e_sensivel(nome):
            raise ValueError(
                f"Coluna sensível chegou ao catálogo de consumo: {no_manifest['name']}.{nome}"
            )
        metadado = catalogo.get(nome, {})
        documentacao = descricoes.get(nome, {})
        saida.append(
            {
                "nome": nome,
                "tipo": metadado.get("type")
                or documentacao.get("data_type")
                or "não informado",
                "descricao": documentacao.get("description")
                or metadado.get("comment")
                or descricao_semantica(nome),
            }
        )
    return saida


def carregar_contagens() -> dict[str, int]:
    """Nº de linhas por model, lido de `gold_qualidade_inventario`.

    O `catalog.json` do dbt-postgres só traz `has_stats`, sem `num_rows` —
    por isso a contagem vem do model de inventário que o próprio projeto
    mantém. Se o banco não estiver acessível, o documento sai sem essa coluna
    em vez de falhar.
    """
    try:
        import subprocess, re as _re

        r = subprocess.run(
            [
                "poetry",
                "run",
                "dbt",
                "show",
                "--inline",
                "select model, linhas from conjuntura_continuo_mart.gold_qualidade_inventario",
                "--output",
                "json",
                "--limit",
                "300",
            ],
            cwd=RAIZ / "dbt" / "mcid",
            capture_output=True,
            text=True,
            timeout=300,
        )
        dados = json.loads(_re.search(r"\{[\s\S]*\}", r.stdout).group(0))["show"]
        return {
            x["model"]: int(x["linhas"]) for x in dados if x.get("linhas") is not None
        }
    except Exception as exc:  # noqa: BLE001
        print(f"  (sem contagem de linhas: {exc})")
        return {}


def stats(catalog: dict, chave: str, contagens: dict, nome: str):
    no = catalog.get("nodes", {}).get(chave)
    ncols = len(no.get("columns", {})) if no else None
    return ncols, contagens.get(nome)


def e(txt) -> str:
    return html.escape(str(txt or "")).replace("\n", " ")


def gerar(saida: Path, alvo: Path = ALVO, incluir_contagens: bool = True) -> None:
    manifest, catalog = carregar(alvo)
    modelos, testes, fontes, jusante = montar(manifest, catalog)
    contagens = carregar_contagens() if incluir_contagens else {}

    memo: dict[str, str] = {}
    por_dominio = defaultdict(lambda: defaultdict(list))
    for chave, no in modelos.items():
        por_dominio[dominio_de(chave, modelos, manifest, memo)][camada_de(no)].append(
            (chave, no)
        )

    n_testes = sum(len(v) for v in testes.values())
    partes = [
        _CABECALHO.format(
            data=date.today().strftime("%d/%m/%Y"),
            n_models=len(modelos),
            n_fontes=len(fontes),
            n_testes=n_testes,
            n_dominios=len(por_dominio),
        )
    ]

    for dominio in sorted(por_dominio):
        partes.append(f"<h2>{e(dominio)}</h2>")
        camadas = por_dominio[dominio]
        # Bronze é espelho técnico da origem. Não é uma camada de consumo e
        # portanto não pertence ao catálogo publicável de metadados.
        for camada in ("silver", "gold"):
            for chave, no in sorted(camadas.get(camada, []), key=lambda x: x[1]["name"]):
                ncols, nlinhas = stats(catalog, chave, contagens, no["name"])
                pais = [
                    modelos[d]["name"]
                    for d in no.get("depends_on", {}).get("nodes", [])
                    if d in modelos
                ]
                filhos = [
                    modelos[f]["name"] for f in jusante.get(chave, []) if f in modelos
                ]
                caminho_fonte = ""
                for d in no.get("depends_on", {}).get("nodes", []):
                    if d in manifest.get("sources", {}):
                        caminho_fonte = (
                            manifest["sources"][d].get("meta", {}).get("caminho", "")
                        )

                linhas_html = [f'<div class="model {camada}">']
                linhas_html.append(
                    f'<div class="mh"><span class="badge {camada}">{camada}</span>'
                    f'<span class="mn">{e(no["name"])}</span>'
                    f'<span class="meta">{e(no["config"]["materialized"])}'
                    + (f" · {ncols} col" if ncols else "")
                    + (
                        f" · {nlinhas:,} linhas".replace(",", ".")
                        if nlinhas is not None
                        else ""
                    )
                    + "</span></div>"
                )
                desc = (no.get("description") or "").strip()
                if desc:
                    linhas_html.append(f'<p class="desc">{e(desc[:420])}</p>')
                colunas = colunas_do_model(manifest, catalog, chave)
                if colunas:
                    linhas_html.append(
                        '<details class="cols"><summary>Campos ('
                        + str(len(colunas))
                        + ")</summary><table><thead><tr>"
                        "<th>Campo</th><th>Tipo</th><th>Significado</th>"
                        "</tr></thead><tbody>"
                    )
                    for coluna in colunas:
                        linhas_html.append(
                            "<tr><td><code>"
                            + e(coluna["nome"])
                            + "</code></td><td>"
                            + e(coluna["tipo"])
                            + "</td><td>"
                            + e(coluna["descricao"])
                            + "</td></tr>"
                        )
                    linhas_html.append("</tbody></table></details>")
                if caminho_fonte:
                    linhas_html.append(
                        f'<div class="rel"><b>lê do lake:</b> <code>{e(caminho_fonte)}</code></div>'
                    )
                if pais:
                    linhas_html.append(
                        f'<div class="rel"><b>vem de:</b> {", ".join(f"<code>{e(p)}</code>" for p in pais)}</div>'
                    )
                if filhos:
                    linhas_html.append(
                        f'<div class="rel"><b>alimenta:</b> {", ".join(f"<code>{e(c)}</code>" for c in filhos)}</div>'
                    )
                if testes.get(chave):
                    linhas_html.append(
                        f'<div class="rel testes"><b>{len(testes[chave])} testes:</b> '
                        + ", ".join(
                            f"<code>{e(t[:46])}</code>" for t in sorted(testes[chave])[:6]
                        )
                        + ("…" if len(testes[chave]) > 6 else "")
                        + "</div>"
                    )
                linhas_html.append("</div>")
                partes.append("".join(linhas_html))

    saida.write_text("\n".join(partes), encoding="utf-8")
    print(f"{saida}  ({len(modelos)} models, {len(fontes)} fontes, {n_testes} testes)")


_CABECALHO = """<title>Pipeline do Conjuntura</title>
<style>
 :root{{--azul:#1351B4;--esc:#0C326F;--tinta:#1B1B1B;--md:#454545;--cl:#6b7280;
  --linha:#c7d2e3;--fundo:#f5f7fb;--bronze:#8a5a2b;--silver:#5b6b7c;--gold:#a07a00}}
 *{{box-sizing:border-box}}
 body{{font-family:"Segoe UI",Roboto,Helvetica,Arial,sans-serif;color:var(--tinta);
  background:#fff;font-size:14px;line-height:1.55;margin:0;padding:28px 22px 60px;max-width:1080px}}
 h1{{font-size:26px;color:var(--esc);margin:0 0 6px}}
 h2{{font-size:16px;color:var(--esc);margin:34px 0 12px;padding-bottom:6px;
  border-bottom:2px solid var(--azul)}}
 .sub{{color:var(--md);margin:0 0 18px;max-width:70ch}}
 .cards{{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0 4px}}
 .card{{border:1px solid var(--linha);border-radius:6px;padding:8px 14px;background:var(--fundo)}}
 .card b{{display:block;font-size:20px;color:var(--esc);line-height:1.1}}
 .card span{{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--md)}}
 .model{{border:1px solid var(--linha);border-left:4px solid var(--azul);border-radius:5px;
  padding:9px 12px;margin:0 0 8px}}
 .model.bronze{{border-left-color:var(--bronze)}}
 .model.silver{{border-left-color:var(--silver)}}
 .model.gold{{border-left-color:var(--gold)}}
 .mh{{display:flex;align-items:baseline;gap:9px;flex-wrap:wrap}}
 .badge{{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
  padding:2px 7px;border-radius:9px;color:#fff}}
 .badge.bronze{{background:var(--bronze)}} .badge.silver{{background:var(--silver)}}
 .badge.gold{{background:var(--gold)}}
 .mn{{font-family:Consolas,"DejaVu Sans Mono",monospace;font-weight:700;font-size:13px}}
 .meta{{font-size:11.5px;color:var(--cl);margin-left:auto}}
 .desc{{margin:6px 0 4px;color:var(--md);font-size:12.7px}}
 .rel{{font-size:12px;color:var(--md);margin-top:3px}}
 .rel b{{color:var(--tinta);font-weight:600}}
 .rel.testes{{color:#168821}} .rel.testes b{{color:#168821}}
 details.cols{{margin-top:8px}} summary{{cursor:pointer;font-weight:600;color:var(--esc)}}
 table{{border-collapse:collapse;width:100%;margin-top:6px;font-size:11.5px}}
 th,td{{border:1px solid var(--linha);padding:5px 7px;text-align:left;vertical-align:top}}
 th{{background:var(--fundo);color:var(--esc)}}
 code{{font-family:Consolas,"DejaVu Sans Mono",monospace;font-size:11.5px;
  background:var(--fundo);border:1px solid #e6ebf3;border-radius:3px;padding:0 4px}}
 @media (prefers-color-scheme:dark){{
  :root:not([data-theme="light"]) body{{background:#12161c;color:#e8eaed}}
  :root:not([data-theme="light"]) h1,:root:not([data-theme="light"]) h2{{color:#8ab4f8}}
  :root:not([data-theme="light"]) .card,:root:not([data-theme="light"]) code{{background:#1b2028;border-color:#2b323c}}
  :root:not([data-theme="light"]) .model{{border-color:#2b323c}}
  :root:not([data-theme="light"]) .desc,:root:not([data-theme="light"]) .rel,
  :root:not([data-theme="light"]) .sub{{color:#b6bcc6}}
  :root:not([data-theme="light"]) .rel b{{color:#e8eaed}}
  :root:not([data-theme="light"]) .card b{{color:#8ab4f8}}
 }}
</style>
<h1>Pipeline do Conjuntura — bronze → silver → gold</h1>
<p class="sub">Catálogo de <b>metadados</b> gerado a partir do dbt: descreve
tabelas e campos das camadas silver e gold, sem valores de dados, SQL compilado,
bronze ou artefatos internos do dbt.</p>
<div class="cards">
 <div class="card"><b>{n_models}</b><span>models</span></div>
 <div class="card"><b>{n_fontes}</b><span>fontes no lake</span></div>
 <div class="card"><b>{n_testes}</b><span>testes</span></div>
 <div class="card"><b>{n_dominios}</b><span>domínios</span></div>
 <div class="card"><b>{data}</b><span>gerado em</span></div>
</div>
"""


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gera catálogo público seguro de metadados."
    )
    parser.add_argument(
        "saida", nargs="?", type=Path, default=RAIZ / "docs-conjuntura" / "pipeline.html"
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=ALVO,
        help="diretório privado contendo manifest.json e catalog.json",
    )
    parser.add_argument(
        "--sem-contagens",
        action="store_true",
        help="não consulta o banco para obter contagens de linhas",
    )
    args = parser.parse_args()
    destino = args.saida
    destino.parent.mkdir(parents=True, exist_ok=True)
    gerar(destino, args.target_dir, incluir_contagens=not args.sem_contagens)
