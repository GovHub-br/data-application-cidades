#!/usr/bin/env python3
"""Gera a página do Boletim de Conjuntura a partir das tabelas gold.

Mesmo dado do dashboard do Superset, com a identidade visual do boletim
publicado. Ver `.claude/skills/publicar-boletim-conjuntura/`.

O número é lido; o texto vem de `scripts/conjuntura/dados/boletim-editorial.yml`. Nada
de texto é inventado aqui: o gerador só posiciona o que já está escrito.

Uso:
    poetry run python scripts/conjuntura/gerar_pagina_boletim.py --edicao 1T2026
"""

from __future__ import annotations

import argparse
import html
import json
import os
import pathlib
import re
import unicodedata
from typing import Any

import yaml

RAIZ = pathlib.Path(__file__).resolve().parents[2]
EDITORIAL = RAIZ / "scripts" / "conjuntura" / "dados" / "boletim-editorial.yml"
GABARITO = RAIZ / "scripts" / "conjuntura" / "dados" / "gabarito-boletins.yml"
CONSTRUTOR = RAIZ / "scripts" / "superset" / "build_boletim.py"

#: Colunas que expressam COMPARAÇÃO e por isso recebem cor por sinal. Uma
#: participação (`% MCMV`) não entra: ela nunca é negativa, e pintá-la de verde
#: sugeriria variação onde há proporção.
COMPARATIVA = re.compile(r"vs\.|12m|varia|var\b|x \d|acum", re.IGNORECASE)


def slug(texto: str) -> str:
    base = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", base.lower()).strip("_")[:44]


def estrutura_dos_quadros() -> list[dict]:
    """Página, seção, título e nota de cada quadro, lidos do construtor.

    A ordem do boletim vive em `build_boletim.py`, junto do SQL que materializa
    cada quadro. Redigitá-la aqui criaria uma segunda verdade que sai de
    sincronia no primeiro quadro novo.
    """
    fonte = CONSTRUTOR.read_text(encoding="utf-8")
    quadros = []
    for bloco in re.findall(r"Quadro\((.*?)\n        \)", fonte, re.S):

        def campo(nome: str) -> str:
            achado = re.search(rf'{nome}=(?:f?"""|")(.*?)(?:"""|")', bloco, re.S)
            return achado.group(1).strip() if achado else ""

        pagina = re.search(r"pagina=(\d+)", bloco)
        if not pagina:
            continue
        titulo = campo("titulo")
        quadros.append(
            {
                "pagina": int(pagina.group(1)),
                "secao": campo("secao"),
                "titulo": titulo,
                "nota": campo("nota"),
                "tabela": f"gld_boletim_p{pagina.group(1)}_{slug(titulo)}",
            }
        )
    return quadros


def ler_quadros(edicao: str, de_arquivo: pathlib.Path | None) -> dict[str, list[dict]]:
    """Lê os quadros do banco, ou de um despejo já feito."""
    if de_arquivo:
        do_arquivo: dict[str, list[dict]] = json.loads(
            de_arquivo.read_text(encoding="utf-8")
        )
        return do_arquivo
    import psycopg2
    import psycopg2.extras

    conexao = psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=os.environ["DB_DW_PORT_MCID"],
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
        connect_timeout=20,
    )
    cursor = conexao.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""select table_name from information_schema.tables
           where table_schema='conjuntura'
             and table_name like 'gld_boletim_p%' order by table_name""")
    saida = {}
    for linha in cursor.fetchall():
        nome = linha["table_name"]
        cursor.execute(f"select * from conjuntura.{nome} where edicao=%s", (edicao,))
        saida[nome] = [dict(r) for r in cursor.fetchall()]
    conexao.close()
    resultado: dict[str, list[dict]] = saida
    return resultado


def numero(valor: Any) -> float | None:
    if valor is None or valor == "":
        return None
    try:
        return float(str(valor).replace(",", "."))
    except ValueError:
        return None


def formatar(valor: Any) -> str:
    """Formata no padrão brasileiro, preservando o que não é número."""
    n = numero(valor)
    if n is None:
        return html.escape(str(valor)) if valor not in (None, "") else "–"
    if n == int(n) and abs(n) >= 1000:
        return f"{int(n):,}".replace(",", ".")
    if n == int(n):
        return str(int(n))
    return f"{n:,.1f}".replace(",", "@").replace(".", ",").replace("@", ".")


def celula(coluna: str, valor: Any, extras: str = "") -> str:
    """Uma célula, com cor por sinal apenas onde há comparação.

    `extras` são as pílulas de variação, que entram coladas ao número.
    """
    n = numero(valor)
    classes = ["v"]
    if n is not None:
        classes.append("num")
        if COMPARATIVA.search(coluna):
            classes.append("neg" if n < 0 else "pos" if n > 0 else "")
    conteudo = formatar(valor) + extras
    return f'<td class="{" ".join(c for c in classes if c)}">{conteudo}</td>'


#: Pílulas de variação, declaradas por quadro.
#:
#: No PPTX elas são caixas posicionadas à mão em cima do slide — não há regra
#: escrita em lugar nenhum dizendo o que cada uma compara. Aqui a regra é
#: explícita, e por isso a pílula recalcula quando o dado muda em vez de
#: continuar mostrando o número de junho.
#:
#: Dois formatos, porque o boletim usa os dois:
#:   `linhas`  compara a linha alvo com uma linha base, coluna a coluna
#:   `colunas` compara duas colunas dentro da MESMA linha
PILULAS: dict[str, dict[str, list[tuple[str, str, str]]]] = {
    "gld_boletim_p2_financiamentos_imobiliarios_bacen": {
        "linhas": [
            ("Mês de referência", "Mês anterior", "mês ant."),
            ("Mês de referência", "Mesmo mês do ano anterior", "ano ant."),
        ]
    },
    "gld_boletim_p2_financiamentos_habitacionais_uh": {
        "linhas": [
            ("Trimestre selecionado", "Trimestre anterior", "tri. ant."),
            ("Trimestre selecionado", "Mesmo trim. do ano anterior", "ano ant."),
        ]
    },
    "gld_boletim_p3_empregos_construcao_caged": {
        "linhas": [
            ("Mês de referência", "Mês anterior", "mês ant."),
            ("Mês de referência", "Mesmo mês do ano anterior", "ano ant."),
            ("Acumulado no trimestre", "Acum. no trim. do ano anterior", "ano ant."),
        ]
    },
    "gld_boletim_p3_pnad_continua_ocupados_e_rendimento_medio_re": {
        "linhas": [("jan-fev-mar 2026", "out-nov-dez 2025", "trim. ant.")]
    },
    "gld_boletim_p4_no_uh_por_condicao_de_uso": {
        "colunas": [
            (
                "Trim. selecionado — UH Usadas",
                "Trim. ano anterior — UH Usadas",
                "ano ant.",
            ),
            (
                "Trim. selecionado — UH Novas",
                "Trim. ano anterior — UH Novas",
                "ano ant.",
            ),
        ]
    },
    "gld_boletim_p5_financiamento_pf_mcmv_por_faixa": {
        "colunas": [
            ("Trim. selecionado — Nº UH", "Trim. ano anterior — Nº UH", "ano ant."),
            (
                "Trim. selecionado — FIN (Bi R$)",
                "Trim. ano anterior — FIN (Bi R$)",
                "ano ant.",
            ),
        ]
    },
}


def pilula(valor: float | None, base: str) -> str:
    """A pílula pequena colada ao número que ela qualifica."""
    if valor is None:
        return ""
    classe = "alta" if valor > 0 else "baixa" if valor < 0 else "neutra"
    sinal = "+" if valor > 0 else ""
    return (
        f'<span class="pil {classe}" title="ante {html.escape(base)}">'
        f"{sinal}{formatar(round(valor, 0))}%</span>"
    )


def pilulas_da_celula(
    quadro: str, linha: dict, rotulo: str, coluna: str, por_rotulo: dict
) -> str:
    """Todas as pílulas que essa célula recebe, nas duas formas de comparação."""
    regra = PILULAS.get(quadro)
    if not regra:
        return ""
    saida = ""
    for alvo, base, texto in regra.get("linhas", []):
        if rotulo == alvo and base in por_rotulo:
            saida += pilula(variacao(linha[coluna], por_rotulo[base].get(coluna)), texto)
    for alvo, base, texto in regra.get("colunas", []):
        if coluna == alvo:
            saida += pilula(variacao(linha[coluna], linha.get(base)), texto)
    return saida


def linhas_uteis(linhas: list[dict]) -> list[dict]:
    """Descarta a linha em que TODA célula de valor está vazia.

    O quadro do BACEN devolve 8 linhas, três delas sem nenhum valor — o
    impresso mostra 5. Uma linha só com o rótulo não é informação; é ruído que
    o leitor tenta interpretar.
    """
    uteis = []
    for linha in linhas:
        valores = [v for k, v in linha.items() if k != "edicao"][1:]
        if any(v not in (None, "") for v in valores):
            uteis.append(linha)
    return uteis


def tabela_html(quadro: dict, linhas: list[dict]) -> str:
    linhas = linhas_uteis(linhas)
    if not linhas:
        return (
            f'<div class="quadro"><h3>{html.escape(quadro["titulo"])}</h3>'
            '<p class="vazio">Sem dado para esta edição.</p></div>'
        )
    colunas = [c for c in linhas[0] if c != "edicao"]
    cabecalho = "".join(f"<th>{html.escape(c)}</th>" for c in colunas)
    por_rotulo = {str(linha[colunas[0]]): linha for linha in linhas}
    corpo = ""
    for linha in linhas:
        bruto = str(linha[colunas[0]])
        celulas = "".join(
            celula(
                coluna,
                linha[coluna],
                pilulas_da_celula(quadro["tabela"], linha, bruto, coluna, por_rotulo),
            )
            for coluna in colunas[1:]
        )
        corpo += f'<tr><th scope="row">{html.escape(bruto)}</th>{celulas}</tr>'
    nota = f'<p class="fonte">{html.escape(quadro["nota"])}</p>' if quadro["nota"] else ""
    return (
        f'<div class="quadro"><h3>{html.escape(quadro["titulo"])}</h3>'
        f'<div class="rolagem"><table><thead><tr>{cabecalho}</tr></thead>'
        f"<tbody>{corpo}</tbody></table></div>{nota}</div>"
    )


def grafico_credito(linhas: list[dict]) -> str:
    """Barras do crédito imobiliário sobre o PIB.

    A série varia 0,54 ponto em 16 meses: as barras ficam quase idênticas e é o
    rótulo que carrega a informação. A escala começa abaixo do mínimo, e não em
    zero, justamente para a variação ser visível — e o eixo diz isso.
    """
    if not linhas:
        return ""
    valores: list[tuple[str, float]] = []
    for linha in linhas:
        valor = numero(linha["Crédito Imobiliário / PIB"])
        if valor is not None:
            valores.append((str(linha["periodo"]), valor))
    if not valores:
        return ""
    minimo = min(v for _, v in valores)
    maximo = max(v for _, v in valores)
    piso = minimo - (maximo - minimo) * 0.6
    barras = ""
    for periodo, valor in valores:
        altura = (valor - piso) / (maximo - piso) * 100
        barras += (
            f'<div class="barra" style="--h:{altura:.1f}%">'
            f'<span class="rot">{formatar(valor)}</span>'
            f'<div class="haste"></div><span class="eixo">{html.escape(periodo)}</span>'
            "</div>"
        )
    return (
        '<div class="quadro grafico"><h3>Crédito Imobiliário / PIB (%)</h3>'
        f'<div class="barras">{barras}</div>'
        f'<p class="fonte">Fonte: BACEN. Escala começa em {formatar(round(piso, 2))}%, '
        "não em zero: a série varia menos de um ponto no período.</p></div>"
    )


def variacao(atual: Any, base: Any) -> float | None:
    a, b = numero(atual), numero(base)
    if a is None or b in (None, 0):
        return None
    return (a / b - 1) * 100


def cartao(titulo: str, pares: list[tuple[float | None, str]]) -> str:
    """Cartão de destaque: o número grande e contra o quê ele é.

    O boletim põe estes cartões ao lado da tabela que os origina. Sem a
    legenda dizendo a base de comparação, um percentual sozinho não significa
    nada — por isso ela vem junto e não some no design.
    """
    caixas = ""
    for valor, base in pares:
        if valor is None:
            continue
        classe = "alta" if valor > 0 else "baixa" if valor < 0 else "neutra"
        sinal = "+" if valor > 0 else ""
        caixas += (
            f'<div class="caixa {classe}"><b>{sinal}{formatar(round(valor, 1))}%</b>'
            f"<span>{html.escape(base)}</span></div>"
        )
    if not caixas:
        return ""
    return (
        f'<div class="cartao"><h4>{html.escape(titulo)}</h4>'
        f'<div class="caixas">{caixas}</div></div>'
    )


def cartoes_dos_totais(linhas: list[dict]) -> str:
    """Os quatro cartões da página 1, calculados da própria tabela."""
    por_periodo = {str(r["periodo"]): r for r in linhas}
    sel = por_periodo.get("Trimestre selecionado")
    ant = por_periodo.get("Trimestre anterior")
    ano = por_periodo.get("Mesmo trim. do ano anterior")
    if not (sel and ant and ano):
        return ""
    blocos = ""
    for rotulo, coluna in (
        ("Lançamentos totais", "Lançamentos TOTAL"),
        ("Lançamentos MCMV", "Lançamentos MCMV"),
        ("Vendas totais", "Vendas TOTAL"),
        ("Vendas MCMV", "Vendas MCMV"),
    ):
        blocos += cartao(
            rotulo,
            [
                (variacao(sel[coluna], ant[coluna]), "ante o trimestre anterior"),
                (variacao(sel[coluna], ano[coluna]), "ante o mesmo trim. de 2025"),
            ],
        )
    return f'<div class="cartoes">{blocos}</div>' if blocos else ""


def cartoes_das_empresas(linhas: list[dict]) -> str:
    """Os dois cartões da página 2 — a tabela já traz as variações prontas."""
    blocos = ""
    for linha in linhas:
        blocos += cartao(
            str(linha["indicador"]),
            [
                (numero(linha.get("vs. trim. anterior")), "ante o trimestre anterior"),
                (
                    numero(linha.get("12m atual / 12m anterior")),
                    "12 meses contra 12 meses",
                ),
            ],
        )
    return f'<div class="cartoes">{blocos}</div>' if blocos else ""


def data_br(iso: str) -> str:
    """AAAA-MM-DD vira DD/MM/AAAA, que é como o boletim escreve."""
    partes = str(iso).split("-")
    return "/".join(reversed(partes)) if len(partes) == 3 else html.escape(str(iso))


def bloco_leitura(texto: str) -> str:
    return f'<aside class="leitura">{html.escape(texto)}</aside>' if texto else ""


#: A folha de estilo vive num arquivo próprio: é a identidade visual do
#: boletim, e mantê-la como literal no Python a deixava ilegível e fora do
#: alcance de qualquer ferramenta de CSS.
ESTILO = (pathlib.Path(__file__).parent / "boletim.css").read_text(encoding="utf-8")


def cabecalho_fixo(edicao: str) -> str:
    """A barra de identificação, uma vez e fixa.

    O PPTX a repete em toda página porque cada página é um slide. Numa página
    de rolagem contínua, repetir seis vezes é pensar em slide; fixá-la no topo
    entrega a mesma informação — quem está lendo sabe o que está lendo.
    """
    return (
        '<div class="topo"><b>Conjuntura do Setor Habitacional</b>'
        f"<span>{html.escape(edicao)}</span></div>"
    )


def montar(
    edicao: str, quadros: dict, dados: dict, editorial: dict, validacao: list
) -> str:
    meta = editorial["edicoes"][edicao]
    leitura = meta.get("leitura") or {}
    divergentes = [v for v in validacao if v["status"] == "DIVERGE"]

    partes = [
        '<div class="capa"><div class="folha">',
        '<p class="orgao">Secretaria Nacional de Habitação</p>',
        "<h1>Conjuntura do<b>Setor Habitacional</b></h1>",
        f'<p><span class="selo">{html.escape(edicao)}</span></p>',
        f'<p class="data">{html.escape(meta["titulo"])} — publicado em '
        f'{data_br(meta["publicado_em"])}</p>',
        "</div></div>",
        cabecalho_fixo(edicao),
    ]

    if divergentes:
        itens = "".join(
            f"<li><b>{html.escape(v['indicador'])}</b> — boletim publicado: "
            f"{formatar(v['esperado'])}; apurado agora: {formatar(v['obtido'])}</li>"
            for v in divergentes
        )
        partes.append(
            f'<div class="folha"><div class="aviso"><h3>'
            f"{len(divergentes)} de {len(validacao)} células divergem do "
            "boletim publicado</h3>"
            "<p>Fontes como BACEN, IBGE, CAGED e FipeZap revisam séries passadas. "
            "Divergência aqui não indica erro de apuração — indica que o dado mudou "
            "depois da publicação.</p>"
            f"<ul>{itens}</ul></div></div>"
        )

    por_pagina: dict[int, list[dict]] = {}
    for q in quadros:
        por_pagina.setdefault(q["pagina"], []).append(q)

    # A leitura é ancorada no QUADRO que ela comenta, não na seção: uma seção
    # pode ter vários quadros, e o texto se refere a um deles.
    leitura_do_quadro = {
        "gld_boletim_p1_pib_construcao_civil_em_de_crescimento": "pib",
        "gld_boletim_p1_cbic_lancamentos_e_vendas_totais": "lancamentos_e_vendas",
        "gld_boletim_p3_novos_financiamentos_imobiliarios_por_banco_": "credito",
        "gld_boletim_p4_credito_imobiliario_pib": "credito_pib",
        "gld_boletim_p4_no_uh_por_condicao_de_uso": "condicao_de_uso",
        "gld_boletim_p5_saldo_caderneta_de_poupanca_captacao_liquida": (
            "poupanca_e_financiamento"
        ),
        "gld_boletim_p5_financiamento_pf_mcmv_por_faixa": "financiamento_pf_mcmv",
        "gld_boletim_p6_sinapi_brasil_e_incc_m": "precos",
        "gld_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc": "ticket_medio",
    }

    # `secao_atual` vive FORA do laço de página: no PPTX a seção "6. Crédito"
    # começa numa página e continua na seguinte, sem repetir o título. Reiniciar
    # a cada página imprimia o cabeçalho duas vezes.
    secao_atual = None
    for pagina in sorted(por_pagina):
        partes.append(f'<div class="folha pagina"><span class="np">{pagina}</span>')
        for q in por_pagina[pagina]:
            if q["secao"] != secao_atual:
                secao_atual = q["secao"]
                numero_secao, _, titulo_secao = secao_atual.partition(". ")
                partes.append(
                    f'<h2><span class="n">{html.escape(numero_secao)}.</span>'
                    f"{html.escape(titulo_secao)}</h2>"
                )
            partes.append('<div class="grade">')
            linhas = dados.get(q["tabela"], [])
            if q["tabela"] == "gld_boletim_p4_credito_imobiliario_pib":
                partes.append(grafico_credito(linhas))
            else:
                partes.append(tabela_html(q, linhas))
            if q["tabela"] == "gld_boletim_p1_cbic_lancamentos_e_vendas_totais":
                partes.append(cartoes_dos_totais(linhas))
            elif q["tabela"] == "gld_boletim_p2_totais_das_empresas_levantadas_variacao":
                partes.append(cartoes_das_empresas(linhas))
            partes.append("</div>")
            chave = leitura_do_quadro.get(q["tabela"])
            if chave and leitura.get(chave):
                partes.append(bloco_leitura(leitura[chave]))
        partes.append("</div>")

    partes.append(fechamento(meta))
    partes.append(
        '<div class="rodape">Quadros lidos das tabelas <code>gld_boletim_*</code>, '
        "as mesmas que alimentam o dashboard do Superset. Texto editorial transcrito "
        f"de <i>{html.escape(meta.get('fonte_da_transcricao', 'boletim publicado'))}</i>."
        "</div>"
    )
    return "\n".join(p for p in partes if p)


def fechamento(meta: dict) -> str:
    """Dados posteriores, expectativas e visão — nunca gerados."""
    blocos = []
    posteriores = meta.get("dados_posteriores") or {}
    if posteriores:
        colunas = ""
        for chave, rotulo, classe in (
            ("positivos", "Positivos", "pos-t"),
            ("neutros", "Neutros", "neu-t"),
            ("negativos", "Negativos", "neg-t"),
        ):
            itens = "".join(
                f"<li>{html.escape(i)}</li>" for i in posteriores.get(chave, [])
            )
            if itens:
                colunas += (
                    f'<div><h4 class="{classe}">{rotulo}</h4><ul>{itens}</ul></div>'
                )
        blocos.append(
            f'<h2><span class="n">9.</span>Dados posteriores</h2>'
            f'<div class="listas">{colunas}</div>'
        )
    exp = meta.get("expectativas_de_mercado") or {}
    if exp:
        linhas = "".join(
            f"<li><b>{html.escape(i['item'])}</b>: {html.escape(i['valor'])} "
            f"({html.escape(i['variacao'])})</li>"
            for i in exp.get("abecip_financiamento_2026", [])
        )
        pib = "".join(
            f"<li><b>{html.escape(i['fonte'])}</b>: {html.escape(i['valor'])}</li>"
            for i in exp.get("pib_da_construcao_2026", [])
        )
        blocos.append(
            '<h2><span class="n">10.</span>Expectativas de mercado</h2>'
            f'<div class="listas"><div><h4>ABECIP — financiamento 2026</h4>'
            f"<ul>{linhas}</ul></div>"
            f"<div><h4>PIB da construção 2026</h4><ul>{pib}</ul></div></div>"
        )
    visao = meta.get("visao_mcid") or {}
    if visao:
        fatores = "".join(f"<li>{html.escape(f)}</li>" for f in visao.get("fatores", []))
        incompleto = (
            '<p class="fonte">Parte desta seção não pôde ser transcrita do arquivo de '
            "origem e foi omitida em vez de completada por dedução.</p>"
            if visao.get("incompleto")
            else ""
        )
        blocos.append(
            '<h2><span class="n">11.</span>Visão MCid</h2>'
            f'<div class="listas"><div><h4>PIB Construção 2026: '
            f'{html.escape(visao.get("pib_construcao_2026", "–"))}</h4>'
            f"<ul>{fatores}</ul></div>"
            f"<div><h4>Crédito Imobiliário / PIB</h4><ul><li>"
            f'{html.escape(visao.get("credito_imobiliario_pib", "–"))}</li></ul>'
            f"{incompleto}</div></div>"
        )
    if not blocos:
        return ""
    return '<div class="folha">' + "".join(blocos) + "</div>"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--edicao", required=True)
    p.add_argument("--quadros", type=pathlib.Path, help="despejo JSON já feito")
    p.add_argument("--validacao", type=pathlib.Path)
    p.add_argument("--saida", type=pathlib.Path, required=True)
    args = p.parse_args()

    editorial = yaml.safe_load(EDITORIAL.read_text(encoding="utf-8"))
    if args.edicao not in editorial["edicoes"]:
        raise SystemExit(
            f"Edição {args.edicao} não está em {EDITORIAL.name}. O texto editorial não "
            "é gerado: sem ele a página não sai."
        )
    dados = ler_quadros(args.edicao, args.quadros)
    validacao = (
        json.loads(args.validacao.read_text(encoding="utf-8")) if args.validacao else []
    )
    corpo = montar(args.edicao, estrutura_dos_quadros(), dados, editorial, validacao)

    pagina = (
        f"<title>Conjuntura do Setor Habitacional {args.edicao}</title>\n"
        '<link rel="preconnect" href="https://fonts.googleapis.com">\n'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n'
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
        "family=Archivo:wght@500;800&family=Source+Sans+3:wght@400;600;700"
        '&display=swap">\n'
        f"<style>{ESTILO}</style>\n{corpo}\n"
    )
    args.saida.write_text(pagina, encoding="utf-8")
    print(f"Página escrita em {args.saida} ({len(pagina):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
