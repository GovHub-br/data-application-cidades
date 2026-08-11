"""
graficos.py: gera SVG estatico para os graficos do site.

Sem biblioteca de charts: sao poucos tipos, e SVG inline imprime, vira PDF e
nao depende de JavaScript.
"""

from xml.sax.saxutils import escape

LARGURA = 760
ALTURA_BARRA = 26
ESPACO = 12
MARGEM_ROTULO = 132
COR_PADRAO = "#7A34F3"


def barras_horizontais(
    dados: list[tuple[str, int]], cores: dict[str, str] | None = None
) -> str:
    """Barras horizontais com rotulo a esquerda e valor na ponta."""
    if not dados:
        return '<p class="secao__intro">Sem dados para exibir.</p>'

    cores = cores or {}
    maximo = max(valor for _, valor in dados) or 1
    altura = len(dados) * (ALTURA_BARRA + ESPACO) + ESPACO
    util = LARGURA - MARGEM_ROTULO - 56

    partes = [
        f'<svg viewBox="0 0 {LARGURA} {altura}" role="img" '
        f'aria-label="Grafico de barras" xmlns="http://www.w3.org/2000/svg">'
    ]
    for indice, (rotulo, valor) in enumerate(dados):
        y = ESPACO + indice * (ALTURA_BARRA + ESPACO)
        largura = max(2, round(util * valor / maximo))
        cor = cores.get(rotulo, COR_PADRAO)
        meio = y + ALTURA_BARRA / 2 + 5
        partes.append(
            f'<text x="{MARGEM_ROTULO - 10}" y="{meio}" text-anchor="end" '
            f'font-size="13" font-weight="600" fill="#2D3748">{escape(rotulo)}</text>'
            f'<rect x="{MARGEM_ROTULO}" y="{y}" width="{largura}" '
            f'height="{ALTURA_BARRA}" rx="5" fill="{cor}" />'
            f'<text x="{MARGEM_ROTULO + largura + 8}" y="{meio}" font-size="13" '
            f'font-weight="700" fill="#666666">{valor}</text>'
        )
    partes.append("</svg>")
    return "".join(partes)


def colunas(dados: list[tuple[str, int]], cor: str = COR_PADRAO) -> str:
    """Colunas verticais para series temporais curtas."""
    if not dados:
        return '<p class="secao__intro">Sem dados para exibir.</p>'

    altura = 240
    base = altura - 34
    maximo = max(valor for _, valor in dados) or 1
    largura_col = LARGURA / len(dados)
    corpo = min(46.0, largura_col * 0.62)

    partes = [
        f'<svg viewBox="0 0 {LARGURA} {altura}" role="img" '
        f'aria-label="Grafico de colunas" xmlns="http://www.w3.org/2000/svg">',
        f'<line x1="0" y1="{base}" x2="{LARGURA}" y2="{base}" stroke="#E5E7EB" />',
    ]
    for indice, (rotulo, valor) in enumerate(dados):
        centro = largura_col * (indice + 0.5)
        h = max(2, round((base - 26) * valor / maximo))
        x = centro - corpo / 2
        partes.append(
            f'<rect x="{x:.1f}" y="{base - h}" width="{corpo:.1f}" height="{h}" '
            f'rx="4" fill="{cor}" />'
            f'<text x="{centro:.1f}" y="{base - h - 6}" text-anchor="middle" '
            f'font-size="12" font-weight="700" fill="#666666">{valor}</text>'
            f'<text x="{centro:.1f}" y="{base + 18}" text-anchor="middle" '
            f'font-size="11" fill="#666666">{escape(rotulo)}</text>'
        )
    partes.append("</svg>")
    return "".join(partes)
