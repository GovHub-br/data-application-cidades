#!/usr/bin/env python3
"""Gera o Catálogo de Fontes do Boletim de Conjuntura.

Um bloco por quadro do boletim: de onde o dado vem, com que frequência, por que
caminho entra e se a coleta está automatizada. O conteúdo vive em
`scripts/conjuntura/dados/catalogo-fontes.yml`; aqui só se posiciona.

O catálogo circulava como PDF solto, sem gerador: mudava a cada automação
concluída e ninguém sabia refazê-lo. Versionar o conteúdo devolve isso — e
resolve o que motivou a regeração: a URL da fonte era texto morto no PDF, e
quem lia tinha de copiar à mão. Agora é link.

Uso:
    python scripts/conjuntura/gerar_catalogo_fontes.py --saida catalogo.html
"""

from __future__ import annotations

import argparse
import html
import pathlib
from typing import Any

import yaml

RAIZ = pathlib.Path(__file__).resolve().parents[2]
DADOS = RAIZ / "scripts" / "conjuntura" / "dados" / "catalogo-fontes.yml"

#: Ordem impressa dos campos de um bloco. `link` não entra: ele é renderizado
#: junto de FONTE, porque é a mesma informação — a origem e como chegar nela.
CAMPOS = [
    ("frequencia", "Frequência"),
    ("fonte", "Fonte"),
    ("tipo", "Tipo"),
    ("insercao", "Inserção"),
    ("situacao", "Situação"),
    ("dado_coletado", "Dado coletado"),
]

ESTILO = """
:root{
  --ambar:#F5B800; --ambar-forte:#D99E00; --creme:#FFF8E7; --papel:#FFFDF8;
  --barra:#E8E4DC; --tinta:#1A1A1A; --tinta-fraca:#5A544A;
  --auto:#00843D; --auto-fundo:#C6EFCE; --manual:#8A5A00; --manual-fundo:#FFF0CC;
  --link:#1F4E79;
}
:root:not([data-theme="light"]){ @media (prefers-color-scheme: dark){
  --ambar:#F5B800; --ambar-forte:#8A6A00; --creme:#22201C; --papel:#161512;
  --barra:#2A2823; --tinta:#F0EDE6; --tinta-fraca:#B0A99C;
  --auto:#5BD98C; --auto-fundo:#123A22; --manual:#FFD27F; --manual-fundo:#3A2E10;
  --link:#8FB8DE;
}}
:root[data-theme="dark"]{
  --ambar:#F5B800; --ambar-forte:#8A6A00; --creme:#22201C; --papel:#161512;
  --barra:#2A2823; --tinta:#F0EDE6; --tinta-fraca:#B0A99C;
  --auto:#5BD98C; --auto-fundo:#123A22; --manual:#FFD27F; --manual-fundo:#3A2E10;
  --link:#8FB8DE;
}
*{box-sizing:border-box}
body{margin:0;background:var(--papel);color:var(--tinta);
  font-family:"Source Sans 3","Segoe UI",system-ui,sans-serif;font-size:15px;line-height:1.5}
.folha{max-width:980px;margin:0 auto;padding:0 1.25rem 3rem}
.capa{padding:2.5rem 1.25rem 1.5rem;border-bottom:6px solid var(--ambar);margin-bottom:1.8rem}
.capa .orgao{font-size:.78rem;font-weight:700;letter-spacing:.09em;
  text-transform:uppercase;color:var(--tinta-fraca)}
.capa h1{font-family:Archivo,Impact,sans-serif;font-size:clamp(1.8rem,4.5vw,2.7rem);
  line-height:1.05;margin:.35rem 0 .5rem;font-weight:800;text-wrap:balance}
.capa p{color:var(--tinta-fraca);font-size:.92rem;margin:.3rem 0;max-width:64ch}
.resumo{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:1rem}
.resumo div{border:1px solid var(--ambar-forte);border-radius:3px;padding:.5rem .8rem;
  display:flex;align-items:baseline;gap:.5rem}
.resumo b{font-family:Archivo,sans-serif;font-size:1.5rem;font-variant-numeric:tabular-nums}
.resumo span{font-size:.72rem;letter-spacing:.05em;text-transform:uppercase;color:var(--tinta-fraca)}
.resumo .a b{color:var(--auto)} .resumo .m b{color:var(--manual)}
h2{font-family:Archivo,sans-serif;font-size:1.05rem;letter-spacing:.03em;text-transform:uppercase;
  margin:2.2rem 0 .9rem;padding-bottom:.4rem;border-bottom:2px solid var(--ambar);text-wrap:balance}
h2 .n{color:var(--tinta-fraca);margin-right:.4rem}
.bloco{background:var(--creme);border:1px solid var(--ambar-forte);border-radius:3px;
  padding:.9rem 1rem;margin-bottom:.9rem}
.cab{display:flex;align-items:baseline;gap:.6rem;flex-wrap:wrap;margin-bottom:.6rem}
.cab h3{font-family:Archivo,sans-serif;font-size:.98rem;margin:0;text-wrap:balance}
.cab .num{font-family:Archivo,sans-serif;font-size:.76rem;font-weight:800;
  color:var(--tinta-fraca);letter-spacing:.06em}
.selo{font-size:.66rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;
  padding:.12rem .45rem;border-radius:2px;white-space:nowrap}
.selo.a{background:var(--auto-fundo);color:var(--auto)}
.selo.m{background:var(--manual-fundo);color:var(--manual)}
dl{display:grid;grid-template-columns:max-content 1fr;gap:.28rem .9rem;margin:0}
dt{font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;
  color:var(--tinta-fraca);padding-top:.12rem}
dd{margin:0;font-size:.88rem}
a{color:var(--link);text-decoration:underline;text-underline-offset:2px;word-break:break-word}
a:focus-visible{outline:2px solid var(--ambar-forte);outline-offset:2px}
.origem{font-size:.8rem;color:var(--tinta-fraca);font-family:ui-monospace,Menlo,monospace;
  word-break:break-all}
.nota{margin:.6rem 0 0;padding:.5rem .7rem;background:var(--barra);border-left:3px solid var(--ambar);
  font-size:.82rem;line-height:1.45}
.rodape{border-top:1px solid var(--barra);margin-top:2.5rem;padding-top:1rem;
  color:var(--tinta-fraca);font-size:.8rem}

@media print{
  .bloco,.nota{break-inside:avoid}
  h2{break-after:avoid}
  body{font-size:12.5px}
  /* O texto do link JÁ é o endereço, então nada de repeti-lo com `attr(href)`:
     no papel saía "https://… (https://…)". Quem imprime lê a URL uma vez; quem
     abre o PDF continua clicando nela. */
  a{color:var(--link)}
}
@page{size:A4;margin:14mm 12mm}
"""


def carregar() -> dict[str, Any]:
    dados: dict[str, Any] = yaml.safe_load(DADOS.read_text(encoding="utf-8"))
    return dados


def data_br(iso: str) -> str:
    partes = str(iso).split("-")
    return "/".join(reversed(partes)) if len(partes) == 3 else html.escape(str(iso))


def linha_fonte(bloco: dict) -> str:
    """FONTE e como chegar nela, juntas.

    Três casos: endereço navegável vira link; procedência que não é endereço
    (bucket, OCR interno) fica em monoespaçada, porque colar aquilo num
    navegador não leva a lugar nenhum; e fonte sem origem declarada só mostra
    o nome.
    """
    partes = [f"<dd>{html.escape(bloco['fonte'])}"]
    if bloco.get("link"):
        alvo = html.escape(bloco["link"], quote=True)
        partes.append(
            f'<br><a href="{alvo}" rel="noopener noreferrer" target="_blank">{alvo}</a>'
        )
    elif bloco.get("link_texto"):
        partes.append(
            f'<br><span class="origem">{html.escape(bloco["link_texto"])}</span>'
        )
    partes.append("</dd>")
    return "".join(partes)


def bloco_html(bloco: dict) -> str:
    auto = bloco["automatizado"]
    itens = ""
    for chave, rotulo in CAMPOS:
        if not bloco.get(chave):
            continue
        itens += f"<dt>{rotulo}</dt>"
        itens += (
            linha_fonte(bloco)
            if chave == "fonte"
            else (f"<dd>{html.escape(str(bloco[chave]))}</dd>")
        )
    notas = "".join(
        f'<p class="nota">{html.escape(n)}</p>' for n in bloco.get("notas", [])
    )
    return (
        f'<article class="bloco">'
        f'<div class="cab"><span class="num">Bloco {bloco["numero"]:02d}</span>'
        f"<h3>{html.escape(bloco['titulo'])}</h3>"
        f'<span class="selo {"a" if auto else "m"}">'
        f'{"Automatizado" if auto else "Não automatizado"}</span></div>'
        f"<dl>{itens}</dl>{notas}</article>"
    )


def montar(dados: dict) -> str:
    blocos = [b for p in dados["paginas"] for b in p["blocos"]]
    auto = sum(1 for b in blocos if b["automatizado"])
    corpo = [
        '<div class="capa">',
        '<p class="orgao">Ministério das Cidades · Secretaria Nacional de Habitação</p>',
        "<h1>Catálogo de Fontes de Dados</h1>",
        "<p>Boletim de Conjuntura do Setor Habitacional — inventário por bloco: origem, "
        "frequência, forma de coleta e situação da automação.</p>",
        f"<p>Posição de {data_br(dados['posicao'])} · {len(blocos)} blocos · "
        f"Validado contra os boletins publicados de "
        f"{', '.join(html.escape(e) for e in dados['validado_contra'])}.</p>",
        f'<div class="resumo"><div class="a"><b>{auto}</b>'
        "<span>Automatizados<br>o dado chega sem digitação</span></div>"
        f'<div class="m"><b>{len(blocos) - auto}</b>'
        "<span>Não automatizados<br>dependem de inserção manual</span></div></div>",
        "</div>",
        '<div class="folha">',
    ]
    for pagina in dados["paginas"]:
        corpo.append(
            f'<h2><span class="n">Página {pagina["numero"]}</span>'
            f"{html.escape(pagina['titulo'])}</h2>"
        )
        corpo += [bloco_html(b) for b in pagina["blocos"]]
    corpo.append(
        '<div class="rodape">Conteúdo em '
        "<code>scripts/conjuntura/dados/catalogo-fontes.yml</code>; esta página é gerada "
        "por <code>scripts/conjuntura/gerar_catalogo_fontes.py</code>. Ao concluir uma "
        "automação, altere o YAML e regere — não edite o PDF.</div>"
    )
    corpo.append("</div>")
    return "\n".join(corpo)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--saida", type=pathlib.Path, required=True)
    args = p.parse_args()

    dados = carregar()
    pagina = (
        "<title>Catálogo de Fontes do Boletim de Conjuntura</title>\n"
        '<link rel="preconnect" href="https://fonts.googleapis.com">\n'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n'
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
        'family=Archivo:wght@500;800&family=Source+Sans+3:wght@400;600;700&display=swap">\n'
        f"<style>{ESTILO}</style>\n" + montar(dados)
    )
    args.saida.parent.mkdir(parents=True, exist_ok=True)
    args.saida.write_text(pagina, encoding="utf-8")
    print(f"Catálogo escrito em {args.saida} ({len(pagina):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
