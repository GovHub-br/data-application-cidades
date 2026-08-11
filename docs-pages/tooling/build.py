"""
build.py: renderiza o site estatico em docs-pages/site/.

Roda offline: consome os JSONs em docs-pages/src/_data/ gravados pela coleta e
a curadoria em docs-pages/src/dominios.yml.

Uso:
    make docs-build
"""

import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import markdown
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from markupsafe import Markup, escape

from tooling import dados, graficos
from tooling.common import ASSETS_DIR, SITE_DIR, SRC_DIR, TEMPLATES_DIR, log, read_json

CORES_CAMADA = {"bronze": "#B45309", "silver": "#64748B", "gold": "#7A34F3"}

ABAS = [
    {
        "id": "gestao",
        "href": "gestao/index.html",
        "rotulo": "Gestão",
        "pergunta": "O que foi entregue e que problema resolveu?",
        "resumo": (
            "O histórico de entregas na linguagem de quem decide, com a evidência "
            "de cada uma."
        ),
    },
    {
        "id": "tecnico",
        "href": "tecnico/index.html",
        "rotulo": "Técnico",
        "pergunta": "Como funciona e como eu mexo nisso?",
        "resumo": ("Fontes, DAGs, modelos dbt e camadas, lidos direto do código."),
    },
    {
        "id": "vitrine",
        "href": "vitrine/index.html",
        "rotulo": "Vitrine",
        "pergunta": "Por que isso importa e como replicar?",
        "resumo": "A narrativa institucional do projeto e o caminho para outros órgãos.",
    },
]

PAGINAS: list[dict[str, Any]] = [
    {"saida": "index.html", "template": "index.html.j2", "aba": None, "titulo": "Início"},
    {
        "saida": "gestao/index.html",
        "template": "gestao.html.j2",
        "aba": "gestao",
        "titulo": "Gestão",
    },
    {
        "saida": "tecnico/index.html",
        "template": "tecnico.html.j2",
        "aba": "tecnico",
        "titulo": "Técnico",
    },
    {
        "saida": "vitrine/index.html",
        "template": "vitrine.html.j2",
        "aba": "vitrine",
        "titulo": "Vitrine",
    },
    {
        "saida": "entregas/index.html",
        "template": "entregas.html.j2",
        "aba": "gestao",
        "titulo": "Entregas",
    },
]


def _markdown(texto: str) -> Markup:
    """Renderiza o corpo de um PR.

    O texto e escapado antes de virar markdown: o corpo do PR e conteudo de
    terceiro e nao deve injetar HTML na pagina.
    """
    if not texto.strip():
        return Markup("")
    html = markdown.markdown(
        str(escape(texto)), extensions=["extra", "sane_lists"], output_format="html"
    )
    return Markup(html)


def _copiar_assets() -> None:
    destino = SITE_DIR / "assets"
    if destino.exists():
        shutil.rmtree(destino)
    shutil.copytree(ASSETS_DIR, destino)


def _validar_markup(paginas: list[Path]) -> int:
    """Detecta markup que vazou escapado para a pagina, em vez de renderizar.

    O autoescape do Jinja e o comportamento certo para texto do acervo, mas
    engole SVG e HTML gerados que nao venham como Markup. O sintoma e a pagina
    imprimir o codigo-fonte do grafico.
    """
    problemas = 0
    for pagina in paginas:
        html = pagina.read_text(encoding="utf-8")
        for vazamento in ("&lt;svg", "&lt;div"):
            if vazamento in html:
                log.error("markup escapado em %s: %s", pagina.name, vazamento)
                problemas += 1
    return problemas


def _validar_links(paginas: list[Path]) -> int:
    """Confere se todo href/src relativo aponta para um arquivo existente."""
    padrao = re.compile(r'(?:href|src)="([^"#:]+)"')
    quebrados = 0
    for pagina in paginas:
        html = pagina.read_text(encoding="utf-8")
        for alvo in padrao.findall(html):
            if alvo.startswith(("http", "//", "mailto:")):
                continue
            if not (pagina.parent / alvo).resolve().exists():
                log.error("link quebrado em %s: %s", pagina, alvo)
                quebrados += 1
    return quebrados


def _contexto(acervo: dict[str, Any]) -> dict[str, Any]:
    """Monta tudo que os templates consomem."""
    git, dbt, airflow = acervo["git"], acervo["dbt"], acervo["airflow"]
    curadoria = dados.carregar_curadoria()
    escopo = curadoria["escopo"]

    modelos = dados.modelos_do_escopo(dbt, escopo)
    dags = dados.dags_do_escopo(airflow, escopo)
    dominios = dados.montar_dominios(curadoria, modelos, dags, git.get("entregas", []))
    entregas = dados.entregas_do_escopo(git, dominios)
    metricas = dados.metricas(dominios, dags, entregas, escopo, git)
    rotulo_periodo, _ = dados.periodo(git.get("resumo", {}))

    camadas = dados.camadas_de(modelos)
    por_dominio = [(d["rotulo"], d["total"]) for d in dominios]

    return {
        "abas": ABAS,
        "escopo": escopo,
        "m": metricas,
        "dominios": dominios,
        "entregas": entregas,
        "entregas_por_trimestre": dados.agrupar_por_trimestre(entregas),
        "dags": sorted(dags, key=lambda d: d["dag_id"]),
        "periodo": rotulo_periodo,
        "data_fork": dados.formatar_data(git.get("data_fork")),
        "gerado_em": datetime.now().strftime("%d/%m/%Y às %H:%M"),
        "grafico_camadas": graficos.barras_horizontais(
            [(c, camadas[c]) for c in camadas], CORES_CAMADA
        ),
        "grafico_dominios": graficos.barras_horizontais(por_dominio),
        "descricao": (
            "Roadmap e documentação da aplicação de dados do Ministério das Cidades."
        ),
    }


def _paginas(contexto: dict[str, Any]) -> list[dict[str, Any]]:
    """Paginas fixas mais uma por dominio."""
    paginas = list(PAGINAS)
    for dominio in contexto["dominios"]:
        paginas.append(
            {
                "saida": f"dominios/{dominio['slug']}/index.html",
                "template": "dominio.html.j2",
                "aba": None,
                "titulo": dominio["rotulo"],
                "extra": {"d": dominio},
            }
        )
    return paginas


def main() -> int:
    acervo = {
        "git": read_json("git_pr"),
        "dbt": read_json("dbt"),
        "airflow": read_json("airflow"),
    }
    if not all(acervo.values()):
        log.error("acervo incompleto em docs-pages/src/_data — rode 'make docs-collect'")
        return 1
    if not (SRC_DIR / "dominios.yml").exists():
        log.error("falta docs-pages/src/dominios.yml")
        return 1

    contexto = _contexto(acervo)

    ambiente = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=True,
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    ambiente.filters["markdown"] = _markdown
    ambiente.filters["data"] = dados.formatar_data

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True)

    escritas: list[Path] = []
    for pagina in _paginas(contexto):
        saida = SITE_DIR / str(pagina["saida"])
        saida.parent.mkdir(parents=True, exist_ok=True)
        profundidade = len(Path(str(pagina["saida"])).parts) - 1
        html = ambiente.get_template(str(pagina["template"])).render(
            **contexto,
            **pagina.get("extra", {}),
            rel="../" * profundidade,
            aba_atual=pagina["aba"],
            titulo=pagina["titulo"],
        )
        saida.write_text(html, encoding="utf-8")
        escritas.append(saida)

    _copiar_assets()

    falhas = _validar_links(escritas) + _validar_markup(escritas)
    if falhas:
        log.error("%d problema(s) na saida; site nao publicavel", falhas)
        return 1

    log.info("site pronto em %s (%d páginas)", SITE_DIR, len(escritas))
    return 0


if __name__ == "__main__":
    sys.exit(main())
