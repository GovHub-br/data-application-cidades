"""
build.py: renderiza o site estatico em site/ a partir do acervo e dos templates.

Roda offline: consome apenas os JSONs em docs-pages/src/_data/ gravados pela coleta.

Uso:
    make docs-build
"""

import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from tooling import graficos
from tooling.common import (
    ASSETS_DIR,
    SITE_DIR,
    TEMPLATES_DIR,
    log,
    read_json,
)

CORES_CAMADA = {"bronze": "#B45309", "silver": "#64748B", "gold": "#7A34F3"}
MESES = {
    1: "jan",
    2: "fev",
    3: "mar",
    4: "abr",
    5: "mai",
    6: "jun",
    7: "jul",
    8: "ago",
    9: "set",
    10: "out",
    11: "nov",
    12: "dez",
}

ABAS = [
    {
        "id": "gestao",
        "href": "gestao/index.html",
        "rotulo": "Gestão",
        "pergunta": "O que foi entregue e que problema resolveu?",
        "resumo": (
            "O roadmap na linguagem de quem decide, com a evidência de cada entrega."
        ),
    },
    {
        "id": "tecnico",
        "href": "tecnico/index.html",
        "rotulo": "Técnico",
        "pergunta": "Como funciona e como eu mexo nisso?",
        "resumo": (
            "Fontes, DAGs, modelos dbt, camadas e linhagem, lidos direto do código."
        ),
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
]


ROTULOS_DOMINIO = {
    "conjuntura": "Conjuntura",
    "contratos": "Contratos",
    "dados_abertos": "Dados Abertos",
    "emendas": "Emendas",
    "empenhos_ted": "Empenhos TED",
    "empreendimento_far": "Empreendimento FAR",
    "entidades": "Entidades",
    "orcamento": "Orçamento",
    "pessoas": "Pessoas",
    "ted": "TED",
}


def _rotulo_dominio(slug: str) -> str:
    """Rotulo de exibicao do dominio, com acentos e siglas em caixa alta."""
    return ROTULOS_DOMINIO.get(slug, slug.replace("_", " ").title())


def _dominios(dbt: dict[str, Any]) -> list[dict[str, Any]]:
    """Agrega os modelos dbt por dominio, ignorando a pasta tecnica metadata."""
    acumulado: dict[str, dict[str, Any]] = {}
    for modelo in dbt.get("modelos", []):
        slug = modelo["dominio"]
        if slug == "metadata":
            continue
        item = acumulado.setdefault(
            slug,
            {
                "slug": slug,
                "rotulo": _rotulo_dominio(slug),
                "projeto": modelo["projeto"],
                "total": 0,
                "testes": 0,
                "camadas": {},
            },
        )
        item["total"] += 1
        item["testes"] += modelo["testes"]
        camada = modelo["camada"]
        item["camadas"][camada] = item["camadas"].get(camada, 0) + 1

    for item in acumulado.values():
        item["camadas"] = {
            camada: item["camadas"][camada]
            for camada in ("bronze", "silver", "gold", "outros")
            if camada in item["camadas"]
        }
    return sorted(acumulado.values(), key=lambda d: -d["total"])


def _clientes_com_dags(airflow: dict[str, Any]) -> list[dict[str, Any]]:
    """Cruza clientes de plugins com a quantidade de DAGs que os referenciam."""
    contagem: dict[str, int] = {}
    for dag in airflow.get("dags", []):
        grupo = dag["grupo"].split("/")[-1]
        contagem[grupo] = contagem.get(grupo, 0) + 1

    clientes = []
    for cliente in airflow.get("clientes", []):
        sistema = cliente["sistema"]
        clientes.append({**cliente, "dags": contagem.get(sistema, 0)})
    return sorted(clientes, key=lambda c: (-c["dags"], c["sistema"]))


def _por_trimestre(commits: list[dict[str, Any]]) -> list[tuple[str, int]]:
    contagem: dict[str, int] = {}
    for commit in commits:
        data = datetime.fromisoformat(commit["data"])
        chave = f"{str(data.year)[2:]}T{(data.month - 1) // 3 + 1}"
        contagem[chave] = contagem.get(chave, 0) + 1
    return sorted(contagem.items())


def _periodo(resumo: dict[str, Any]) -> tuple[str, int]:
    primeiro = resumo.get("primeiro_commit")
    ultimo = resumo.get("ultimo_commit")
    if not primeiro or not ultimo:
        return "período indefinido", 0
    inicio = datetime.fromisoformat(primeiro)
    fim = datetime.fromisoformat(ultimo)
    meses = (fim.year - inicio.year) * 12 + fim.month - inicio.month
    rotulo = f"{MESES[inicio.month]}/{inicio.year} — {MESES[fim.month]}/{fim.year}"
    return rotulo, meses


def _metricas(
    git: dict[str, Any],
    dbt: dict[str, Any],
    airflow: dict[str, Any],
    dominios: list[dict[str, Any]],
) -> dict[str, Any]:
    resumo_dbt = dbt.get("resumo", {})
    resumo_git = git.get("resumo", {})
    resumo_air = airflow.get("resumo", {})
    _, meses = _periodo(resumo_git)
    return {
        "total_modelos": resumo_dbt.get("total_modelos", 0),
        "total_testes": resumo_dbt.get("total_testes", 0),
        "total_dominios": len(dominios),
        "total_projetos": len(resumo_dbt.get("por_projeto", {})),
        "total_gold": resumo_dbt.get("por_camada", {}).get("gold", 0),
        "total_dags": resumo_air.get("total_dags", 0),
        "total_clientes": resumo_air.get("total_clientes", 0),
        "total_fontes_sistemas": resumo_air.get("total_clientes", 0),
        "total_commits": resumo_git.get("total_commits", 0),
        "total_prs": resumo_git.get("total_prs", 0),
        "total_autores": resumo_git.get("total_autores", 0),
        "meses_projeto": meses,
    }


def _prs_recentes(git: dict[str, Any], limite: int = 10) -> list[dict[str, Any]]:
    mergeados = [pr for pr in git.get("prs", []) if pr.get("mergeado_em")]
    mergeados.sort(key=lambda pr: pr["mergeado_em"], reverse=True)
    recentes = []
    for pr in mergeados[:limite]:
        data = datetime.fromisoformat(pr["mergeado_em"].replace("Z", "+00:00"))
        recentes.append(
            {
                "numero": pr["numero"],
                "titulo": pr["titulo"],
                "url": pr["url"],
                "data": f"{data.day:02d}/{MESES[data.month]}/{data.year}",
            }
        )
    return recentes


def _copiar_assets() -> None:
    destino = SITE_DIR / "assets"
    if destino.exists():
        shutil.rmtree(destino)
    shutil.copytree(ASSETS_DIR, destino)


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
                log.error("link quebrado em %s: %s", pagina.name, alvo)
                quebrados += 1
    return quebrados


def main() -> int:
    git = read_json("git_pr")
    dbt = read_json("dbt")
    airflow = read_json("airflow")

    if not (git and dbt and airflow):
        log.error("acervo incompleto em docs-pages/src/_data — rode 'make docs-collect'")
        return 1

    dominios = _dominios(dbt)
    metricas = _metricas(git, dbt, airflow, dominios)
    rotulo_periodo, _ = _periodo(git.get("resumo", {}))

    camadas = dbt.get("resumo", {}).get("por_camada", {})
    dados_camadas = [
        (camada, camadas[camada])
        for camada in ("bronze", "silver", "gold", "outros")
        if camada in camadas
    ]

    ambiente = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=True,
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    contexto_comum: dict[str, Any] = {
        "abas": ABAS,
        "m": metricas,
        "dominios": dominios,
        "clientes": _clientes_com_dags(airflow),
        "prs_recentes": _prs_recentes(git),
        "periodo": rotulo_periodo,
        "gerado_em": datetime.now().strftime("%d/%m/%Y às %H:%M"),
        "grafico_camadas": graficos.barras_horizontais(dados_camadas, CORES_CAMADA),
        "grafico_trimestres": graficos.colunas(_por_trimestre(git.get("commits", []))),
        "descricao": (
            "Roadmap e documentação da aplicação de dados Cidades do Gov Hub BR."
        ),
    }

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True)

    escritas: list[Path] = []
    for pagina in PAGINAS:
        saida = SITE_DIR / str(pagina["saida"])
        saida.parent.mkdir(parents=True, exist_ok=True)
        profundidade = len(Path(str(pagina["saida"])).parts) - 1
        html = ambiente.get_template(str(pagina["template"])).render(
            **contexto_comum,
            rel="../" * profundidade,
            aba_atual=pagina["aba"],
            titulo=pagina["titulo"],
        )
        saida.write_text(html, encoding="utf-8")
        escritas.append(saida)
        log.info("%s", saida.relative_to(SITE_DIR.parent))

    _copiar_assets()

    quebrados = _validar_links(escritas)
    if quebrados:
        log.error("%d link(s) interno(s) quebrado(s)", quebrados)
        return 1

    log.info("site pronto em %s (%d páginas)", SITE_DIR, len(escritas))
    return 0


if __name__ == "__main__":
    sys.exit(main())
