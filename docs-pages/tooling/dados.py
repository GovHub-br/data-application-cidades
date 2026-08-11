"""
dados.py: monta o modelo de dados do site a partir do acervo e da curadoria.

Separa a agregacao da renderizacao: aqui nada sabe de HTML, e o build so
recebe estruturas prontas. O recorte do escopo (MCID) acontece nesta camada —
tudo que nao pertence ao escopo e descartado antes de chegar ao template.
"""

import re
from datetime import datetime
from typing import Any

import yaml

from tooling.common import SRC_DIR, log

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
CAMADAS = ("bronze", "silver", "gold", "outros")


def carregar_curadoria() -> dict[str, Any]:
    """Le docs-pages/src/dominios.yml — escopo e contexto escritos a mao."""
    arquivo = SRC_DIR / "dominios.yml"
    dados: dict[str, Any] = yaml.safe_load(arquivo.read_text(encoding="utf-8"))
    return dados


def _casa(texto: str, chaves: list[str]) -> bool:
    """Uma chave curta precisa casar palavra inteira; sigla como FAR erra facil."""
    for chave in chaves:
        if len(chave) <= 4:
            if re.search(rf"\b{re.escape(chave)}\b", texto):
                return True
        elif chave in texto:
            return True
    return False


def _texto_da_entrega(entrega: dict[str, Any]) -> str:
    partes = [
        entrega["titulo"],
        entrega["corpo"],
        entrega["referencia"] or "",
        " ".join(entrega["labels"]),
    ]
    return " ".join(partes).lower()


def formatar_data(iso: str | None) -> str:
    if not iso:
        return ""
    data = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return f"{data.day:02d}/{MESES[data.month]}/{data.year}"


def _trimestre(iso: str) -> str:
    data = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return f"{data.year}T{(data.month - 1) // 3 + 1}"


def modelos_do_escopo(dbt: dict[str, Any], escopo: dict[str, Any]) -> list[Any]:
    projetos = set(escopo["projetos_dbt"])
    return [
        m
        for m in dbt.get("modelos", [])
        if m["projeto"] in projetos and m["dominio"] != "metadata"
    ]


def dags_do_escopo(airflow: dict[str, Any], escopo: dict[str, Any]) -> list[Any]:
    grupos = set(escopo["grupos_dag"])
    return [d for d in airflow.get("dags", []) if d["grupo"] in grupos]


def camadas_de(modelos: list[dict[str, Any]]) -> dict[str, int]:
    contagem: dict[str, int] = {}
    for modelo in modelos:
        contagem[modelo["camada"]] = contagem.get(modelo["camada"], 0) + 1
    return {c: contagem[c] for c in CAMADAS if c in contagem}


def montar_dominios(
    curadoria: dict[str, Any],
    modelos: list[dict[str, Any]],
    dags: list[dict[str, Any]],
    entregas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Junta contexto curado com a evidencia do repositorio, dominio a dominio."""
    dominios = []
    for ficha in curadoria["dominios"]:
        slug = ficha["slug"]
        chaves = [c.lower() for c in ficha["chaves"]]
        meus_modelos = [m for m in modelos if m["dominio"] == slug]
        meus_dags = [
            d
            for d in dags
            if _casa(f"{d['dag_id']} {' '.join(d['tags'])}".lower(), chaves)
        ]
        minhas_entregas = [e for e in entregas if _casa(_texto_da_entrega(e), chaves)]

        camadas = camadas_de(meus_modelos)
        dominios.append(
            {
                **ficha,
                "href": f"dominios/{slug}/index.html",
                "total": len(meus_modelos),
                "camadas": camadas,
                "testes": sum(m["testes"] for m in meus_modelos),
                "modelos": sorted(
                    meus_modelos, key=lambda m: (CAMADAS.index(m["camada"]), m["nome"])
                ),
                "gold": [m for m in meus_modelos if m["camada"] == "gold"],
                "dags": sorted(meus_dags, key=lambda d: d["dag_id"]),
                "entregas": sorted(
                    minhas_entregas, key=lambda e: e["data"], reverse=True
                ),
                "entregas_documentadas": sum(
                    1 for e in minhas_entregas if len(e["corpo"]) > 60
                ),
            }
        )
    return sorted(dominios, key=lambda d: -d["total"])


def entregas_do_escopo(
    git: dict[str, Any], dominios: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Entregas que casaram com algum dominio, sem repetir."""
    vistas: dict[str, dict[str, Any]] = {}
    for dominio in dominios:
        for entrega in dominio["entregas"]:
            registro = vistas.setdefault(entrega["id"], {**entrega, "dominios": []})
            registro["dominios"].append(
                {"slug": dominio["slug"], "rotulo": dominio["rotulo"]}
            )
    fora = len(git.get("entregas", [])) - len(vistas)
    if fora > 0:
        log.info("%d entrega(s) fora do escopo MCID, nao publicadas", fora)
    return sorted(vistas.values(), key=lambda e: e["data"], reverse=True)


def agrupar_por_trimestre(entregas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grupos: dict[str, list[dict[str, Any]]] = {}
    for entrega in entregas:
        grupos.setdefault(_trimestre(entrega["data"]), []).append(entrega)
    return [
        {"rotulo": chave, "entregas": grupos[chave]}
        for chave in sorted(grupos, reverse=True)
    ]


def periodo(resumo: dict[str, Any]) -> tuple[str, int]:
    primeiro, ultimo = resumo.get("primeiro_commit"), resumo.get("ultimo_commit")
    if not primeiro or not ultimo:
        return "período indefinido", 0
    inicio = datetime.fromisoformat(primeiro)
    fim = datetime.fromisoformat(ultimo)
    meses = (fim.year - inicio.year) * 12 + fim.month - inicio.month
    return f"{MESES[inicio.month]}/{inicio.year} — {MESES[fim.month]}/{fim.year}", meses


def metricas(
    dominios: list[dict[str, Any]],
    dags: list[dict[str, Any]],
    entregas: list[dict[str, Any]],
    escopo: dict[str, Any],
    git: dict[str, Any],
) -> dict[str, Any]:
    sistemas = {s for d in dominios for s in d["sistemas"]}
    _, meses = periodo(git.get("resumo", {}))
    return {
        "total_modelos": sum(d["total"] for d in dominios),
        "total_testes": sum(d["testes"] for d in dominios),
        "total_gold": sum(d["camadas"].get("gold", 0) for d in dominios),
        "total_dominios": len(dominios),
        "total_dags": len(dags),
        "total_sistemas": len(sistemas),
        "total_entregas": len(entregas),
        "entregas_documentadas": sum(1 for e in entregas if len(e["corpo"]) > 60),
        "total_scripts": len(escopo.get("ingestoes_script", [])),
        "meses_projeto": meses,
    }
