"""
git_pr.py: coleta commits, pull requests e entregas do historico do projeto.

O repositorio e um fork de GovHub-br/data-application-gov-hub, criado em
13/04/2026. O historico anterior ao fork veio da plataforma base; o trabalho
proprio de Cidades comeca ali, e e onde surgem os PRs do GitHub. Antes disso as
entregas chegaram como merge de branch, no GitLab.

Cada entrega carrega, portanto, duas marcacoes: a origem do registro ("pr" ou
"branch") e o momento no projeto ("base" ou "cidades").

Saida: docs-pages/src/_data/git_pr.json
"""

import json
import re
from datetime import datetime, timezone
from typing import Any

from tooling.common import log, run

SEP = "\x1f"
FORMATO = SEP.join(["%H", "%an", "%aI", "%s"])
FORMATO_MERGE = SEP.join(["%H", "%an", "%aI", "%s"])

DATA_FORK = "2026-04-13"
RE_BRANCH = re.compile(r"Merge branch '([^']+)'")
RE_PR_MERGE = re.compile(r"Merge pull request #(\d+) from [\w.-]+/(\S+)")
# Merge de integracao nao e entrega: so traz a main de volta para a branch.
BRANCHES_INTEGRACAO = {"main", "master", "develop", "dev"}
RE_ISSUES = re.compile(r"(?:issues?|resolve[sm]?)[^\n]*?#(\d+)", re.I)
RE_CHECKLIST = re.compile(r"^\s*[-*]\s*\[[ xX]\]\s*(.+)$", re.M)
RE_ARQUIVOS = re.compile(r"`([\w./-]+\.(?:py|sql|yml|yaml|json))`")

TIPOS = {
    "feat": "funcionalidade",
    "fix": "correção",
    "refact": "refatoração",
    "refactor": "refatoração",
    "test": "testes",
    "docs": "documentação",
    "chore": "manutenção",
    "style": "estilo",
}


def _commits() -> list[dict[str, str]]:
    saida = run(["git", "log", "--no-merges", f"--format={FORMATO}"])
    commits: list[dict[str, str]] = []
    for linha in saida.splitlines():
        if not linha.strip():
            continue
        sha, autor, data, titulo = linha.split(SEP)
        prefixo = titulo.split(":")[0].strip().lower() if ":" in titulo[:12] else ""
        commits.append(
            {
                "sha": sha[:8],
                "autor": autor,
                "data": data,
                "titulo": titulo,
                "tipo": TIPOS.get(prefixo, "outro"),
            }
        )
    return commits


def _merges() -> list[dict[str, str]]:
    """Merge commits, usados como entrega no periodo anterior aos PRs."""
    saida = run(["git", "log", "--merges", f"--format={FORMATO_MERGE}"])
    merges: list[dict[str, str]] = []
    for linha in saida.splitlines():
        if not linha.strip():
            continue
        sha, autor, data, titulo = linha.split(SEP)
        merges.append({"sha": sha[:8], "autor": autor, "data": data, "titulo": titulo})
    return merges


def _pull_requests() -> list[dict[str, Any]]:
    campos = "number,title,author,createdAt,mergedAt,state,labels,url,body"
    saida = run(
        ["gh", "pr", "list", "--state", "all", "--limit", "500", "--json", campos]
    )
    prs: list[dict[str, Any]] = []
    for pr in json.loads(saida):
        prs.append(
            {
                "numero": pr["number"],
                "titulo": pr["title"],
                "autor": (pr.get("author") or {}).get("login", ""),
                "criado_em": pr.get("createdAt"),
                "mergeado_em": pr.get("mergedAt"),
                "estado": pr.get("state"),
                "labels": [rot["name"] for rot in pr.get("labels") or []],
                "url": pr.get("url"),
                "corpo": (pr.get("body") or "").strip(),
            }
        )
    return prs


def _tipo_do_titulo(titulo: str) -> str:
    prefixo = titulo.split(":")[0].strip().lower() if ":" in titulo[:12] else ""
    return TIPOS.get(prefixo, "outro")


def _limpar_titulo(titulo: str) -> str:
    """Remove o prefixo convencional, ja capturado no campo tipo."""
    if ":" in titulo[:12] and titulo.split(":")[0].strip().lower() in TIPOS:
        return titulo.split(":", 1)[1].strip()
    return titulo.strip()


def _entrega_de_pr(pr: dict[str, Any]) -> dict[str, Any]:
    corpo = pr["corpo"]
    return {
        "id": f"pr-{pr['numero']}",
        "origem": "pr",
        "momento": "cidades",
        "referencia": f"#{pr['numero']}",
        "url": pr["url"],
        "titulo": _limpar_titulo(pr["titulo"]),
        "tipo": _tipo_do_titulo(pr["titulo"]),
        "autor": pr["autor"],
        "data": pr["mergeado_em"],
        "labels": pr["labels"],
        "corpo": corpo,
        "checklist": RE_CHECKLIST.findall(corpo),
        "issues": sorted({int(n) for n in RE_ISSUES.findall(corpo)}),
        "arquivos": sorted(set(RE_ARQUIVOS.findall(corpo)))[:12],
    }


def _referencia_do_merge(titulo: str, data: str, prs_conhecidos: set[int]) -> Any:
    """Devolve (origem, referencia, branch) do merge, ou None se nao for entrega.

    Tres casos: PR deste repositorio (ja coletado pelo gh, com corpo), PR
    herdado do repositorio de origem (so o merge sobrou no historico) e merge de
    branch, o formato do periodo GitLab.

    Os dois repositorios numeram PRs na mesma faixa, entao o numero sozinho nao
    diz de quem e o merge: quando este fork chegou ao #123, o #123 herdado da
    base seria descartado como duplicata e a entrega sumiria do historico. A
    data e que desempata — antes do fork, o PR so pode ser da base.
    """
    pr_ref = RE_PR_MERGE.search(titulo)
    if pr_ref:
        numero, branch = int(pr_ref.group(1)), pr_ref.group(2)
        if numero in prs_conhecidos and data[:10] >= DATA_FORK:
            return None
        return "pr_base", f"#{numero}", branch

    branch_ref = RE_BRANCH.search(titulo)
    if not branch_ref:
        return None
    nome = branch_ref.group(1)
    if nome.split("/")[-1].lower() in BRANCHES_INTEGRACAO:
        return None
    return "branch", nome, nome


def _entrega_de_merge(merge: dict[str, str], prs_conhecidos: set[int]) -> Any:
    """Converte um merge em entrega, quando ele representa trabalho concluido."""
    identificacao = _referencia_do_merge(merge["titulo"], merge["data"], prs_conhecidos)
    if not identificacao:
        return None

    origem, referencia, branch = identificacao
    rotulo = branch.split("/")[-1].replace("-", " ").replace("_", " ").strip()
    return {
        "id": f"merge-{merge['sha']}",
        "origem": origem,
        "momento": "base" if merge["data"][:10] < DATA_FORK else "cidades",
        "referencia": referencia,
        "url": None,
        "titulo": rotulo.capitalize(),
        "tipo": _tipo_do_titulo(rotulo),
        "autor": merge["autor"],
        "data": merge["data"],
        "labels": [],
        "corpo": "",
        "checklist": [],
        "issues": [],
        "arquivos": [],
    }


def _entregas(prs: list[dict[str, Any]], merges: list[dict[str, str]]) -> list[Any]:
    numeros = {pr["numero"] for pr in prs}
    entregas = [_entrega_de_pr(pr) for pr in prs if pr["mergeado_em"]]
    for merge in merges:
        entrega = _entrega_de_merge(merge, numeros)
        if entrega:
            entregas.append(entrega)
    entregas.sort(key=lambda e: e["data"], reverse=True)
    return entregas


def coletar() -> dict[str, Any]:
    commits = _commits()
    merges = _merges()
    try:
        prs = _pull_requests()
    except Exception as erro:  # gh indisponivel nao invalida o historico local
        log.warning("gh indisponivel, seguindo apenas com o historico local: %s", erro)
        prs = []

    entregas = _entregas(prs, merges)
    datas = sorted(c["data"] for c in commits)
    autores = {c["autor"] for c in commits}
    por_momento: dict[str, int] = {}
    for entrega in entregas:
        chave = str(entrega["momento"])
        por_momento[chave] = por_momento.get(chave, 0) + 1

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "data_fork": DATA_FORK,
        "resumo": {
            "total_commits": len(commits),
            "total_merges": len(merges),
            "total_prs": len(prs),
            "prs_mergeados": sum(1 for pr in prs if pr["mergeado_em"]),
            "total_entregas": len(entregas),
            "entregas_documentadas": sum(1 for e in entregas if len(e["corpo"]) > 60),
            "total_autores": len(autores),
            "primeiro_commit": datas[0] if datas else None,
            "ultimo_commit": datas[-1] if datas else None,
            "por_momento": por_momento,
        },
        "entregas": entregas,
        "commits": commits,
        "prs": prs,
    }
