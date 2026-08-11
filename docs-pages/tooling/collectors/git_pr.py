"""
git_pr.py: coleta commits do git e pull requests via gh CLI.

Saida: docs-pages/src/_data/git_pr.json
"""

import json
from datetime import datetime, timezone
from typing import Any

from tooling.common import log, run

SEP = "\x1f"
FORMATO = SEP.join(["%H", "%an", "%aI", "%s"])


def _commits() -> list[dict[str, str]]:
    saida = run(["git", "log", "--no-merges", f"--format={FORMATO}"])
    commits: list[dict[str, str]] = []
    for linha in saida.splitlines():
        if not linha.strip():
            continue
        sha, autor, data, titulo = linha.split(SEP)
        commits.append(
            {
                "sha": sha[:8],
                "autor": autor,
                "data": data,
                "titulo": titulo,
                "tipo": titulo.split(":")[0].strip() if ":" in titulo[:12] else "outro",
            }
        )
    return commits


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


def coletar() -> dict[str, Any]:
    commits = _commits()
    try:
        prs = _pull_requests()
    except Exception as erro:  # gh indisponivel nao invalida os commits
        log.warning("gh indisponivel, seguindo apenas com commits: %s", erro)
        prs = []

    datas = sorted(c["data"] for c in commits)
    mergeados = [pr for pr in prs if pr["mergeado_em"]]
    autores = {c["autor"] for c in commits}

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "resumo": {
            "total_commits": len(commits),
            "total_prs": len(prs),
            "prs_mergeados": len(mergeados),
            "total_autores": len(autores),
            "primeiro_commit": datas[0] if datas else None,
            "ultimo_commit": datas[-1] if datas else None,
        },
        "commits": commits,
        "prs": prs,
    }
