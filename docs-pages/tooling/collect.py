"""
collect.py: roda os coletores e grava o acervo em docs-pages/src/_data/.

Coletor que falha nao derruba os demais nem apaga a coleta anterior: o JSON
antigo permanece e o site publica com o ultimo dado bom.

Uso:
    make docs-collect
    make docs-collect --somente dbt
"""

import argparse
import sys
from typing import Any, Callable

from tooling.collectors import airflow_dags, assets, dbt_models, git_pr
from tooling.common import DATA_DIR, log, write_json

COLETORES: dict[str, Callable[[], dict[str, Any]]] = {
    "git_pr": git_pr.coletar,
    "dbt": dbt_models.coletar,
    "airflow": airflow_dags.coletar,
    "acervo": assets.coletar,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Coleta o acervo do site de docs")
    parser.add_argument("--somente", choices=sorted(COLETORES), help="roda so um")
    args = parser.parse_args()

    alvos = [args.somente] if args.somente else list(COLETORES)
    falhas = 0

    for nome in alvos:
        try:
            payload = COLETORES[nome]()
        except Exception as erro:
            falhas += 1
            anterior = DATA_DIR / f"{nome}.json"
            estado = "mantendo coleta anterior" if anterior.exists() else "sem dado"
            log.error("coletor %s falhou (%s): %s", nome, estado, erro)
            continue

        destino = write_json(nome, payload)
        resumo = payload.get("resumo", {})
        principais = ", ".join(
            f"{k}={v}" for k, v in resumo.items() if isinstance(v, (int, str))
        )
        log.info("%s -> %s (%s)", nome, destino.name, principais)

    if falhas:
        log.warning("%d coletor(es) falharam; acervo publicavel mesmo assim", falhas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
