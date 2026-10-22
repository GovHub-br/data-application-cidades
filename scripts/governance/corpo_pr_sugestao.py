#!/usr/bin/env python3
"""Corpo do PR aberto a partir de uma sugestão do OpenMetadata.

O texto da sugestão vem de um usuário do catálogo; entra citado, e `@` perde
o poder de mencionar alguém no GitHub.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import quote

SEM_MENCAO = "@\u200b"


def citar(texto: str) -> str:
    return "\n".join(
        f"> {linha}" for linha in texto.replace("@", SEM_MENCAO).splitlines()
    )


def corpo(carga: dict, arquivo: str, om_host: str) -> str:
    alvo = f"`{carga['fqn']}`"
    if carga.get("coluna"):
        alvo += f", coluna `{carga['coluna']}`"
    link = (
        f"{om_host.rstrip('/')}/table/{quote(carga['fqn'], safe='.')}" if om_host else ""
    )
    autor = str(carga.get("autor") or "desconhecido").replace("@", SEM_MENCAO)
    linhas = [
        f"Sugestão de descrição feita no OpenMetadata por **{autor}** "
        f"(Task {carga['task_id']}).",
        "",
        f"**Ativo:** {alvo}" + (f" — [abrir no catálogo]({link})" if link else ""),
        f"**Arquivo:** `{arquivo}`",
        "",
        "**Texto sugerido:**",
        "",
        citar(carga["sugestao"]),
        "",
        "---",
        "",
        "Revise o texto como qualquer outra mudança de documentação: pode editar "
        "nesta branch antes do merge. Ao fechar este PR, mergeado ou não, a Task é "
        "fechada no catálogo com o link dele; a descrição aprovada chega ao "
        "OpenMetadata na próxima sincronização.",
    ]
    return "\n".join(linhas) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--carga", type=Path, required=True)
    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--om-host", default="")
    args = parser.parse_args()
    carga = json.loads(args.carga.read_text(encoding="utf-8"))
    print(corpo(carga, args.arquivo, args.om_host), end="")


if __name__ == "__main__":
    main()
