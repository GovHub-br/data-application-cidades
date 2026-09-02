"""Gera e publica somente o catálogo seguro de metadados do Conjuntura.

`dbt docs generate` produz manifest e catalog completos, incluindo SQL
compilado e metadados da bronze. Por isso ele roda exclusivamente num diretório
temporário com permissão 0700. O único artefato persistido/publicável é o HTML
controlado por ``gerar_doc_pipeline.py``.
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[2]
DBT_PROJECT = RAIZ / "dbt" / "mcid"
GERADOR = RAIZ / "scripts" / "conjuntura" / "gerar_doc_pipeline.py"

# Não são dados; são identificadores que não podem aparecer no catálogo
# publicado. Mantido alinhado a macros/coluna_sensivel.sql.
PADROES_PROIBIDOS = (
    "cpf",
    "cnpj",
    "mutuario",
    "nascimento",
    "logradouro",
    "endereco",
    "telefone",
    "celular",
    "email",
    "nis",
    "titular",
    "beneficiario",
    "cep",
)


def verificar_catalogo_publico(caminho: Path) -> None:
    texto = caminho.read_text(encoding="utf-8").lower()
    encontrados = [p for p in PADROES_PROIBIDOS if re.search(rf"\b{p}\w*\b", texto)]
    if encontrados:
        raise RuntimeError(
            "Catálogo bloqueado: identificadores pessoais encontrados no artefato público: "
            + ", ".join(encontrados)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--saida", type=Path, default=RAIZ / "build" / "pipeline.html")
    parser.add_argument(
        "--public-dir",
        type=Path,
        help="diretório para publicação; grava somente index.html",
    )
    parser.add_argument(
        "--dbt-command",
        default="poetry run dbt",
        help="comando dbt, por exemplo 'dbt' na CI",
    )
    args = parser.parse_args()

    # Fonte única local de configuração. Variáveis já definidas pela CI ou
    # pelo shell têm precedência; `local.env` não participa deste fluxo.
    load_dotenv(RAIZ / ".env", override=False)

    comando_dbt = shlex.split(args.dbt_command)
    if not comando_dbt:
        raise ValueError("--dbt-command não pode ser vazio")

    # TemporaryDirectory cria 0700 por padrão; nenhum JSON bruto vai para
    # target/, public/ ou outro diretório servido pelo projeto.
    with tempfile.TemporaryDirectory(prefix="conjuntura-dbt-docs-") as temporario:
        alvo_privado = Path(temporario)
        subprocess.run(
            [
                *comando_dbt,
                "docs",
                "generate",
                "--profiles-dir",
                str(DBT_PROJECT),
                "--target-path",
                str(alvo_privado),
            ],
            cwd=DBT_PROJECT,
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                str(GERADOR),
                str(args.saida),
                "--target-dir",
                str(alvo_privado),
                "--sem-contagens",
            ],
            cwd=RAIZ,
            check=True,
        )

    verificar_catalogo_publico(args.saida)
    if args.public_dir:
        args.public_dir.mkdir(parents=True, exist_ok=True)
        destino_publico = args.public_dir / "index.html"
        destino_publico.write_text(
            args.saida.read_text(encoding="utf-8"), encoding="utf-8"
        )
        print(f"Catálogo seguro publicado em {destino_publico}")
    else:
        print(f"Catálogo seguro gerado em {args.saida}")


if __name__ == "__main__":
    main()
