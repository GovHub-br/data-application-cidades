#!/usr/bin/env python3
"""Converte cortes CadÚnico já mascarados de raw/ para staging/.

Executar sempre com um arquivo ``local.env`` fora do controle de versão. O
script não imprime variáveis, valores de linhas nem identificadores pessoais.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import dotenv_values


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--pattern", required=True)
    parser.add_argument("--chunksize", type=int, default=100_000)
    args = parser.parse_args()

    values = dotenv_values(args.env_file)
    os.environ.update({key: value for key, value in values.items() if value is not None})
    os.environ["LAKE_LOCAL_ARTIFACTS"] = "0"

    scripts_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(scripts_dir))
    from raw_para_staging import run  # noqa: PLC0415

    resultado = run(
        apply=True,
        pattern=args.pattern,
        chunksize=args.chunksize,
        bad_lines="skip",
    )
    print({"pattern": args.pattern, "resultado": resultado})


if __name__ == "__main__":
    main()
