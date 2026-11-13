#!/usr/bin/env python3
"""Gera o catálogo OpenMetadata sem persistir artefatos dbt brutos."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = ROOT / "dbt" / "mcid"
EXPORTER = ROOT / "scripts" / "governance" / "exportar_catalogo_openmetadata.py"
OUTPUT = ROOT / "dbt" / "mcid" / "governance" / "openmetadata_semantic_catalog.json"


def main() -> int:
    # O executável do dbt não é o mesmo em todo lugar: no ambiente do time é
    # `poetry run dbt`, no CI é `dbt` puro. O `gerar_docs_seguros.py` já aceitava
    # a escolha; este fixava poetry e quebrava fora dele.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dbt-command",
        default="poetry run dbt",
        help="executável do dbt (padrão: 'poetry run dbt'; no CI, 'dbt')",
    )
    parser.add_argument(
        "--select",
        nargs="+",
        help="limita docs generate a estes seletores dbt e mescla o resultado ao catálogo existente",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env", override=False)
    with tempfile.TemporaryDirectory(prefix="openmetadata-dbt-") as private_target:
        command = [
            *shlex.split(args.dbt_command),
            "docs",
            "generate",
            "--profiles-dir",
            str(DBT_PROJECT),
            "--target-path",
            private_target,
        ]
        if args.select:
            command.extend(["--select", *args.select])
        subprocess.run(command, cwd=DBT_PROJECT, check=True)

        output = OUTPUT
        export_command = [
            sys.executable,
            str(EXPORTER),
            "--target-dir",
            private_target,
        ]
        if args.select:
            output = Path(private_target) / "openmetadata_semantic_catalog.json"
            seletores_de_caminho = []
            for seletor in args.select:
                if not seletor.startswith("path:"):
                    raise ValueError(
                        "A geração seletiva do catálogo aceita apenas seletores path:."
                    )
                seletores_de_caminho.append(seletor.removeprefix("path:"))
            export_command.extend(["--model-path", *seletores_de_caminho])
        export_command.extend(["--output", str(output)])
        subprocess.run(export_command, cwd=ROOT, check=True)
        if args.select:
            catalogo_novo = json.loads(output.read_text(encoding="utf-8"))
            catalogo_existente = (
                json.loads(OUTPUT.read_text(encoding="utf-8"))
                if OUTPUT.exists()
                else {}
            )
            models = {
                model["id"]: model
                for model in catalogo_existente.get("models", [])
            }
            models.update({model["id"]: model for model in catalogo_novo["models"]})
            catalogo_novo["models"] = [models[key] for key in sorted(models)]
            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT.write_text(
                json.dumps(catalogo_novo, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(
                f"Catálogo semântico mesclado: {OUTPUT} "
                f"({len(catalogo_novo['models'])} models)"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
