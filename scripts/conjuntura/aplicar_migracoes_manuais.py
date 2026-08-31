#!/usr/bin/env python3
"""Aplica as cargas manuais versionadas do boletim de conjuntura.

As migrações são idempotentes e precisam rodar antes do dbt para que as Golds
reflitam os valores manuais revisados. Credenciais são lidas do ``.env`` por
python-dotenv; o script não usa ``source .env`` nem registra segredos.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
DIRETORIO = ROOT / "scripts" / "database"


def migracoes(diretorio: Path) -> list[Path]:
    """Todos os `.sql` do diretório, em ordem numérica de prefixo.

    Varre em vez de manter lista fixa: com lista, cada script novo exigia
    lembrar de editar dois arquivos, e foi assim que o `0007` ficou de fora —
    quem rodasse o pipeline do zero não recebia a correção da Tenda. Os
    `0001`/`0002`, que criam e alteram o schema `manual_conjuntura`, também
    estavam ausentes, o que quebraria um banco novo já no `0003`.

    A ordem é pelo número do prefixo, não alfabética: com dois dígitos ainda
    coincidem, mas `0010` viria antes de `0002` numa ordenação de texto.
    """
    encontrados = sorted(
        diretorio.glob("*.sql"),
        key=lambda p: (int(m.group(1)) if (m := re.match(r"(\d+)", p.name)) else 10**9, p.name),
    )
    sem_prefixo = [p.name for p in encontrados if not re.match(r"\d+__", p.name)]
    if sem_prefixo:
        raise SystemExit(
            "Script sem prefixo numérico, ordem indefinida: " + ", ".join(sem_prefixo)
        )
    return encontrados


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--somente",
        help="aplica só estes scripts (nomes separados por vírgula), na ordem do diretório",
    )
    args = parser.parse_args()
    load_dotenv(ROOT / ".env", override=True)

    paths = migracoes(DIRETORIO)
    if not paths:
        raise SystemExit(f"Nenhum .sql em {DIRETORIO}")
    if args.somente:
        pedidos = {nome.strip() for nome in args.somente.split(",")}
        disponiveis = {p.name for p in paths}
        if faltando := pedidos - disponiveis:
            raise SystemExit("Não encontrado em scripts/database: " + ", ".join(sorted(faltando)))
        paths = [p for p in paths if p.name in pedidos]

    if args.dry_run:
        print("Aplicaria, nesta ordem:")
        print("\n".join(f"- {path.name}" for path in paths))
        return

    connection = psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=os.environ["DB_DW_PORT_MCID"],
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
        connect_timeout=15,
    )
    try:
        for path in paths:
            with connection.cursor() as cursor:
                cursor.execute(path.read_text(encoding="utf-8"))
            connection.commit()
            print(f"Aplicada: {path.name}")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
