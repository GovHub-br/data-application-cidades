#!/usr/bin/env python3
"""
Migração pontual: renomeia tabelas do schema sftp para usar a nova estratégia
de nomeação (truncamento pelo lado esquerdo para arquivos internos de zip,
sem incluir o nome do zip no caminho).

Execução: python scripts/sftp_rename_tables.py [--dry-run]
"""
import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DB_HOST     = os.getenv("DB_DW_HOST_MCID")
DB_PORT     = os.getenv("DB_DW_PORT_MCID")
DB_USER     = os.getenv("DB_DW_USER_MCID")
DB_PASSWORD = os.getenv("DB_DW_PASSWORD_MCID")
DB_NAME     = os.getenv("DB_DW_DBNAME_MCID")
DB_SCHEMA   = "sftp"
INGEST_LOG  = "_ingest_log"
TABLE_NAME_LIMIT = 57


def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^\w]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def build_table_name_new(sftp_key: str) -> str:
    """Nova lógica de nomeação — mesma do sftp_ingest.py atualizado."""
    if "::" in sftp_key:
        zip_path, inner = sftp_key.split("::", 1)
        parts = Path(zip_path).parts
        segments = [normalize_name(p) for p in parts if p not in (".", "..")]
        folder_segs = segments[:-1]
        inner_stem = normalize_name(Path(inner).stem)
        name = "_".join(folder_segs + [inner_stem])
        if len(name) <= TABLE_NAME_LIMIT:
            return name
        prefix = "_".join(folder_segs)
        available = TABLE_NAME_LIMIT - len(prefix) - 1
        if available <= 0:
            return prefix[:TABLE_NAME_LIMIT]
        raw_suffix = inner_stem[-available:]
        us = raw_suffix.find("_")
        suffix = (
            raw_suffix[us + 1:] if us >= 0 and raw_suffix[us + 1:]
            else raw_suffix.lstrip("_")
        )
        return f"{prefix}_{suffix}"
    else:
        parts = Path(sftp_key).parts
        segments = [normalize_name(p) for p in parts if p not in (".", "..")]
        segments[-1] = normalize_name(Path(sftp_key).stem)
        return "_".join(segments)[:TABLE_NAME_LIMIT]


def resolve_table_name(base: str, registry: dict[str, int]) -> str:
    if base not in registry:
        registry[base] = 0
        return base
    registry[base] += 1
    return f"{base}_{registry[base]:04d}"


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — nenhuma alteração será aplicada ===\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=int(DB_PORT or 5432),
        user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME,
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(
        f"SELECT sftp_key, table_name FROM {DB_SCHEMA}.{INGEST_LOG}"
        f" ORDER BY processed_at"
    )
    entries = cur.fetchall()
    print(f"Entradas no log: {len(entries)}")

    cur.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename != %s",
        (DB_SCHEMA, INGEST_LOG),
    )
    existing = {row[0] for row in cur.fetchall()}

    # Calcula novos nomes com a mesma lógica de deduplicação do script de ingestão
    registry: dict[str, int] = {}
    plan: list[tuple[str, str, str]] = []  # (sftp_key, old_name, new_name)

    for sftp_key, old_name in entries:
        new_base = build_table_name_new(sftp_key)
        new_name = resolve_table_name(new_base, registry)
        plan.append((sftp_key, old_name, new_name))

    to_rename = [(k, o, n) for k, o, n in plan if o != n]
    unchanged  = [(k, o, n) for k, o, n in plan if o == n]
    print(f"Sem mudança:   {len(unchanged)}")
    print(f"Para renomear: {len(to_rename)}\n")

    errors = 0
    renamed = 0
    skipped = 0

    for sftp_key, old_name, new_name in to_rename:
        if old_name not in existing:
            print(f"  SKIP (tabela ausente no bd): {old_name} → {new_name}")
            skipped += 1
            if not dry_run:
                cur.execute(
                    f"UPDATE {DB_SCHEMA}.{INGEST_LOG}"
                    f" SET table_name = %s WHERE sftp_key = %s",
                    (new_name, sftp_key),
                )
            continue

        if new_name in existing:
            print(f"  CONFLITO (novo nome já existe): {old_name} → {new_name}")
            errors += 1
            continue

        print(f"  ✓ {old_name}  →  {new_name}")
        if not dry_run:
            cur.execute(
                f'ALTER TABLE "{DB_SCHEMA}"."{old_name}" RENAME TO "{new_name}"'
            )
            cur.execute(
                f"UPDATE {DB_SCHEMA}.{INGEST_LOG}"
                f" SET table_name = %s WHERE sftp_key = %s",
                (new_name, sftp_key),
            )
        existing.discard(old_name)
        existing.add(new_name)
        renamed += 1

    print(f"\nResultado: {renamed} renomeadas, {skipped} puladas, {errors} conflitos")

    if dry_run:
        conn.rollback()
        print("Dry run concluído — nenhuma mudança aplicada.")
    elif errors > 0:
        conn.rollback()
        print("Rollback — corrija os conflitos antes de reaplicar.")
    else:
        conn.commit()
        print("Commit realizado com sucesso.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
