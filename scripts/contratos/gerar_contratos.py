"""Bootstrap the ODCS contracts of the SFTP/SharePoint sources (F2, ADR-0006).

Reads what the project already declares (the `lake_staging*` tables of the dbt
`sources.yml` files) and, from the lake, only the SCHEMA of the newest staging Parquet of
each table (column names, never values). Writes one contract per source in `contracts/`,
with one schema object per dataset.

Run once to create the contracts; after that the contract is the source of truth and is
edited by PR. An existing contract is never overwritten without `--force`.

    uv run python scripts/contratos/gerar_contratos.py           # needs .env (lake + DW)
    uv run python scripts/contratos/gerar_contratos.py --force
"""

from __future__ import annotations

import argparse
import fnmatch
from os.path import commonprefix as os_commonprefix
import re
from pathlib import Path

import boto3
import duckdb
import psycopg2
import yaml
from dotenv import dotenv_values

from lake_utils import norm_header
from mascaramento.regras import _categoria

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "dbt" / "mcid" / "models"
CONTRACTS = ROOT / "contracts"

# Which source a staging path belongs to: the area or system that delivers it.
SOURCES = [
    (
        re.compile(r"^staging/sftp/fabrica/GEFUS/CadUnico/|^staging/sharepoint/Cadastro Único/"),
        "cadunico",
    ),
    (re.compile(r"^staging/sftp/fabrica/GEFUS/"), "sftp_gefus"),
    (re.compile(r"^staging/sftp/caixa\.geavo/GEAVO/"), "sftp_geavo"),
    (re.compile(r"^staging/sharepoint/.*fgts_canal"), "sftp_canal_fgts"),
    (re.compile(r"^staging/sharepoint/"), "sharepoint_mcid"),
]
SOURCE_INFO = {
    "cadunico": ("CadÚnico", "Cortes do Cadastro Único entregues ao MCid.", "CadÚnico (MDS)"),
    "sftp_gefus": ("SFTP · GEFUS", "Remessas da CAIXA (GEFUS) no SFTP do MCid.", "CAIXA · GEFUS"),
    "sftp_geavo": ("SFTP · GEAVO", "Remessas da CAIXA (GEAVO) no SFTP do MCid.", "CAIXA · GEAVO"),
    "sftp_canal_fgts": (
        "Canal FGTS",
        "Tabelas do Canal FGTS (AO1) e orçamentos.",
        "CAIXA · Canal FGTS",
    ),
    "sharepoint_mcid": ("SharePoint · MCid", "Planilhas do SharePoint do MCid.", "MCid · SNH"),
}
# Categories of the masking rules that are personal data (masked, redacted or kept).
PERSONAL = {
    "cpf",
    "nis",
    "documento",
    "nome",
    "nome_ambiguo",
    "nome_bare",
    "nascimento",
    "sensivel",
    "cep",
    "endereco",
    "texto_livre",
}


# A date or year in a file name: 8 digits, or a year with an optional month and day.
DATE = re.compile(r"\d{5,}|\d{4}(?:_\d{2}){0,2}")


def source_of(path: str) -> str:
    for pattern, source in SOURCES:
        if pattern.search(path):
            return source
    raise ValueError(f"no source for {path!r}")


def file_pattern(raw_name: str) -> str:
    """Regex over the raw file name, with dates and years (4+ digits) generalized.

    Shorter numbers name the dataset (FAIXA3, ao_1, INT039) and are kept.
    """
    return DATE.sub(
        lambda m: re.sub(r"\d+", lambda d: rf"\d{{{len(d.group())}}}", m.group()),
        re.escape(raw_name),
    )


def path_glob(paths: list[str]) -> str:
    """Staging path without fixed dates (ADR-0005 §9), covering every merged table."""
    globs = {DATE.sub("*", p) for p in paths}
    if len(globs) == 1:
        return globs.pop()
    names = {Path(g).name for g in globs}
    if len(names) == 1:  # same file name in different folders
        return f"staging/**/{names.pop()}"
    raise ValueError(f"cannot merge paths {sorted(paths)}")


def dbt_tables() -> list[dict]:
    """The `lake_staging*` tables that read SFTP/SharePoint staging."""
    tables = []
    for yml in sorted(MODELS.rglob("sources.yml")):
        doc = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
        for src in doc.get("sources") or []:
            bucket = (src.get("meta") or {}).get("bucket")
            for table in src.get("tables") or []:
                meta = table.get("meta") or {}
                path = meta.get("caminho") or meta.get("path") or ""
                if path.startswith(("staging/sftp/", "staging/sharepoint/")):
                    tables.append({**table, "path": path, "bucket": bucket, "dag": meta.get("dag")})
    return tables


def newest_key(s3, bucket: str, path: str) -> str:
    """The newest staging object that matches the declared path (a glob or a fixed key)."""
    prefix = re.split(r"[*?\[]", path, maxsplit=1)[0]
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if fnmatch.fnmatch(o["Key"], path)]
    if not keys:
        raise FileNotFoundError(path)
    return max(keys)


def columns(db, bucket: str, key: str) -> list[str]:
    """Column names from the Parquet footer only; the lineage columns stay out."""
    rows = db.execute(f"select name from parquet_schema('s3://{bucket}/{key}')").fetchall()
    return [r[0] for r in rows[1:] if not r[0].startswith("_")]  # rows[0] is the root


def raw_name(cur, staging_key: str) -> str:
    cur.execute(
        "select raw_key from lake._staging_log where staging_key = %s order by created_at desc limit 1",
        (staging_key,),
    )
    row = cur.fetchone()
    return Path(row[0]).name if row else Path(staging_key).name


def build(tables: list[dict], env: dict) -> dict[str, dict]:
    endpoint = env["MINIO_ENDPOINT"]
    if "://" not in endpoint:  # the .env keeps host:port, like the Airflow clients
        endpoint = f"http://{endpoint}"
    host = re.sub(r"^https?://", "", endpoint)
    secure = endpoint.startswith("https")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=env["MINIO_ACCESS_KEY"],
        aws_secret_access_key=env["MINIO_SECRET_KEY"],
    )
    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")
    db.execute(
        f"CREATE SECRET lake (TYPE s3, KEY_ID '{env['MINIO_ACCESS_KEY']}', "
        f"SECRET '{env['MINIO_SECRET_KEY']}', ENDPOINT '{host}', URL_STYLE 'path', "
        f"USE_SSL {str(secure).lower()})"
    )
    conn = psycopg2.connect(
        host=env["DB_DW_HOST_MCID"],
        port=env.get("DB_DW_PORT_MCID") or 5432,
        user=env["DB_DW_USER_MCID"],
        password=env["DB_DW_PASSWORD_MCID"],
        dbname=env["DB_DW_DBNAME_MCID"],
    )
    conn.set_session(readonly=True)
    cur = conn.cursor()

    collected = []
    for table in sorted(tables, key=lambda t: t["name"]):
        key = newest_key(s3, table["bucket"], table["path"])
        collected.append(
            {
                **table,
                "source": source_of(table["path"]),
                "columns": columns(db, table["bucket"], key),
                "pattern": file_pattern(raw_name(cur, key)),
            }
        )

    # One object per dataset: tables that only differ by date or year (two CadÚnico cuts,
    # one budget file per year) are the same recurring delivery.
    datasets: dict[tuple[str, str], list[dict]] = {}
    for item in collected:
        # an .mdb gives one staging table per Access table: the table is part of the key
        table = Path(item["path"]).stem.rpartition("__")[2] if "__" in item["path"] else ""
        datasets.setdefault((item["source"], item["pattern"], table), []).append(item)

    contracts: dict[str, dict] = {}
    for (source, pattern, _table), items in sorted(datasets.items()):
        name, description, owner = SOURCE_INFO[source]
        contract = contracts.setdefault(
            source,
            {
                "apiVersion": "v3.1.0",
                "kind": "DataContract",
                "id": source,
                "name": name,
                "version": "0.1.0",
                "status": "draft",
                "description": {"purpose": description},
                "team": [{"username": owner, "role": "owner"}],
                "servers": [
                    {
                        "server": "staging",
                        "type": "s3",
                        "format": "parquet",
                        "location": f"s3://{items[0]['bucket']}/staging/",
                    }
                ],
                "schema": [],
            },
        )
        names = [i["name"] for i in items]
        # merged tables take their common name, word by word (orcamento_fgts_2021..2024)
        object_name = "_".join(os_commonprefix([n.split("_") for n in names])) or min(names)
        cols = list(dict.fromkeys(c for i in items for c in i["columns"]))  # union, in order
        props = []
        for col in cols:
            prop = {"name": col, "logicalType": "string"}
            if _categoria(norm_header(col)) in PERSONAL:
                prop["classification"] = "restricted"
            props.append(prop)
        text = " ".join((items[0].get("description") or "").split())
        if len(items) > 1:
            text += f" Reúne as tabelas atuais {', '.join(names)}, que só diferem pela data."
        contract["schema"].append(
            {
                "name": object_name,
                "physicalName": object_name,
                "description": text,
                "customProperties": [
                    {"property": "path", "value": path_glob([i["path"] for i in items])},
                    {"property": "file_pattern", "value": pattern},
                    {"property": "load_type", "value": "full"},
                    {"property": "dag", "value": items[0]["dag"]},
                    {"property": "dbt_tables", "value": names},
                ],
                "properties": props,
            }
        )
    return contracts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--force", action="store_true", help="overwrite existing contracts")
    args = parser.parse_args()
    env = {**dotenv_values(ROOT / ".env")}
    CONTRACTS.mkdir(exist_ok=True)
    for source, contract in build(dbt_tables(), env).items():
        target = CONTRACTS / f"{source}.odcs.yaml"
        if target.exists() and not args.force:
            print(f"kept {target.relative_to(ROOT)} (use --force to overwrite)")
            continue
        target.write_text(yaml.safe_dump(contract, allow_unicode=True, sort_keys=False, width=100))
        print(f"wrote {target.relative_to(ROOT)}: {len(contract['schema'])} objects")


if __name__ == "__main__":
    main()
