"""O `normalizar_colunas` real: os nomes da staging antiga sobre o cabeçalho original.

A staging nova guarda o cabeçalho como a fonte mandou; as pratas leem os nomes
que o `raw_para_staging` gravava (`ingestion.text.normalizar_colunas`). O macro tem
de dar exatamente os mesmos nomes, então o teste compara com a função Python.
"""

import unicodedata
from datetime import datetime, timezone

from ingestion.text import norm_header, normalizar_colunas
from tests.ingestion.dbt.conftest import DbtProject

WHEN = datetime(2026, 10, 1, 9, tzinfo=timezone.utc)

HEADERS = [
    "Município",
    "Nº Contrato",
    "  Valor (R$) ",
    "valor",
    "",
    "municÃ\xadpio",  # utf-8 lido como latin-1
    "DATA-ASSINATURA",
    "Ação/Programa",
    '"Aspas"',
    "1º Titular",
    "½ área²",
    "QT_UH",
    "qt uh",
    "Situação Atual — Obra",
    "Ã",  # marcador sem utf-8 válido por trás: fica como está
    "ﬁm",  # ligadura: o NFKD a desfaz
]


def _csv(headers: list[str]) -> bytes:
    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    line = ",".join(quote(h) for h in headers)
    return f"{line}\n{','.join('x' for _ in headers)}\n".encode()


def _prata(dbt_project: DbtProject, headers: list[str]) -> list[str]:
    dbt_project.source("sftp_teste", caminho="staging/sftp/teste", load_mode="append")
    dbt_project.model("bronze_teste", "select * from {{ fonte_lake('sftp_teste') }}")
    dbt_project.model(
        "prata_teste", "select * from {{ normalizar_colunas(ref('bronze_teste')) }} b"
    )
    dbt_project.ingest("sftp", "teste", WHEN, {"arquivo.csv": _csv(headers)})
    dbt_project.build()
    return list(dbt_project.types("prata_teste"))


def test_names_are_the_ones_the_old_staging_wrote(dbt_project: DbtProject) -> None:
    columns = _prata(dbt_project, HEADERS)

    # a staging nova já resolveu vazio e repetido (column_5); a normalização vem depois
    staged = [name for name in dbt_project.types("bronze_teste") if name != "filename"]
    expected, _ = normalizar_colunas(staged)
    assert columns == [*expected, "_source_file", "_ingested_at"]


def test_lineage_columns(dbt_project: DbtProject) -> None:
    _prata(dbt_project, ["a", "b"])

    [(source_file, ingested_at)] = dbt_project.query(
        "select _source_file, _ingested_at from prata_teste"
    )
    assert source_file.endswith("/staging/sftp/teste/2026-10-01/060000/arquivo.parquet")
    assert ingested_at == "2026-10-01 06:00:00"
    assert dbt_project.types("prata_teste")["_ingested_at"] == "VARCHAR"


def test_every_character_folds_like_unicodedata(dbt_project: DbtProject) -> None:
    """Todo caractere do mapa do macro, contra o NFKD do Python, um por coluna."""
    folds = [
        chr(cp)
        for cp in range(0x80, 0x10000)
        if unicodedata.normalize("NFKD", chr(cp)).encode("ascii", "ignore")
    ]
    # prefixo e sufixo para que o caractere não suma no strip nem colida
    headers = [f"c{i}_{c}_x" for i, c in enumerate(folds)]

    columns = _prata(dbt_project, headers)

    assert columns[: len(headers)] == [norm_header(h) for h in headers]
