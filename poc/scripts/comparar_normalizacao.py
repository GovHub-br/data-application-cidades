#!/usr/bin/env python3
"""Compara `normalize_names=true` do DuckDB com `lake_utils.normalizar_colunas`.

Importa: se os dois divergirem, adotar o DuckDB renomeia colunas silenciosamente e quebra
os contratos que silver/gold já assumem. Produz resultados/normalizacao_nomes.md.
"""

import os
import sys
import tempfile
from pathlib import Path

import duckdb

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ.parent / "scripts"))
from lake_utils import normalizar_colunas  # noqa: E402

SAIDA = RAIZ / "resultados" / "normalizacao_nomes.md"

# Casos tirados dos padrões de header que aparecem no lake do MCid
CASOS = [
    "Código Cliente", "Nome do Cliente", "Observação",
    "Nº Contrato", "VALOR (R$)", "UF/Município", "2º Repasse",
    "  espaço  ", "Data\nMovimento", "select", "Ação-Nº", "Col", "Col",
]


def duckdb_normaliza(header: list) -> list:
    caminho = tempfile.mktemp(suffix=".csv")
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(";".join('"' + h.replace('"', "") + '"' for h in header) + "\n")
        f.write(";".join("x" for _ in header) + "\n")
    try:
        con = duckdb.connect()
        return list(con.sql(
            f"SELECT * FROM read_csv('{caminho}', delim=';', header=true, "
            f"all_varchar=true, normalize_names=true) LIMIT 0"
        ).columns)
    finally:
        os.unlink(caminho)


def main() -> None:
    duck = duckdb_normaliza(CASOS)
    py, _ = normalizar_colunas(list(CASOS))

    divergencias = [(o, d, p) for o, d, p in zip(CASOS, duck, py) if d != p]

    SAIDA.parent.mkdir(exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("# Normalização de nomes de coluna — DuckDB vs `lake_utils`\n\n")
        f.write(f"DuckDB {duckdb.__version__} `normalize_names=true` vs `normalizar_colunas()`.\n\n")
        f.write("| header original | DuckDB | lake_utils | igual? |\n|---|---|---|---|\n")
        for o, d, p in zip(CASOS, duck, py):
            marca = "sim" if d == p else "**NÃO**"
            f.write(f"| `{o!r}` | `{d}` | `{p}` | {marca} |\n")
        f.write(f"\n**{len(divergencias)} de {len(CASOS)} divergem.** Padrões:\n\n")
        f.write("- separador perdido em `/` e `-`: `UF/Município` → `ufmunicipio` (DuckDB) vs `uf_municipio`\n")
        f.write("- prefixo `_` em nome iniciado por dígito e em palavra reservada: `2º Repasse` → `_2o_repasse`, `select` → `_select`\n")
        f.write("- **dedup com base diferente**: duplicata vira `col_1` no DuckDB e `col_2` no `lake_utils`\n\n")
        f.write(
            "Consequência: `normalize_names` não é substituto drop-in. Ou se renomeia coluna a "
            "coluna no SQL da bronze (viável, já que a bronze é escrita por arquivo), ou os nomes "
            "mudam silenciosamente e quebram silver/gold.\n"
        )
    print(f"Escrito: {SAIDA}  ({len(divergencias)}/{len(CASOS)} divergem)")
    for o, d, p in divergencias:
        print(f"  {o!r:20s} duckdb={d:16s} lake_utils={p}")


if __name__ == "__main__":
    main()
