"""Cabeçalho da staging: o mínimo para virar schema Parquet válido."""

from collections.abc import Sequence


def fix_header(names: Sequence[str | None]) -> list[str]:
    """Nome vazio (ou só espaço) vira `column_<n>` (1-based); repetido ganha `_2`...

    Só isso: acento, espaço, caixa e caracteres especiais ficam como a fonte mandou.
    Normalizar nome (snake_case, ≤63 bytes) é da prata do dbt.
    """
    fixed: list[str] = []
    used: set[str] = set()
    for position, name in enumerate(names, start=1):
        base = name if name and name.strip() else f"column_{position}"
        candidate, suffix = base, 2
        while candidate in used:
            candidate, suffix = f"{base}_{suffix}", suffix + 1
        used.add(candidate)
        fixed.append(candidate)
    return fixed
