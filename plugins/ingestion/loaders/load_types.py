"""Modos de carga: como uma nova ingestão se combina com o que o bronze já tem.

As mesmas regras valem no `fonte_lake` do dbt, que é quem carrega o bronze hoje, e
nos loaders Python (Postgres sem MinIO, Iceberg) que vierem depois.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum


class LoadMode(StrEnum):
    """`overwrite`: a última ingestão substitui tudo; deleções na fonte se propagam.

    `merge`: por chave, vale a ingestão mais recente; chave ausente da ingestão nova
    continua com o último valor (deleções não se propagam). Para extração parcial.

    `append`: todas as ingestões empilhadas. Só para fonte que nunca reentrega linha.
    """

    OVERWRITE = "overwrite"
    MERGE = "merge"
    APPEND = "append"


@dataclass(frozen=True)
class LoadResult:
    mode: LoadMode
    table: str
    rows: int


def validate_load(mode: LoadMode | str, keys: Sequence[str]) -> tuple[str, ...]:
    """Confere a combinação de modo e chaves; devolve as chaves normalizadas.

    `merge` exige chaves, sem repetição; `overwrite` e `append` não aceitam chaves
    (seriam ignoradas em silêncio). Chave inexistente no dado só aparece na carga.
    """
    try:
        load_mode = LoadMode(mode)
    except ValueError:
        valid = ", ".join(m.value for m in LoadMode)
        raise ValueError(
            f"modo de carga desconhecido: {mode!r} (válidos: {valid})"
        ) from None
    keys = tuple(keys)
    if load_mode is LoadMode.MERGE:
        if not keys:
            raise ValueError("merge exige keys")
        repeated = sorted({key for key in keys if keys.count(key) > 1})
        if repeated:
            raise ValueError(f"chave repetida no merge: {', '.join(repeated)}")
        return keys
    if keys:
        raise ValueError(f"keys só no merge; {load_mode.value} recebeu {list(keys)}")
    return ()
