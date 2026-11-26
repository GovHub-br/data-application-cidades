"""Nome de coluna em snake_case ASCII: a mesma regra de scripts/lake_utils.py (o teste
confere), sem importar de scripts/."""

import re
import unicodedata
from typing import List, Set, Tuple

_MARCAS_MOJIBAKE = ("Ã", "Â", "â€")


def corrigir_mojibake(texto: str) -> str:
    if not texto or not any(m in texto for m in _MARCAS_MOJIBAKE):
        return texto
    try:
        return texto.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return texto


def norm_header(texto: str) -> str:
    """Nome de coluna em snake_case ASCII."""
    texto = corrigir_mojibake(texto)
    s = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip().strip('"').strip("﻿").strip()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def normalizar_colunas(header: List[str]) -> Tuple[List[str], dict]:
    """Normaliza e desambigua (`_2`, `_3`...); cabeçalho vazio vira `coluna_<i>`."""
    finais: List[str] = []
    usados: Set[str] = set()
    mapa: dict = {}
    for i, original in enumerate(header, 1):
        base = norm_header(original) or f"coluna_{i}"
        nome, n = base, 2
        while nome in usados:
            nome, n = f"{base}_{n}", n + 1
        usados.add(nome)
        finais.append(nome)
        mapa[original] = nome
    return finais, mapa
