"""Access (.mdb): só verificação, porque o mdbtools não escreve."""

import logging
from typing import List, Tuple

from lake_utils import (
    mdb_contar,
    mdb_header,
    mdb_tabelas,
)

from .regras import classificar

log = logging.getLogger(__name__)


# Análise de .mdb (Access) — LEITURA APENAS
def _analisar_mdb(src_path: str) -> Tuple[List[dict], bool, int]:
    """Varre as tabelas do .mdb procurando colunas sensíveis. Retorna (targets, has_pf,
    linhas).

    NÃO mascara: o mdbtools é read-only e reescrever um .mdb exigiria Java. A função só
    responde "tem PII?"; se tiver, o chamador falha, porque gravar PII em silêncio no lake
    seria pior que um erro visível.
    """
    targets: List[dict] = []
    has_pf_any = False
    total = 0
    for tabela in mdb_tabelas(src_path):
        header = mdb_header(src_path, tabela)
        if not header:
            continue
        n = mdb_contar(src_path, tabela)
        if n > 0:
            total += n
        # mdb_header já devolve str decodificado de MDB_ENCODING: nada a reinterpretar
        t, has_pf = classificar(header, None)
        has_pf_any = has_pf_any or has_pf
        targets.extend({**x, "table": tabela} for x in t)
    return targets, has_pf_any, total
