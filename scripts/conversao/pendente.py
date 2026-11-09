"""Parquet guardado quando o upload falha, para a próxima execução só reenviar."""

import hashlib
import json
import logging
import os
import tempfile

from .config import TMPDIR

log = logging.getLogger(__name__)

# Upload que falhou: o parquet fica em disco e a próxima execução só reenvia, sem
# reconverter (o CadÚnico pessoa leva ~3h para converter).
def _pendente(destino: str, source_hash: str) -> str:
    nome = hashlib.sha256(destino.encode("utf-8")).hexdigest()[:16]
    pasta = os.path.join(TMPDIR or tempfile.gettempdir(), "staging_pendente")
    return os.path.join(pasta, f"{nome}-{source_hash}.parquet")


def _descartar_pendentes_antigos(pendente: str) -> None:
    """Apaga o que sobrou de um conteúdo anterior do mesmo destino."""
    pasta = os.path.dirname(pendente)
    prefixo = os.path.basename(pendente).split("-", 1)[0] + "-"
    if not os.path.isdir(pasta):
        return
    for nome in os.listdir(pasta):
        antigo = os.path.join(pasta, nome)
        if nome.startswith(prefixo) and not antigo.startswith(pendente):
            os.unlink(antigo)
            log.info("Pendente de outro conteúdo descartado: %s", antigo)


def _guardar_pendente(dst: str, pendente: str, rec: dict) -> None:
    os.makedirs(os.path.dirname(pendente), exist_ok=True)
    os.replace(dst, pendente)
    campos = ("n_linhas", "n_colunas", "n_bad_lines", "column_map", "encoding")
    # o .json é gravado por último: é ele que marca o parquet como completo
    with open(pendente + ".json", "w", encoding="utf-8") as f:
        json.dump({c: rec[c] for c in campos}, f, ensure_ascii=False)
    log.error(
        "%s: upload falhou; parquet guardado em %s. Rodar de novo só reenvia.",
        rec["raw_key"],
        pendente,
    )
