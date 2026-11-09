"""Descarte de gêmeos: o mesmo dado em mais de um formato ou pasta vira um parquet só."""

import os
from typing import Dict, List, Tuple

from .config import RAW_PREFIX, SUPORTADAS

# Gêmeos: o mesmo dado aparece no lake em mais de um formato e em mais de uma pasta, e
# converter todos geraria N tabelas iguais na staging. A identidade é o NOME DO ARQUIVO,
# sem extensão e sem pasta; para cada nome sobrevive um objeto, escolhido por:
#
#   1. formato suportado antes de não-suportado, senão um .xls descarta o .csv equivalente
#      e o dado some da staging;
#   2. texto antes de planilha, porque o mascaramento reescreve texto byte a byte e no
#      xlsx precisa remontar a pasta de trabalho;
#   3. .csv antes de .txt e .xlsx antes de .xls, por consistência;
#   4. empate -> menor key lexicográfica, para a escolha não variar entre execuções.
#
# Extensão fora da lista fica por último.
PRECEDENCIA_EXT = [".csv", ".txt", ".xlsx", ".xls", ".mdb", ".accdb", ".json"]

# Ignorar a pasta pressupõe que ela é só um CANAL alternativo do mesmo arquivo (SFTP vs
# SharePoint, GEFUS/ vs GEFUS/ANTERIORES/). Não vale para fontes de extração automatizada
# (PDF/OCR) que usam nomes padronizados por dado e a pasta É a competência ou a entidade
# (abecip/2025-10/, construtoras/tenda/): aí o nome se repete de propósito, e ignorar a
# pasta juntaria dados diferentes num "gêmeo" só e descartaria quase tudo.
FONTES_COM_PASTA_NA_IDENTIDADE = {"abecip", "construtoras", "mrv"}


def _rank_ext(ext: str) -> Tuple[int, int]:
    ext = ext.lower()
    # suportado vence a posição na lista: .xls vem antes de .mdb, mas o script só lê .mdb
    suportado = 0 if ext in SUPORTADAS else 1
    try:
        return (suportado, PRECEDENCIA_EXT.index(ext))
    except ValueError:
        return (suportado, len(PRECEDENCIA_EXT))


def _stem_sem_extensao(key: str) -> str:
    """Identidade que os gêmeos compartilham: nome do arquivo sem extensão.

    Ignora a pasta por padrão — o mesmo nome em `GEFUS/` e em `GEFUS/ANTERIORES/` é o
    mesmo dado chegando por dois canais, e só um deve virar parquet. Exceção: fontes em
    `FONTES_COM_PASTA_NA_IDENTIDADE`, onde a pasta faz parte do dado (ver comentário
    acima) — aí a identidade é o caminho relativo inteiro, não só o nome do arquivo.
    """
    rel = key[len(RAW_PREFIX) :] if key.startswith(RAW_PREFIX) else key
    fonte = rel.split("/", 1)[0]
    if fonte in FONTES_COM_PASTA_NA_IDENTIDADE:
        return os.path.splitext(rel)[0]
    return os.path.splitext(os.path.basename(key))[0]


def _descartar_gemeos(
    candidatos: List[Tuple[str, int]],
) -> Tuple[List[Tuple[str, int]], List[str]]:
    """Separa (mantidos, descartados) entre objetos que só diferem na extensão."""
    por_stem: Dict[str, List[Tuple[str, int]]] = {}
    for key, size in candidatos:
        por_stem.setdefault(_stem_sem_extensao(key), []).append((key, size))

    mantidos: List[Tuple[str, int]] = []
    descartados: List[str] = []
    for _stem, grupo in por_stem.items():
        if len(grupo) == 1:
            mantidos.append(grupo[0])
            continue
        ordenado = sorted(
            grupo, key=lambda kv: (_rank_ext(os.path.splitext(kv[0])[1]), kv[0])
        )
        mantidos.append(ordenado[0])
        descartados.extend(k for k, _ in ordenado[1:])
    # Preserva a ordem da listagem do MinIO (previsível no log).
    ordem = {k: i for i, (k, _) in enumerate(candidatos)}
    mantidos.sort(key=lambda kv: ordem[kv[0]])
    return mantidos, descartados
