"""Mascaramento de CSV/TXT em fluxo, preservando os bytes do que não é PII."""

import csv
from typing import List, Optional, Tuple

from ingestion.masking.keys import MaskingKeys
from ingestion.masking.rules import _PF_INDICATOR_CATS, classificar


# Processamento CSV/TXT (streaming, byte-preserving via latin-1)
def mascarar_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    lineterm: str,
    fully_quoted: bool,
    real_encoding: str,
    targets_fixos: Optional[List[dict]] = None,
    keys: Optional[MaskingKeys] = None,
) -> Tuple[List[dict], bool, int, int]:
    """Retorna (targets, has_pf, registros_total, registros_alterados).

    `targets_fixos` (de `targets_por_posicao`) troca a descoberta por nome de coluna por
    posições declaradas — e implica arquivo SEM cabeçalho: nenhuma linha é consumida antes
    do laço, então a linha 0 é mascarada como dado, que é o ponto todo do override.
    """
    keys = keys or MaskingKeys.from_env()
    quoting = csv.QUOTE_ALL if fully_quoted else csv.QUOTE_MINIMAL
    total = alterados = 0
    targets: List[dict] = []
    has_pf = False

    with (
        open(src_path, "r", encoding="latin-1", newline="") as fin,
        open(dst_path, "w", encoding="latin-1", newline="") as fout,
    ):
        reader = csv.reader(fin, delimiter=delim, quotechar='"')
        writer = csv.writer(
            fout, delimiter=delim, quotechar='"', quoting=quoting, lineterminator=lineterm
        )

        if targets_fixos is not None:
            targets = list(targets_fixos)
            has_pf = any(t["category"] in _PF_INDICATOR_CATS for t in targets)
        else:
            try:
                header = next(reader)
            except StopIteration:
                return targets, has_pf, 0, 0

            targets, has_pf = classificar(header, real_encoding)
            writer.writerow(header)
            if not targets:
                # sem colunas sensíveis: nada a fazer (o chamador trata como skip_no_pii)
                return targets, has_pf, 0, 0

        idx_action = [(t["idx"], t["action"]) for t in targets]
        for row in reader:
            total += 1
            row_alterada = False
            for idx, action in idx_action:
                if idx < len(row) and row[idx] is not None and row[idx].strip() != "":
                    row[idx] = (
                        keys.token(row[idx])
                        if action == "hmac"
                        else keys.redact(row[idx])
                    )
                    row_alterada = True
            if row_alterada:
                alterados += 1
            writer.writerow(row)

    return targets, has_pf, total, alterados


def verificar_roundtrip_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    total_esperado: int,
    sem_header: bool = False,
) -> None:
    """Garante que nº de linhas/colunas do header foi preservado.

    `sem_header`: a primeira linha é dado, então entra na contagem — senão a checagem
    acusaria uma linha a menos e derrubaria o arquivo por engano.
    """

    def _header_e_linhas(path: str) -> Tuple[int, int]:
        with open(path, "r", encoding="latin-1", newline="") as f:
            reader = csv.reader(f, delimiter=delim, quotechar='"')
            primeira = next(reader, [])
            n = sum(1 for _ in reader)
        return len(primeira), n + (1 if sem_header and primeira else 0)

    ncols_src, _ = _header_e_linhas(src_path)
    ncols_dst, n_dst = _header_e_linhas(dst_path)
    if ncols_src != ncols_dst:
        raise ValueError(
            f"round-trip: colunas do header divergem ({ncols_src} != {ncols_dst})"
        )
    if n_dst != total_esperado:
        raise ValueError(
            f"round-trip: nº de linhas divergem ({n_dst} != {total_esperado})"
        )
