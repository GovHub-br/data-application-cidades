"""CSV/TXT: mascaramento linha a linha, em disco ou direto do MinIO."""

import csv
import io
import logging
import shutil
from typing import IO, Any, List, Optional, Tuple

from cliente_minio import ClienteMinio

from .config import HMAC_SECRET
from .regras import (
    _PF_INDICATOR_CATS,
    _hmac_token,
    _redigir,
    classificar,
)

log = logging.getLogger(__name__)


# Processamento CSV/TXT (streaming, byte-preserving via latin-1)
def _mascarar_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    lineterm: str,
    fully_quoted: bool,
    real_encoding: str,
    targets_fixos: Optional[List[dict]] = None,
) -> Tuple[List[dict], bool, int, int]:
    """Retorna (targets, has_pf, registros_total, registros_alterados).

    `targets_fixos` (de `targets_por_posicao`) troca a descoberta por nome de coluna por
    posições declaradas — e implica arquivo SEM cabeçalho: nenhuma linha é consumida antes
    do laço, então a linha 0 é mascarada como dado, que é o ponto todo do override.
    """
    with (
        open(src_path, "r", encoding="latin-1", newline="") as fin,
        open(dst_path, "w", encoding="latin-1", newline="") as fout,
    ):
        return _mascarar_fluxo(
            fin, fout, delim, lineterm, fully_quoted, real_encoding, targets_fixos
        )


class _LoteVerificado:
    """Escreve linhas em lotes e reparseia cada lote antes de repassá-lo.

    Substitui a releitura do arquivo de saída quando ele não está em disco: qualquer
    linha que não volte idêntica ao que foi escrito derruba o arquivo.
    """

    def __init__(
        self, fout: IO[str], delim: str, quoting: int, lineterm: str, tamanho: int = 10000
    ) -> None:
        self._fout, self._delim, self._tamanho = fout, delim, tamanho
        self._buf = io.StringIO(newline="")
        self._writer = csv.writer(
            self._buf,
            delimiter=delim,
            quotechar='"',
            quoting=quoting,
            lineterminator=lineterm,
        )
        self._linhas: List[List[str]] = []

    def writerow(self, row: List[str]) -> None:
        self._writer.writerow(row)
        self._linhas.append(list(row))
        if len(self._linhas) >= self._tamanho:
            self.descarregar()

    def descarregar(self) -> None:
        texto = self._buf.getvalue()
        leitor = csv.reader(
            io.StringIO(texto, newline=""), delimiter=self._delim, quotechar='"'
        )
        relidas = list(leitor)
        if relidas != self._linhas:
            raise ValueError(
                f"round-trip: lote de {len(self._linhas)} linhas não relê igual "
                f"({len(relidas)} linhas relidas)"
            )
        self._fout.write(texto)
        self._buf.seek(0)
        self._buf.truncate()
        self._linhas.clear()


def _mascarar_fluxo(
    fin: IO[str],
    fout: IO[str],
    delim: str,
    lineterm: str,
    fully_quoted: bool,
    real_encoding: str,
    targets_fixos: Optional[List[dict]] = None,
    verificar: bool = False,
) -> Tuple[List[dict], bool, int, int]:
    """Núcleo de `_mascarar_tabular` sobre streams texto em latin-1.

    `verificar` confere o round-trip lote a lote, para quando a saída não pode ser relida.
    """
    quoting = csv.QUOTE_ALL if fully_quoted else csv.QUOTE_MINIMAL
    total = alterados = 0
    targets: List[dict] = []
    has_pf = False

    reader = csv.reader(fin, delimiter=delim, quotechar='"')
    if verificar:
        writer: Any = _LoteVerificado(fout, delim, quoting, lineterm)
    else:
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
                    _hmac_token(row[idx]) if action == "hmac" else _redigir(row[idx])
                )
                row_alterada = True
        if row_alterada:
            alterados += 1
        writer.writerow(row)

    if verificar:
        writer.descarregar()
    return targets, has_pf, total, alterados


def _mascarar_tabular_stream(
    minio: ClienteMinio,
    key: str,
    dest_key: str,
    delim: str,
    lineterm: str,
    fully_quoted: bool,
    real_encoding: str,
    targets_fixos: Optional[List[dict]],
    masked_hashes: set,
) -> Tuple[List[dict], bool, int, int, str, Optional[str]]:
    """Mascara do MinIO para o MinIO sem gravar em disco.

    `dest_key` só é substituído no fim, depois do round-trip; se algo falhar o multipart
    é abortado e o original fica. Retorna (targets, has_pf, total, alterados, hash_before,
    hash_after); hash_after é None quando nada foi gravado.
    """
    leitor = minio.abrir_leitura(key)
    escritor = minio.abrir_escrita(dest_key, leitor.tamanho)
    bruto = io.BufferedReader(leitor, 8 * 1024 * 1024)
    try:
        fin = io.TextIOWrapper(bruto, encoding="latin-1", newline="")
        fout = io.TextIOWrapper(
            io.BufferedWriter(escritor, 8 * 1024 * 1024), encoding="latin-1", newline=""
        )
        targets, has_pf, total, alterados = _mascarar_fluxo(
            fin,
            fout,
            delim,
            lineterm,
            fully_quoted,
            real_encoding,
            targets_fixos,
            verificar=True,
        )
        fout.flush()
        # o hash de origem cobre o arquivo inteiro, mesmo quando o laço para no header
        while bruto.read(8 * 1024 * 1024):
            pass
        hash_before = leitor.md5.hexdigest()
        if not targets or hash_before in masked_hashes:
            escritor.abortar()
            return targets, has_pf, total, alterados, hash_before, None
        escritor.concluir()
        return targets, has_pf, total, alterados, hash_before, escritor.md5.hexdigest()
    except BaseException:
        escritor.abortar()
        raise


def _verificar_roundtrip_tabular(
    src_path: str,
    dst_path: str,
    delim: str,
    total_esperado: int,
) -> None:
    """Garante que nº de linhas/colunas do header foi preservado."""

    def _header_e_linhas(path: str) -> Tuple[int, int]:
        with open(path, "r", encoding="latin-1", newline="") as f:
            reader = csv.reader(f, delimiter=delim, quotechar='"')
            primeira = next(reader, [])
            n = sum(1 for _ in reader)
        return len(primeira), n

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


def mascarar_csv(
    src_path: str, dst_path: str, delim: str, real_encoding: str
) -> Tuple[List[dict], int, int]:
    """Mascara um CSV LOCAL com as mesmas regras e o mesmo HMAC do raw/.

    Para quem converte um formato que este script não consegue reescrever no lugar: o
    .mdb fica com PII no raw/ (o mdbtools é read-only), então o raw_para_staging mascara
    o CSV que o `mdb-export` gera, antes de virar parquet. Mesmo segredo, mesmo token:
    o CPF continua cruzando com as bases mascaradas aqui.

    `dst_path` sai sempre completo, mascarado ou cópia do original. Retorna (targets,
    registros_total, registros_alterados).
    """
    with open(src_path, "r", encoding="latin-1", newline="") as f:
        header = next(csv.reader(f, delimiter=delim, quotechar='"'), [])
    targets, _ = classificar(header, real_encoding)
    if not targets:
        shutil.copyfile(src_path, dst_path)
        return targets, 0, 0
    if not HMAC_SECRET and any(t["action"] == "hmac" for t in targets):
        # Sem segredo o token vira um hash previsível do CPF: melhor parar.
        raise RuntimeError(
            "MASKING_HMAC_SECRET não definida — necessária para pseudonimizar "
            f"{[t['column'] for t in targets if t['action'] == 'hmac']}."
        )
    targets, _, total, alterados = _mascarar_tabular(
        src_path, dst_path, delim, "\n", False, real_encoding
    )
    _verificar_roundtrip_tabular(src_path, dst_path, delim, total)
    return targets, total, alterados
