"""CSV/TXT grande e sem aspas: mascaramento em pedaços, um por processo."""

import contextlib
import csv
import hashlib
import io
import json
import logging
import multiprocessing
import os
import sys
import threading
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Any, Dict, Iterator, List, Optional, Tuple

from cliente_minio import ClienteMinio

from .config import (
    PARALELO_MIN,
    PEDACO,
    PEDACOS_PREFIX,
    PROCESSOS,
    _LOCAL_ARTIFACTS,
    _formatter,
)
from .regras import (
    _PF_INDICATOR_CATS,
    classificar,
)
from .tabular import _mascarar_fluxo

log = logging.getLogger(__name__)


# Paralelismo do CSV/TXT grande. Só vale sem aspas: aí todo "\n" fecha um registro e o
# arquivo pode ser cortado em qualquer fim de linha.
class _SemAspas(io.RawIOBase):
    """Repassa a leitura e para no primeiro byte de aspas."""

    def __init__(self, src: io.RawIOBase) -> None:
        self._src = src

    def readable(self) -> bool:
        return True

    def readinto(self, destino: Any) -> int:
        n = self._src.readinto(destino)
        if n and b'"' in memoryview(destino)[:n]:
            raise _ComAspas()
        return n


class _ComAspas(Exception):
    pass


def _cliente() -> ClienteMinio:
    return ClienteMinio()


def _iniciar_processo(com_log: bool) -> None:
    if com_log:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_formatter)
        logging.root.addHandler(handler)
        logging.root.setLevel(logging.INFO)


@contextlib.contextmanager
def _pool(processos: int) -> Iterator[Executor]:
    # o filho reimporta este módulo: sem isso cada um abriria um arquivo de log próprio
    anterior = os.environ.get("LAKE_LOCAL_ARTIFACTS")
    os.environ["LAKE_LOCAL_ARTIFACTS"] = "0"
    try:
        with ProcessPoolExecutor(
            processos,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_iniciar_processo,
            initargs=(_LOCAL_ARTIFACTS,),
        ) as pool:
            yield pool
    finally:
        if anterior is None:
            os.environ.pop("LAKE_LOCAL_ARTIFACTS", None)
        else:
            os.environ["LAKE_LOCAL_ARTIFACTS"] = anterior


def _pode_paralelizar(sample: bytes, fully_quoted: bool) -> bool:
    return PROCESSOS > 1 and not fully_quoted and b'"' not in sample


def _cortes(minio: ClienteMinio, key: str, tamanho: int, n: int) -> Optional[List[int]]:
    """Offsets logo após um "\\n", dividindo o objeto em até n pedaços."""
    cortes = [0]
    for i in range(1, n):
        pos = tamanho * i // n
        amostra = minio.sample_bytes(key, 1024 * 1024, inicio=pos)
        idx = amostra.find(b"\n")
        if idx < 0:
            return None
        corte = pos + idx + 1
        if cortes[-1] < corte < tamanho:
            cortes.append(corte)
    cortes.append(tamanho)
    return cortes


def _mascarar_pedaco(
    key: str,
    etag: str,
    inicio: int,
    fim: int,
    chave_pedaco: str,
    delim: str,
    lineterm: str,
    real_encoding: str,
    targets: List[dict],
    com_header: bool,
) -> Tuple[str, int, int]:
    """Mascara [inicio, fim) de `key` para o objeto `chave_pedaco`.

    Roda num processo filho. Retorna ("ok", total, alterados), ou ("aspas", 0, 0) se o
    pedaço tiver aspas. Pedaço já pronto de uma execução anterior não é refeito.
    """
    minio = _cliente()
    # marcador em objeto, não em tag: o usuário do lake não grava tag fora de raw/
    marcador = chave_pedaco + ".json"
    if any(k == marcador for k, _ in minio.listar_objetos(marcador)):
        pronto = json.loads(minio.s3.get_object(Bucket=minio.bucket, Key=marcador)["Body"].read())
        return "ok", pronto["total"], pronto["alterados"]

    leitor = minio.abrir_leitura(key, inicio=inicio, fim=fim, etag=etag)
    escritor = minio.abrir_escrita(chave_pedaco, fim - inicio)
    bruto = io.BufferedReader(_SemAspas(leitor), 8 * 1024 * 1024)
    try:
        fin = io.TextIOWrapper(bruto, encoding="latin-1", newline="")
        fout = io.TextIOWrapper(
            io.BufferedWriter(escritor, 8 * 1024 * 1024), encoding="latin-1", newline=""
        )
        _, _, total, alterados = _mascarar_fluxo(
            fin,
            fout,
            delim,
            lineterm,
            False,
            real_encoding,
            None if com_header else targets,
            verificar=True,
        )
        fout.flush()
        escritor.concluir()
    except _ComAspas:
        escritor.abortar()
        return "aspas", 0, 0
    except BaseException:
        escritor.abortar()
        raise
    minio.put_object(marcador, json.dumps({"total": total, "alterados": alterados}).encode())
    return "ok", total, alterados


def _md5_objeto(minio: ClienteMinio, key: str, etag: Optional[str] = None) -> str:
    leitor = minio.abrir_leitura(key, etag=etag)
    bruto = io.BufferedReader(leitor, 8 * 1024 * 1024)
    while bruto.read(8 * 1024 * 1024):
        pass
    return leitor.md5.hexdigest()


def _targets_do_sample(
    sample: bytes,
    delim: str,
    real_encoding: str,
    targets_fixos: Optional[List[dict]],
) -> Tuple[List[dict], bool]:
    if targets_fixos is not None:
        targets = list(targets_fixos)
        return targets, any(t["category"] in _PF_INDICATOR_CATS for t in targets)
    texto = sample.decode("latin-1")
    if "\n" not in texto:
        return [], False
    header = next(csv.reader([texto.split("\n", 1)[0]], delimiter=delim), [])
    return classificar(header, real_encoding)


def _esperar_pedacos(
    minio: ClienteMinio,
    futuros: list,
    pedacos: List[Tuple[int, int, str]],
    tamanho: int,
) -> Optional[Tuple[int, int, str]]:
    """Coleta os pedaços em ordem e calcula o md5 da saída lendo cada um.

    Retorna (total, alterados, md5), ou None se algum pedaço tiver aspas.
    """
    total = alterados = 0
    md5_saida = hashlib.md5()
    for i, (futuro, (_, fim, chave)) in enumerate(zip(futuros, pedacos), 1):
        status, n_total, n_alterados = futuro.result()
        if status == "aspas":
            return None
        total += n_total
        alterados += n_alterados
        bruto = io.BufferedReader(minio.abrir_leitura(chave), 8 * 1024 * 1024)
        while bloco := bruto.read(8 * 1024 * 1024):
            md5_saida.update(bloco)
        log.info(
            "Pedaço %d/%d pronto (%.1f de %.1f GiB)",
            i,
            len(pedacos),
            fim / 2**30,
            tamanho / 2**30,
        )
    return total, alterados, md5_saida.hexdigest()


def _mascarar_tabular_paralelo(
    minio: ClienteMinio,
    key: str,
    dest_key: str,
    delim: str,
    lineterm: str,
    real_encoding: str,
    sample: bytes,
    targets_fixos: Optional[List[dict]],
    masked_hashes: set,
) -> Optional[Tuple[List[dict], bool, int, int, str, Optional[str]]]:
    """Mascara em pedaços, um por processo, e junta tudo no MinIO.

    Cada pedaço vira um objeto em tmp/masking/ e só no fim eles são concatenados em
    `dest_key`. Uma falha preserva os pedaços prontos: rodar de novo refaz só os que
    faltam. Retorna None quando o caminho sequencial tem que assumir (arquivo pequeno,
    sem PII, com aspas ou sem fim de linha para cortar).
    """
    targets, has_pf = _targets_do_sample(sample, delim, real_encoding, targets_fixos)
    if not targets:
        return None
    head = minio.s3.head_object(Bucket=minio.bucket, Key=key)
    etag, tamanho = head["ETag"], int(head["ContentLength"])
    if tamanho < PARALELO_MIN:
        return None
    cortes = _cortes(minio, key, tamanho, max(PROCESSOS, -(-tamanho // PEDACO)))
    if cortes is None:
        return None

    raiz = f"{PEDACOS_PREFIX}{hashlib.sha256(key.encode()).hexdigest()[:16]}/"
    base = f"{raiz}{etag.strip(chr(34))}/"
    pedacos = [
        (ini, fim, f"{base}{i:04d}-{ini}-{fim}")
        for i, (ini, fim) in enumerate(zip(cortes, cortes[1:]))
    ]
    log.info(
        "%s: %d pedaços em %d processos (%.1f GiB)",
        key,
        len(pedacos),
        PROCESSOS,
        tamanho / 2**30,
    )

    # o md5 da origem vai numa thread enquanto os pedaços processam
    origem: Dict[str, Any] = {}

    def _hash_origem() -> None:
        try:
            origem["md5"] = _md5_objeto(_cliente(), key, etag)
        except Exception as exc:  # noqa: BLE001
            origem["erro"] = exc

    thread = threading.Thread(target=_hash_origem, daemon=True)
    thread.start()

    with _pool(PROCESSOS) as pool:
        futuros = [
            pool.submit(
                _mascarar_pedaco,
                key,
                etag,
                ini,
                fim,
                chave,
                delim,
                lineterm,
                real_encoding,
                targets,
                targets_fixos is None and i == 0,
            )
            for i, (ini, fim, chave) in enumerate(pedacos)
        ]
        try:
            saida = _esperar_pedacos(minio, futuros, pedacos, tamanho)
        finally:
            for f in futuros:
                f.cancel()

    if saida is None:
        log.warning("%s tem aspas: segue pelo caminho sequencial", key)
        minio.apagar_prefixo(raiz)
        return None
    total, alterados, hash_after = saida

    thread.join()
    if "erro" in origem:
        raise origem["erro"]
    hash_before = origem["md5"]
    if hash_before in masked_hashes:
        minio.apagar_prefixo(raiz)
        return targets, has_pf, total, alterados, hash_before, None

    minio.juntar_objetos([chave for _, _, chave in pedacos], dest_key)
    minio.apagar_prefixo(raiz)
    return targets, has_pf, total, alterados, hash_before, hash_after
