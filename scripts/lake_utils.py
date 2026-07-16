# scripts/lake_utils.py

"""
Utilitários compartilhados dos scripts do data lake (MinIO).

Reúne o que mais de uma etapa do pipeline usa: detecção de encoding/dialeto dos arquivos
heterogêneos do raw/, normalização de nome de coluna, hash de arquivo e helpers de
S3/MinIO (cliente, amostra por Range, download para tempfile).

Usado por `mascarar_minio.py` e `raw_para_staging.py`. Cada script mantém o que é próprio
dele (conexão Postgres, tabela de controle, regras de negócio).
"""

import csv
import hashlib
import io
import re
import tempfile
import unicodedata
from typing import List, Optional, Tuple

# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)


# Detecção de encoding / dialeto

# cp1252 NÃO define estes 5 bytes — decodificar com ele estoura UnicodeDecodeError.
# Aparecem de fato no lake: há arquivos com mojibake da origem (ex.: b'PARTICIPA\x9d\xd1ES'
# onde deveria estar 'PARTICIPAÇÕES'), provavelmente de conversão errada num sistema legado.
# Nenhum encoding decodifica isso "certo"; o que importa é não quebrar nem perder o byte.
_CP1252_INDEFINIDOS = frozenset({0x81, 0x8D, 0x8F, 0x90, 0x9D})


def detectar_encoding(sample: bytes) -> str:
    """Detecta o encoding de um arquivo de dados pt-BR: utf-8, cp1252 ou latin-1.

    O `charset_normalizer` puro confunde cp1252 com cp1250/latin2 nesses dados (0xE3 vira 'ă'
    em vez de 'ã'), então a decisão aqui é feita à mão:

      - Sample com sequências multibyte utf-8 válidas -> utf-8 (tolera truncamento do sample).
      - Sample com algum byte indefinido em cp1252 -> latin-1, que mapeia os 256 bytes e nunca
        estoura (esses arquivos já vêm com mojibake; latin-1 preserva o byte em vez de falhar).
      - Caso contrário -> cp1252 (correto p/ o intervalo 0x80-0x9F em exports Windows pt-BR:
        aspas curvas, travessão etc., que latin-1 leria como controles C1).

    ATENÇÃO: a decisão é feita sobre o SAMPLE. Um byte indefinido pode aparecer só depois dele
    — por isso quem decodifica o arquivo inteiro deve usar `encoding_fallback()` ao pegar
    UnicodeDecodeError, em vez de confiar cegamente neste retorno.
    """
    if any(byte in _CP1252_INDEFINIDOS for byte in sample):
        return "latin-1"
    if all(byte < 0x80 for byte in sample):
        return "cp1252"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as e:
        # erro só nos últimos bytes = provável char utf-8 cortado no fim do sample
        if e.start >= len(sample) - 3:
            return "utf-8"
        return "cp1252"


def encoding_fallback(encoding: str) -> Optional[str]:
    """Próximo encoding a tentar quando `encoding` estourou no arquivo inteiro.

    utf-8 e cp1252 podem falhar em bytes que não existem no sample (64 KB) mas aparecem
    depois. latin-1 aceita qualquer byte, então é o fim da cadeia: None = não há fallback.
    """
    return None if encoding == "latin-1" else "latin-1"


def detectar_dialeto(sample: bytes, real_encoding: str) -> Optional[Tuple[str, str, bool]]:
    """Retorna (delimitador, lineterminator, fully_quoted) ou None se não parecer tabular.

    Usa csv.reader (e não a primeira linha física) porque há arquivos com quebras de
    linha DENTRO de campos aspeados do header (ex.: '"Data de\\nMovimento";...') — a
    primeira linha física nesses casos não contém o delimitador.
    """
    texto = sample.decode(real_encoding, errors="replace")
    lineterm = "\r\n" if "\r\n" in texto else "\n"
    stripped = texto.lstrip("﻿")
    if not stripped.strip():
        return None

    melhor_delim, melhor_campos = None, 1
    for d in [";", "|", "\t", ","]:
        try:
            primeiro = next(csv.reader(io.StringIO(stripped), delimiter=d, quotechar='"'), None)
        except csv.Error:
            continue
        # sample pode cortar no meio de um campo aspeado; ainda assim o nº de campos
        # do primeiro registro indica se o delimitador é plausível
        if primeiro and len(primeiro) > melhor_campos:
            melhor_delim, melhor_campos = d, len(primeiro)
    if melhor_delim is None:
        return None
    fully_quoted = stripped.startswith('"')
    return melhor_delim, lineterm, fully_quoted


# Normalização de nome de coluna
def norm_header(texto: str) -> str:
    """Normaliza um nome de coluna para snake_case ASCII."""
    s = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip().strip('"').strip("﻿").strip()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def normalizar_colunas(header: List[str]) -> Tuple[List[str], dict]:
    """Normaliza + deduplica nomes de coluna.

    - Header vazio vira `coluna_<i>` (1-based).
    - Nomes repetidos recebem sufixo `_2`, `_3`, ...
    Retorna (nomes_finais, mapa original->final).
    """
    finais: List[str] = []
    usados: set = set()
    mapa: dict = {}
    for i, original in enumerate(header, 1):
        base = norm_header(original) or f"coluna_{i}"
        nome = base
        n = 2
        while nome in usados:
            nome = f"{base}_{n}"
            n += 1
        usados.add(nome)
        finais.append(nome)
        mapa[original] = nome
    return finais, mapa


# Hash de arquivo
def md5_arquivo(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# MinIO (S3) helpers
def criar_cliente_minio(endpoint: str, access_key: str, secret_key: str):
    import boto3

    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def sample_bytes(s3, bucket: str, key: str, n: int = 65536) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{n - 1}")["Body"].read()


def baixar(s3, bucket: str, key: str, suffix: str, tmpdir: Optional[str] = None) -> str:
    """Baixa o objeto para um tempfile em disco (evita OOM em arquivos grandes)."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=tmpdir)
    try:
        s3.download_fileobj(bucket, key, tmp)
        tmp.flush()
    finally:
        tmp.close()
    return tmp.name


def format_size(size_bytes: float) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"
