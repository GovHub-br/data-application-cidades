# scripts/lake_utils.py

"""
Utilitários compartilhados dos scripts do data lake (MinIO).

Reúne o que mais de uma etapa do pipeline usa: detecção de encoding/dialeto dos arquivos
heterogêneos do raw/, normalização de nome de coluna, hash de arquivo e leitura de bases
Access (.mdb). O I/O de S3/MinIO fica em plugins/cliente_minio.py (ClienteMinio).

Usado por `mascarar_minio.py` e `raw_para_staging.py`. Cada script mantém o que é próprio
dele (conexão Postgres, tabela de controle, regras de negócio).
"""

import csv
import hashlib
import io
import re
import shutil
import subprocess
import unicodedata
from typing import List, Optional, Tuple

# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)


# Detecção de encoding / dialeto

# bytes que o cp1252 não define: decodificar com ele estoura UnicodeDecodeError. Ocorrem
# em arquivos que já vêm com mojibake da origem, onde só importa não perder o byte.
_CP1252_INDEFINIDOS = frozenset({0x81, 0x8D, 0x8F, 0x90, 0x9D})


def detectar_encoding(sample: bytes) -> str:
    """Detecta o encoding de um arquivo de dados pt-BR: utf-8, cp1252 ou latin-1.

    O `charset_normalizer` puro confunde cp1252 com cp1250/latin2 nesses dados (0xE3 vira
    'ă' em vez de 'ã'), então a decisão é feita à mão, nesta ordem:

      - só ASCII -> cp1252, palpite seguro p/ export Windows pt-BR
      - decodifica como utf-8 -> utf-8 (tolera truncamento no fim do sample)
      - tem byte indefinido em cp1252 -> latin-1, que mapeia os 256 bytes e nunca estoura
      - resto -> cp1252, correto p/ 0x80-0x9F (aspas curvas, travessão)

    A ordem importa: os bytes indefinidos no cp1252 também são continuação utf-8 válida
    ('Á' é C3 81), então testá-los antes do utf-8 leria todo acento como latin-1.

    A decisão vale para o SAMPLE. Quem decodifica o arquivo inteiro deve tratar
    UnicodeDecodeError com `encoding_fallback()` em vez de confiar neste retorno.
    """
    if all(byte < 0x80 for byte in sample):
        return "cp1252"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as e:
        # erro só nos últimos bytes = provável char utf-8 cortado no fim do sample
        if e.start >= len(sample) - 3:
            return "utf-8"
    # utf-8 "sujo": um punhado de bytes inválidos não desfaz um arquivo que é utf-8 no
    # resto. Sem esta checagem, UM byte estranho no meio de um arquivo grande jogava a
    # decisão para cp1252/latin-1, e aí TODO acento do arquivo virava mojibake — o
    # cabeçalho junto. Comparar sequências multibyte válidas contra bytes inválidos
    # separa os dois casos: um utf-8 com um byte solto tem milhares de sequências
    # válidas e um punhado de erros; um cp1252 de verdade tem o oposto, porque cada
    # acento isolado é uma sequência utf-8 inválida.
    validas, invalidas = _contar_sequencias_utf8(sample)
    if validas > invalidas:
        return "utf-8"
    if any(byte in _CP1252_INDEFINIDOS for byte in sample):
        return "latin-1"
    return "cp1252"


def _contar_sequencias_utf8(sample: bytes) -> Tuple[int, int]:
    """(sequências multibyte utf-8 válidas, bytes que não formam sequência válida)."""
    validas = invalidas = 0
    i, n = 0, len(sample)
    while i < n:
        b = sample[i]
        if b < 0x80:
            i += 1
            continue
        largura = 2 if b >> 5 == 0b110 else 3 if b >> 4 == 0b1110 else 4 if b >> 3 == 0b11110 else 0
        if largura and i + largura <= n and all(
            sample[i + k] >> 6 == 0b10 for k in range(1, largura)
        ):
            validas += 1
            i += largura
        else:
            invalidas += 1
            i += 1
    return validas, invalidas


def encoding_fallback(encoding: str) -> Optional[str]:
    """Próximo encoding a tentar quando `encoding` estourou no arquivo inteiro.

    utf-8 e cp1252 podem falhar em bytes que não existem no sample (64 KB) mas aparecem
    depois. latin-1 aceita qualquer byte, então é o fim da cadeia: None = não há fallback.
    """
    return None if encoding == "latin-1" else "latin-1"


def detectar_dialeto(
    sample: bytes, real_encoding: str
) -> Optional[Tuple[str, str, bool]]:
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
            primeiro = next(
                csv.reader(io.StringIO(stripped), delimiter=d, quotechar='"'), None
            )
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


# Reparo de mojibake
# Marcadores do round-trip utf-8 -> latin-1/cp1252: "Ã" cobre os acentos latinos
# (Ã£=ã, Ã©=é, Ã³=ó...), "Â" os símbolos (Âº, Â°) e "â€" a pontuação tipográfica.
_MARCAS_MOJIBAKE = ("Ã", "Â", "â€")


def corrigir_mojibake_texto(texto: str) -> str:
    """Desfaz o round-trip utf-8 -> latin-1 quando ele é reversível sem perda.

    Um arquivo utf-8 lido como latin-1 vira mojibake: os bytes C3 A3 ("ã") são exibidos
    como "Ã£". A transformação é byte a byte e não perde informação, então re-encodar em
    latin-1 e decodificar em utf-8 recupera o original exatamente.

    Conservadora de propósito — só mexe quando as três condições valem:
      1. o texto contém um marcador de mojibake (senão não há o que corrigir);
      2. ele é representável em latin-1 (senão não veio desse round-trip);
      3. os bytes resultantes são utf-8 válido (senão a "correção" seria um chute).

    Texto já correto passa incólume: "José" não tem marcador e volta igual.
    """
    if not texto or not any(m in texto for m in _MARCAS_MOJIBAKE):
        return texto
    try:
        return texto.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        # não veio de um round-trip latin-1/utf-8 limpo: melhor não adivinhar
        return texto


# Normalização de nome de coluna
def norm_header(texto: str) -> str:
    """Normaliza um nome de coluna para snake_case ASCII.

    Repara mojibake antes de tirar o acento: sem isso, "municÃ­pio" perderia o "Ã­" e
    viraria "municapio" em vez de "municipio".
    """
    texto = corrigir_mojibake_texto(texto)
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


# Bases Access (.mdb) — leitura via mdbtools, que é READ-ONLY: não há mdb-import, e quem
# precisar modificar um .mdb tem que falhar explicitamente. O mdb-export entrega CSV em
# UTF-8, então este caminho dispensa detectar_encoding/detectar_dialeto.
MDB_EXT = {".mdb", ".accdb"}
MDB_DELIM = ","
MDB_ENCODING = "utf-8"


def mdb_disponivel() -> bool:
    """True se o binário mdbtools está no PATH."""
    return shutil.which("mdb-tables") is not None


def mdb_tabelas(path: str) -> List[str]:
    """Nomes das tabelas de usuário do .mdb (mdbtools já omite as MSys* de sistema)."""
    r = subprocess.run(["mdb-tables", "-1", path], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"mdb-tables falhou: {r.stderr.strip() or 'erro desconhecido'}"
        )
    return [t.strip() for t in r.stdout.splitlines() if t.strip()]


def mdb_contar(path: str, tabela: str) -> int:
    """Nº de linhas da tabela. -1 se o mdb-count falhar (não é motivo p/ abortar)."""
    r = subprocess.run(["mdb-count", path, tabela], capture_output=True, text=True)
    if r.returncode != 0:
        return -1
    try:
        return int(r.stdout.strip())
    except ValueError:
        return -1


def mdb_header(path: str, tabela: str) -> List[str]:
    """Só o cabeçalho da tabela — sem materializar as linhas.

    Tabelas de .mdb podem ter milhões de linhas (ex.: acompanhamento_termino_obra com
    6,1M), então lê o stdout incrementalmente e para na 1ª linha, matando o processo em
    seguida.
    """
    p = subprocess.Popen(
        ["mdb-export", path, tabela],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding=MDB_ENCODING,
    )
    try:
        linha = p.stdout.readline() if p.stdout else ""
    finally:
        p.kill()
        p.wait()
    if not linha.strip():
        return []
    return next(csv.reader(io.StringIO(linha), delimiter=MDB_DELIM, quotechar='"'), [])


def mdb_export_para_csv(path: str, tabela: str, dst: str) -> None:
    """Exporta a tabela inteira para um CSV em disco (streaming, memória constante)."""
    with open(dst, "wb") as f:
        r = subprocess.run(["mdb-export", path, tabela], stdout=f, stderr=subprocess.PIPE)
    if r.returncode != 0:
        raise RuntimeError(
            f"mdb-export falhou em '{tabela}': "
            f"{r.stderr.decode('utf-8', 'replace').strip()}"
        )


# Hash de arquivo
def md5_arquivo(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# NOTA: os helpers de S3/MinIO (cliente, amostra por Range, download) foram movidos para
# plugins/cliente_minio.py (classe ClienteMinio), usada pelos scripts e pela DAG.


def format_size(size_bytes: float) -> str:
    value: float = size_bytes
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"
