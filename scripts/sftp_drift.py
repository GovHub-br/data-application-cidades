# scripts/sftp_drift.py

"""
Drift estrutural das entregas do SFTP na camada raw/ do data lake (MinIO).

Cada arquivo que o `sftp_para_minio.py` acabou de pousar é comparado com a entrega
anterior da mesma série. A série é o nome do arquivo sem pasta e sem extensão, com a
data trocada por DATA: `INT040_..._20231228.TXT` e `INT040_..._20240131.TXT`, em
GEFUS/ ou em GEFUS/ANTERIORES/, são a mesma série, e a data ordena as entregas.

Formatos: csv e txt; xlsx aba a aba; .mdb/.accdb tabela a tabela (mdbtools); zip
que pousou fechado (o CadÚnico), membro a membro, lido em fluxo. O .xls fica de fora,
como na staging: não há leitor dele na imagem (e os do SFTP são ementários avulsos,
sem série).

O que o relatório aponta:
  - colunas adicionadas, removidas, esvaziadas (tinham valor e agora estão todas
    vazias), que ficaram constantes (variavam e agora têm um valor só em todas as
    linhas) ou que passaram a ter valor, e se a ordem das que ficaram mudou;
  - o formato dominante de cada coluna, quando muda (`9,9` -> `9.9`: a vírgula
    decimal virou ponto, o que quebra a tipagem no dbt);
  - o carimbo da entrega: coluna com um valor só no arquivo, diferente do anterior
    e no mesmo formato (`dt_movimento`). Ela sai do hash da linha, senão toda linha
    pareceria alterada;
  - tamanho do arquivo e número de linhas, antes e depois (o tamanho do novo é o de
    antes do mascaramento, então só a variação de linhas é comparável entre os dois);
  - conteúdo, por hash de cada linha: linhas iguais, novas e removidas. Com chave
    declarada em CHAVES, por registro: novos, removidos e ALTERADOS (mesma chave,
    conteúdo diferente), além de chave duplicada. E as colunas que mais trazem
    valores que não existiam na entrega anterior.

PII: o arquivo novo ainda não passou pelo `mascarar_minio.py` e o anterior já passou.
As colunas que o `classificar` do mascaramento aponta como PII ficam fora do hash (a
comparação nem lê o valor delas), e o relatório só leva nomes de coluna e contagens.

Relatório em drift/sftp/<série>/<AAAAMMDDTHHMMSS>__<arquivo>.json no MinIO, e um
WARNING no log quando há drift. A detecção nunca derruba a ingestão: um arquivo que
não dá para comparar só gera aviso.
"""

import json
import logging
import os
import re
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

from lake_utils import (
    MDB_DELIM,
    MDB_ENCODING,
    MDB_EXT,
    detectar_dialeto,
    detectar_encoding,
    mdb_disponivel,
    mdb_export_para_csv,
    mdb_tabelas,
    normalizar_colunas,
)
from mascarar_minio import COLUNAS_POR_POSICAO, classificar

log = logging.getLogger(__name__)

RAW_PREFIX = "raw/sftp/"
DRIFT_PREFIX = "drift/sftp/"
TEXTO = {".csv", ".txt"}
LEGIVEIS = TEXTO | {".xlsx", ".zip"} | MDB_EXT
# Extensões que saem do nome na série (encadeadas: `X.TXT.zip` -> `X`).
EXTENSOES = LEGIVEIS | {".xls"}
TMPDIR = os.environ.get("LAKE_TMPDIR") or None
CHUNKSIZE = 200_000

# Chave de cada série (nomes de coluna normalizados, os mesmos da staging), para
# separar a linha ALTERADA da removida + nova. Só entram chaves que o dbt já testa
# como únicas no arquivo (`unique` + `not_null` no bronze, que é `select *` da
# staging). Série sem chave compara o conjunto de linhas.
CHAVES: Dict[str, Tuple[str, ...]] = {
    "DATA_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA": ("apf",),
    "INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS_DATA": ("nu_apf",),
}

# Acima disso a comparação linha a linha fica de fora (só colunas e volume): o hash
# por linha custa 16 bytes por linha de cada versão na memória do worker.
LIMITE_LINHAS = 50_000_000
# Até aqui (linhas x colunas) o relatório também diz quais colunas mais mudaram: o
# hash de cada valor fica na memória (8 bytes por célula de cada versão).
LIMITE_CELULAS = 20_000_000

# A data no nome: AAAA[-_]MM[[-_]DD], DDMMAAAA ou MMAAAA, sem dígito colado.
_DATAS = [
    (
        re.compile(
            r"(?<!\d)((?:19|20)\d{2})[-_]?(0[1-9]|1[0-2])"
            r"(?:[-_]?(0[1-9]|[12]\d|3[01]))?(?!\d)"
        ),
        (1, 2, 3),
    ),
    (
        re.compile(r"(?<!\d)(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])((?:19|20)\d{2})(?!\d)"),
        (3, 2, 1),
    ),
    (re.compile(r"(?<!\d)(0[1-9]|1[0-2])((?:19|20)\d{2})(?!\d)"), (2, 1, 0)),
]


# Série e entrega anterior
def serie(chave: str) -> Tuple[str, Tuple[int, ...]]:
    """(série, data) de um arquivo: o nome sem extensão nem sufixo de colisão
    (`_0000`), com as datas trocadas por DATA."""
    nome = os.path.basename(chave)
    while os.path.splitext(nome)[1].lower() in EXTENSOES:
        nome = os.path.splitext(nome)[0]
    # o sufixo que o sftp_para_minio.py põe quando o nome já existe na raw
    nome = re.sub(r"_0\d{3}$", "", nome)
    datas: List[int] = []
    for padrao, (ano, mes, dia) in _DATAS:

        def trocar(m: "re.Match[str]") -> str:
            datas.extend(
                (
                    int(m.group(ano)),
                    int(m.group(mes)),
                    int(m.group(dia)) if dia and m.group(dia) else 0,
                )
            )
            return "DATA"

        nome = padrao.sub(trocar, nome)
    return nome, tuple(datas)


def anterior(chave: str, chaves: List[str]) -> Optional[str]:
    """A entrega da mesma série com a maior data antes da de `chave` (None: não há)."""
    nome, data = serie(chave)
    if not data:
        return None
    candidatas = []
    for outra in chaves:
        outra_nome, outra_data = serie(outra)
        if outra != chave and outra_nome == nome and outra_data < data:
            candidatas.append((outra_data, outra))
    return max(candidatas)[1] if candidatas else None


# Leitura: cada tabela do arquivo (o arquivo, ou cada aba do xlsx)
class _Tabela:
    """Colunas (normalizadas como na staging), as de PII, e as linhas em lotes."""

    def __init__(
        self,
        colunas: List[str],
        pii: List[str],
        lotes: Callable[[], Iterator[pd.DataFrame]],
    ) -> None:
        self.colunas = colunas
        self.pii = pii
        self.lotes = lotes


def _tabelas(path: str, ext: str) -> Dict[str, _Tabela]:
    if ext in TEXTO:
        tabela = _tabular(path)
        return {"": tabela} if tabela else {}
    if ext in MDB_EXT:
        return _mdb(path)
    if ext == ".zip":
        return _zip(path)
    return _xlsx(path)


@contextmanager
def _abrir(path: str, membro: Optional[str] = None) -> Iterator[IO[bytes]]:
    """O arquivo, ou um membro do zip lido em fluxo (sem extrair em disco)."""
    if membro is None:
        with open(path, "rb") as f:
            yield f
        return
    with zipfile.ZipFile(path) as zf, zf.open(membro) as f:
        yield f


def _tabular(
    path: str,
    encoding: Optional[str] = None,
    delim: Optional[str] = None,
    membro: Optional[str] = None,
) -> Optional[_Tabela]:
    """CSV/TXT: encoding e delimitador detectados pela amostra, como na staging."""
    if encoding is None or delim is None:
        with _abrir(path, membro) as f:
            amostra = f.read(65536)
        encoding = detectar_encoding(amostra)
        dialeto = detectar_dialeto(amostra, encoding)
        if dialeto is None:
            return None
        delim = dialeto[0]
    leitura: Dict[str, Any] = dict(
        sep=delim,
        dtype=str,
        encoding=encoding,
        encoding_errors="replace",
        quotechar='"',
    )
    with _abrir(path, membro) as f:
        header = [str(c) for c in pd.read_csv(f, nrows=0, **leitura).columns]
    colunas, _ = normalizar_colunas(header)

    def lotes() -> Iterator[pd.DataFrame]:
        with _abrir(path, membro) as f:
            for lote in pd.read_csv(
                f,
                keep_default_na=False,
                na_filter=False,
                chunksize=CHUNKSIZE,
                on_bad_lines="skip",
                engine="c",
                **leitura,
            ):
                lote.columns = colunas
                yield lote

    return _Tabela(colunas, _pii(header, colunas), lotes)


def _mdb(path: str) -> Dict[str, _Tabela]:
    """Uma tabela por tabela do Access, exportada para CSV em disco pelo mdbtools
    (UTF-8, vírgula), como no `raw_para_staging.py`."""
    if not mdb_disponivel():
        raise RuntimeError("mdbtools não encontrado no PATH (necessário para .mdb)")
    pasta = tempfile.mkdtemp(prefix="mdb-", dir=os.path.dirname(path))
    tabelas = {}
    for i, nome in enumerate(mdb_tabelas(path)):
        csv = os.path.join(pasta, f"{i}.csv")
        mdb_export_para_csv(path, nome, csv)
        tabela = _tabular(csv, MDB_ENCODING, MDB_DELIM) if os.path.getsize(csv) else None
        if tabela is not None and tabela.colunas:
            tabelas[nome] = tabela
    return tabelas


def _zip(path: str) -> Dict[str, _Tabela]:
    """Zip que pousou fechado (o CadÚnico): cada membro CSV/TXT, lido em fluxo."""
    with zipfile.ZipFile(path) as zf:
        membros = [
            n
            for n in zf.namelist()
            if not n.endswith("/") and os.path.splitext(n)[1].lower() in TEXTO
        ]
    tabelas = {}
    for membro in membros:
        tabela = _tabular(path, membro=membro)
        if tabela is not None:
            tabelas[os.path.basename(membro)] = tabela
    return tabelas


def _xlsx(path: str) -> Dict[str, _Tabela]:
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        abas = list(wb.sheetnames)
    finally:
        wb.close()
    tabelas = {}
    for aba in abas:
        df = pd.read_excel(path, sheet_name=aba, dtype=str, header=0, engine="openpyxl")
        df = df.fillna("")
        header = [str(c) for c in df.columns]
        colunas, _ = normalizar_colunas(header)
        df.columns = colunas
        tabelas[aba] = _Tabela(
            colunas, _pii(header, colunas), lambda df=df: iter([df])  # type: ignore[misc]
        )
    return tabelas


def _pii(header: List[str], colunas: List[str]) -> List[str]:
    """As colunas que o mascaramento trata como PII (pelo cabeçalho já decodificado)."""
    targets, _ = classificar(header, None)
    return [colunas[t["idx"]] for t in targets]


# Comparação
class _Perfil:
    """Uma passada pela tabela: linhas, hash de cada linha e da chave e, por coluna,
    se ela é constante (e com que valor), quantas linhas têm valor, o formato
    dominante dos valores e (até LIMITE_CELULAS) o hash de cada valor."""

    def __init__(self, tabela: _Tabela, colunas: List[str], chave: Tuple[str, ...]):
        self.linhas = 0
        self.constante: Dict[str, Optional[str]] = {}
        self.variavel: set = set()
        self.preenchidas = dict.fromkeys(colunas, 0)
        self.formato: Dict[str, Optional[str]] = {}
        h_linhas: Optional[List[np.ndarray]] = [] if colunas else None
        h_chave: Optional[List[np.ndarray]] = [] if chave else None
        h_colunas: Optional[Dict[str, List[np.ndarray]]] = {c: [] for c in colunas}
        for lote in tabela.lotes():
            if not self.linhas:
                self.formato = {c: _formato_dominante(lote[c]) for c in colunas}
            self.linhas += len(lote)
            if self.linhas > LIMITE_LINHAS:
                h_linhas = h_chave = None
            if self.linhas * max(len(colunas), 1) > LIMITE_CELULAS:
                h_colunas = None
            soma = np.zeros(len(lote), dtype="uint64")
            for c in colunas:
                valores = lote[c]
                self._observar(c, valores)
                if h_linhas is None and h_colunas is None:
                    continue
                h = _hash_valores(valores.to_numpy())
                with np.errstate(over="ignore"):
                    soma += _peso(c) * h
                if h_colunas is not None:
                    h_colunas[c].append(h)
            if h_linhas is not None:
                h_linhas.append(soma)
            if h_chave is not None:
                h_chave.append(
                    pd.util.hash_pandas_object(lote[list(chave)], index=False).to_numpy()
                )
        self.h_linhas = _juntar(h_linhas) if h_linhas is not None else None
        self.h_chave = _juntar(h_chave) if h_chave is not None else None
        self.h_colunas = (
            {c: _juntar(h) for c, h in h_colunas.items()}
            if h_colunas is not None
            else None
        )

    def _observar(self, coluna: str, valores: pd.Series) -> None:
        self.preenchidas[coluna] += int((valores.str.strip() != "").sum())
        if coluna not in self.variavel and len(valores):
            primeiro = self.constante.setdefault(coluna, valores.iloc[0])
            if not (valores == primeiro).all():
                self.variavel.add(coluna)

    def valor_unico(self, coluna: str) -> Optional[str]:
        """O valor da coluna se ele é o mesmo em todas as linhas (senão None)."""
        return None if coluna in self.variavel else self.constante.get(coluna)

    def tirar(self, coluna: str) -> None:
        """Tira do hash de cada linha uma coluna constante (o carimbo da entrega)."""
        valor = self.valor_unico(coluna)
        if self.h_linhas is not None and valor is not None:
            with np.errstate(over="ignore"):
                self.h_linhas -= _peso(coluna) * _hash_valores(np.array([valor]))[0]


def _formato_dominante(valores: pd.Series) -> Optional[str]:
    """O formato de pelo menos 90% dos valores preenchidos, ou None se a coluna não
    tem um formato regular (texto livre). Dígitos viram 9 e letras viram A:
    `1.234,56` -> `9.9,9`, `31/08/2026` -> `9/9/9`."""
    preenchidos = valores[valores.str.strip() != ""]
    if preenchidos.empty:
        return None
    formatos = preenchidos.str.replace(r"\d+", "9", regex=True).str.replace(
        r"[^\W\d_]+", "A", regex=True
    )
    contagem = formatos.value_counts()
    return str(contagem.index[0]) if contagem.iloc[0] >= 0.9 * len(formatos) else None


def _peso(coluna: str) -> np.uint64:
    """Peso ímpar por nome de coluna: o hash da linha não depende da ordem delas."""
    return np.uint64(_hash_valores(np.array([coluna]))[0] | np.uint64(1))


def _hash_valores(valores: np.ndarray) -> np.ndarray:
    return pd.util.hash_array(valores.astype(object))


def _juntar(partes: List[np.ndarray]) -> np.ndarray:
    return np.concatenate(partes) if partes else np.empty(0, dtype="uint64")


def comparar(
    novo: _Tabela, velho: _Tabela, chave: Tuple[str, ...] = ()
) -> Dict[str, Any]:
    """O drift entre duas versões de uma tabela (só estrutura e contagens).

    O carimbo da entrega (coluna com um valor só no arquivo inteiro, diferente entre
    as duas versões, como `dt_movimento`) sai da comparação de linhas: senão toda
    linha pareceria alterada.
    """
    pii = set(novo.pii) | set(velho.pii)
    comuns_todas = [c for c in velho.colunas if c in novo.colunas]
    comuns = [c for c in comuns_todas if c not in pii]
    chave = tuple(c for c in chave if c in comuns) if chave else ()
    p_novo, p_velho = _Perfil(novo, comuns, chave), _Perfil(velho, comuns, chave)
    formato_mudou = {
        c: {"anterior": p_velho.formato[c], "atual": p_novo.formato[c]}
        for c in comuns
        if None not in (p_velho.formato[c], p_novo.formato[c])
        and p_velho.formato[c] != p_novo.formato[c]
    }
    carimbo = [
        c
        for c in comuns
        if c not in chave
        and c not in formato_mudou
        and None not in (p_novo.valor_unico(c), p_velho.valor_unico(c))
        and p_novo.valor_unico(c) != p_velho.valor_unico(c)
    ]
    for c in carimbo:
        p_novo.tirar(c)
        p_velho.tirar(c)
    relatorio: Dict[str, Any] = {
        "colunas": {
            "adicionadas": [c for c in novo.colunas if c not in velho.colunas],
            "removidas": [c for c in velho.colunas if c not in novo.colunas],
            "ordem_mudou": [c for c in novo.colunas if c in comuns_todas] != comuns_todas,
            "esvaziadas": [
                c for c in comuns if p_velho.preenchidas[c] and not p_novo.preenchidas[c]
            ],
            "ficaram_constantes": [
                c
                for c in comuns
                if c in p_velho.variavel
                and c not in p_novo.variavel
                and p_novo.linhas > 1
                and p_novo.preenchidas[c]
            ],
            "passaram_a_ter_valor": [
                c for c in comuns if p_novo.preenchidas[c] and not p_velho.preenchidas[c]
            ],
            "formato_mudou": formato_mudou,
            "carimbo_da_entrega": carimbo,
            "pii_fora_da_comparacao": sorted(pii & set(comuns_todas)),
        },
        "linhas": {
            "anterior": p_velho.linhas,
            "atual": p_novo.linhas,
            "variacao": p_novo.linhas - p_velho.linhas,
        },
    }
    comparadas = len(comuns) - len(carimbo)
    if p_novo.h_linhas is None or p_velho.h_linhas is None:
        motivo = "nenhuma coluna em comum" if not comuns else "acima do limite de linhas"
        relatorio["conteudo"] = {"comparado": False, "motivo": motivo}
    elif not chave:
        relatorio["conteudo"] = _por_linha(p_novo.h_linhas, p_velho.h_linhas, comparadas)
    else:
        assert p_novo.h_chave is not None and p_velho.h_chave is not None
        relatorio["conteudo"] = _por_registro(
            p_novo.h_chave,
            p_novo.h_linhas,
            p_velho.h_chave,
            p_velho.h_linhas,
            comparadas,
            chave,
        )
    if relatorio["conteudo"]["comparado"]:
        relatorio["conteudo"]["colunas_que_mais_mudaram"] = _mais_mudaram(
            p_novo, p_velho, [c for c in comuns if c not in carimbo]
        )
    relatorio["drift"] = _tipos(relatorio)
    return relatorio


def _mais_mudaram(
    novo: _Perfil, velho: _Perfil, colunas: List[str], top: int = 10
) -> Optional[List[Dict[str, Any]]]:
    """As colunas com mais linhas trazendo um valor que não existia na entrega
    anterior (o que explica as linhas alteradas). None acima de LIMITE_CELULAS."""
    if novo.h_colunas is None or velho.h_colunas is None or not novo.linhas:
        return None
    mudancas: List[Dict[str, Any]] = []
    for c in colunas:
        n = int((~np.isin(novo.h_colunas[c], np.unique(velho.h_colunas[c]))).sum())
        if n:
            mudancas.append(
                {
                    "coluna": c,
                    "linhas_com_valor_novo": n,
                    "pct": round(100 * n / novo.linhas, 1),
                }
            )
    return sorted(mudancas, key=lambda m: -m["linhas_com_valor_novo"])[:top]


def _por_linha(h_novo: np.ndarray, h_velho: np.ndarray, n_colunas: int) -> Dict[str, Any]:
    """Diferença dos multiconjuntos de linhas: sem chave, uma linha alterada conta
    como uma removida mais uma nova."""
    v_novo, n_novo = np.unique(h_novo, return_counts=True)
    v_velho, n_velho = np.unique(h_velho, return_counts=True)
    _, i_novo, i_velho = np.intersect1d(v_novo, v_velho, return_indices=True)
    iguais = int(np.minimum(n_novo[i_novo], n_velho[i_velho]).sum())
    return {
        "comparado": True,
        "colunas_comparadas": n_colunas,
        "chave": None,
        "linhas_iguais": iguais,
        "linhas_novas": len(h_novo) - iguais,
        "linhas_removidas": len(h_velho) - iguais,
    }


def _por_registro(
    k_novo: np.ndarray,
    h_novo: np.ndarray,
    k_velho: np.ndarray,
    h_velho: np.ndarray,
    n_colunas: int,
    chave: Tuple[str, ...],
) -> Dict[str, Any]:
    (u_novo, c_novo), dup_novo = _agrupar(k_novo, h_novo)
    (u_velho, c_velho), dup_velho = _agrupar(k_velho, h_velho)
    _, i_novo, i_velho = np.intersect1d(u_novo, u_velho, return_indices=True)
    alterados = int((c_novo[i_novo] != c_velho[i_velho]).sum())
    return {
        "comparado": True,
        "colunas_comparadas": n_colunas,
        "chave": list(chave),
        "registros_iguais": len(i_novo) - alterados,
        "registros_novos": len(u_novo) - len(i_novo),
        "registros_removidos": len(u_velho) - len(i_velho),
        "registros_alterados": alterados,
        "chaves_duplicadas": {"anterior": dup_velho, "atual": dup_novo},
    }


def _agrupar(
    chaves: np.ndarray, linhas: np.ndarray
) -> Tuple[Tuple[np.ndarray, np.ndarray], int]:
    """((chaves únicas, conteúdo de cada uma), linhas com chave repetida).

    O conteúdo é a soma, com estouro em uint64, dos hashes das linhas da chave: não
    depende da ordem das linhas.
    """
    ordem = np.argsort(chaves, kind="stable")
    chaves, linhas = chaves[ordem], linhas[ordem]
    unicas, inicio = np.unique(chaves, return_index=True)
    with np.errstate(over="ignore"):
        conteudo = np.add.reduceat(linhas, inicio) if len(linhas) else linhas
    return (unicas, conteudo), int(len(chaves) - len(unicas))


def _tipos(relatorio: Dict[str, Any]) -> List[str]:
    colunas, conteudo = relatorio["colunas"], relatorio["conteudo"]
    tipos = []
    if colunas["adicionadas"]:
        tipos.append("colunas_adicionadas")
    if colunas["removidas"]:
        tipos.append("colunas_removidas")
    if colunas["ordem_mudou"]:
        tipos.append("ordem_das_colunas")
    if colunas["esvaziadas"]:
        tipos.append("colunas_esvaziadas")
    if colunas["ficaram_constantes"]:
        tipos.append("colunas_ficaram_constantes")
    if colunas["formato_mudou"]:
        tipos.append("formato_das_colunas")
    if relatorio["linhas"]["variacao"] < 0:
        tipos.append("volume_caiu")
    if conteudo.get("linhas_removidas") or conteudo.get("registros_removidos"):
        tipos.append("linhas_removidas")
    if conteudo.get("registros_alterados"):
        tipos.append("linhas_alteradas")
    return tipos


def comparar_arquivos(
    novo: str, velho: str, ext_novo: str, ext_velho: str, chave: Tuple[str, ...] = ()
) -> Dict[str, Any]:
    """O relatório de um arquivo: uma comparação por tabela (aba do xlsx, tabela do
    .mdb) presente nos dois."""
    t_novo, t_velho = _tabelas(novo, ext_novo), _tabelas(velho, ext_velho)
    if not t_novo or not t_velho:
        return {
            "comparado": False,
            "motivo": "arquivo sem cabeçalho legível",
            "drift": [],
        }
    # arquivo de uma tabela só (csv, xlsx de uma aba): compara uma com a outra
    if len(t_novo) == 1 and len(t_velho) == 1:
        pares = {"": (next(iter(t_novo.values())), next(iter(t_velho.values())))}
    else:
        pares = {aba: (t_novo[aba], t_velho[aba]) for aba in t_novo if aba in t_velho}
    tabelas = {aba: comparar(n, v, chave) for aba, (n, v) in pares.items()}
    abas = {
        "adicionadas": [a for a in t_novo if a not in t_velho and len(t_novo) > 1],
        "removidas": [a for a in t_velho if a not in t_novo and len(t_velho) > 1],
    }
    drift = sorted({tipo for r in tabelas.values() for tipo in r["drift"]})
    if abas["adicionadas"]:
        drift.append("abas_adicionadas")
    if abas["removidas"]:
        drift.append("abas_removidas")
    return {"abas": abas, "tabelas": tabelas, "drift": drift}


# Execução
def _baixar(minio: Any, chave: str, pasta: str) -> str:
    """Baixa com a extensão de origem (o openpyxl recusa o `.part` do download)."""
    parcial = minio.baixar_para_tempfile(chave, os.path.splitext(chave)[1], pasta)
    destino = os.path.join(pasta, os.path.basename(chave))
    os.replace(parcial, destino)
    return destino


def detectar(chaves: List[str]) -> List[str]:
    """Compara cada arquivo pousado com a entrega anterior; devolve os relatórios.

    Ponto de entrada da task `detectar_drift` da `sftp_ingest_dag`.
    """
    from cliente_minio import ClienteMinio

    minio = ClienteMinio()
    tamanhos = {
        k: s
        for k, s in minio.listar_objetos(RAW_PREFIX)
        if os.path.splitext(k)[1].lower() in LEGIVEIS
    }
    gerado_em = datetime.now(timezone.utc)
    relatorios = []
    for chave in dict.fromkeys(chaves):
        ext = os.path.splitext(chave)[1].lower()
        if ext not in LEGIVEIS or chave in COLUNAS_POR_POSICAO:
            continue
        chave_anterior = anterior(chave, list(tamanhos))
        if chave_anterior is None:
            log.info("drift: %s é a primeira entrega da série", chave)
            continue
        nome_serie = serie(chave)[0]
        try:
            with tempfile.TemporaryDirectory(prefix="drift-", dir=TMPDIR) as tmp:
                os.makedirs(os.path.join(tmp, "novo"))
                os.makedirs(os.path.join(tmp, "anterior"))
                comparacao = comparar_arquivos(
                    _baixar(minio, chave, os.path.join(tmp, "novo")),
                    _baixar(minio, chave_anterior, os.path.join(tmp, "anterior")),
                    ext,
                    os.path.splitext(chave_anterior)[1].lower(),
                    CHAVES.get(nome_serie, ()),
                )
        except Exception as e:  # noqa: BLE001 — drift só avisa, nunca derruba a ingestão
            log.warning("drift: não foi possível comparar %s: %s", chave, e)
            continue
        relatorio = {
            "serie": nome_serie,
            "atual": chave,
            "anterior": chave_anterior,
            "gerado_em": gerado_em.isoformat(),
            "bytes": {
                "anterior": tamanhos.get(chave_anterior),
                "atual": tamanhos.get(chave),
            },
            **comparacao,
        }
        key = (
            f"{DRIFT_PREFIX}{re.sub(r'[^A-Za-z0-9._-]', '_', nome_serie)}/"
            f"{gerado_em:%Y%m%dT%H%M%S}__{os.path.basename(chave)}.json"
        )
        minio.put_object(
            key, json.dumps(relatorio, ensure_ascii=False, indent=2).encode("utf-8")
        )
        relatorios.append(key)
        log.log(
            logging.WARNING if relatorio["drift"] else logging.INFO,
            "drift %s vs %s: %s | %s",
            chave,
            chave_anterior,
            relatorio["drift"] or "sem drift estrutural",
            json.dumps(comparacao.get("tabelas", comparacao), ensure_ascii=False),
        )
    log.info("drift: %d relatório(s) em %s", len(relatorios), DRIFT_PREFIX)
    return relatorios
