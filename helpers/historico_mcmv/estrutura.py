"""analytics -> cleansed, parte estrutural: duplicação 2x, cabeçalho, descartes e conservação."""

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from historico_mcmv.erros import ErroHistorico
from historico_mcmv.nomes import norm_header, normalizar_colunas

# 3. analytics -> cleansed: tratamento estrutural de um arquivo
# A ordem importa: (1) desfazer a duplicação 2x antes de mexer no cabeçalho (senão a 2ª
# cópia da linha de cabeçalho vira dado), (2) cabeçalho, (3) separar o que não é dado,
# (4) tirar coluna vazia sem nome, (5) conferir que nada se perdeu. Os valores não mudam:
# continuam texto, sem trim e sem tipagem (isso é da silver).

MANTER, PROMOVER, COMPOR, NOMEAR, DIVIDIR, EXCLUIR = (
    "manter",
    "promover",
    "compor",
    "nomear",
    "dividir",
    "excluir",
)
CABECALHO, TITULO, VAZIA, COPIA_CABECALHO = "cabecalho", "titulo", "vazia", "copia_cabecalho"
SUBTITULO, TOTAL, RODAPE = "subtitulo", "total", "rodape"
ZERADA = "zerada"  # linha-modelo do Excel: só zeros (e no máximo um rótulo), com regra no mapa
DUPLICADA = "duplicada"  # linha idêntica repetida em família com chave
LINHA_ORIGEM = "_linha_origem"
_NUMERO = re.compile(r"^[\d\s.,%:/()+-]+$")

# total: começa ("TOTAL FASE I"), termina ("SP Total") ou "total fase" no meio; "BRASIL"
# sozinho é o total nacional. "TOTAL" no meio de um nome ("BELLA CITTA TOTAL VILLE") é dado.
_TOTAL = re.compile(r"^total\b|\btotal$|\btotal fase\b|^brasil$")
_DATA_SOLTA = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^\d{2}/\d{2}/\d{4}$")
_SEM_NOME = re.compile(r"^(unnamed|coluna)_\d+$")


@dataclass
class Regra:
    """Cabeçalho de um bloco. `linha` e `fim` contam no arquivo já sem a duplicação 2x.
    Com tabelas empilhadas, uma regra por bloco, cada uma com `bloco` e `fim` (exclusivo;
    o último bloco vai até o fim)."""

    acao: str = MANTER
    linha: int = 0
    motivo: str = ""
    bloco: str = ""
    fim: Optional[int] = None
    colunas: List[str] = field(default_factory=list)  # nomes para nomear/dividir
    zeradas: bool = False  # descarta as linhas-modelo zeradas (regra explícita no mapa)
    totais: List[int] = field(default_factory=list)  # _linha_origem de total sem rótulo (mapa)


@dataclass
class Resultado:
    dados: pd.DataFrame  # colunas finais + _linha_origem
    descartes: pd.DataFrame  # _linha_origem, motivo, valores
    colunas_removidas: List[str] = field(default_factory=list)
    linhas_entrada: int = 0
    colunas_vazias: List[str] = field(default_factory=list)  # com nome, mantidas

    @property
    def por_motivo(self) -> Dict[str, int]:
        return self.descartes.motivo.value_counts().to_dict() if len(self.descartes) else {}


def desfazer_duplicacao(df: pd.DataFrame) -> pd.DataFrame:
    """De cada grupo de n linhas idênticas, mantém as n/2 primeiras ocorrências.

    A exportação do dump gravou toda linha 2x, em páginas embaralhadas. Repetição legítima
    (4 cópias = 2 reais) continua; grupo ímpar quer dizer que a premissa não vale.
    """
    df = df.reset_index(drop=True)
    if df.empty:
        return df.assign(**{LINHA_ORIGEM: pd.Series(dtype="int64")})
    chave = pd.util.hash_pandas_object(df, index=False)
    n = chave.map(chave.value_counts())
    impares = int((n % 2 == 1).sum())
    if impares:
        raise ErroHistorico(f"{impares} linha(s) em grupo de contagem ímpar: a duplicação não é 2x")
    manter = chave.groupby(chave).cumcount() < n // 2
    return df[manter].assign(**{LINHA_ORIGEM: df.index[manter]}).reset_index(drop=True)


def _texto(v) -> str:
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _nomes_compostos(grupo: List[str], coluna: List[str]) -> List[str]:
    """Nome = grupo + coluna, para cabeçalho em dois níveis. O grupo (célula mesclada) vale
    para a direita só onde há sub-cabeçalho; rótulo de grupo centralizado passa ao próximo
    grupo quando um sub-cabeçalho se repete."""
    atual, nomes, vistos = "", [], set()
    for i, (g, c) in enumerate(zip(grupo, coluna)):
        if g.strip() and g.strip() != atual:
            atual, vistos = g.strip(), set()
        sub = c.strip()
        if sub and sub in vistos:
            proximo = next((x.strip() for x in grupo[i + 1 :] if x.strip()), "")
            if proximo and proximo != atual:
                atual, vistos = proximo, set()
        if sub:
            vistos.add(sub)
            partes = [p for p in (atual, sub) if p]
        else:
            partes = [g.strip()] if g.strip() else []
        nomes.append(" ".join(dict.fromkeys(partes)))
    return nomes


def _vazio_descartes() -> pd.DataFrame:
    return pd.DataFrame(
        {LINHA_ORIGEM: pd.Series(dtype="int64"), "motivo": pd.Series(dtype=str), "valores": []}
    )


def _descartes(df: pd.DataFrame, cols: List[str], motivo) -> pd.DataFrame:
    if df.empty:
        return _vazio_descartes()
    return pd.DataFrame(
        {
            LINHA_ORIGEM: df[LINHA_ORIGEM].to_numpy(),
            "motivo": motivo if isinstance(motivo, str) else motivo.to_numpy(),
            "valores": [[_texto(v) for v in linha] for linha in df[cols].itertuples(index=False)],
        }
    )


def aplicar_cabecalho(
    df: pd.DataFrame, regra: Regra
) -> Tuple[pd.DataFrame, pd.DataFrame, List[List[str]]]:
    """(dados, linhas consumidas, linhas de referência do cabeçalho)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    if regra.acao == MANTER:
        return df, _vazio_descartes(), [cols]
    if regra.acao == NOMEAR:
        if len(regra.colunas) != len(cols):
            raise ErroHistorico(
                f"nomear: o mapa dá {len(regra.colunas)} nome(s) para {len(cols)} coluna(s)"
            )
        finais, _ = normalizar_colunas(regra.colunas)
        dados = df.copy()
        dados.columns = finais + [LINHA_ORIGEM]
        return dados, _vazio_descartes(), [finais]
    n_cab = {PROMOVER: 1, COMPOR: 2}.get(regra.acao)
    if n_cab is None:
        raise ErroHistorico(f"ação de cabeçalho desconhecida: {regra.acao}")
    fim = regra.linha + n_cab
    if len(df) < fim:
        raise ErroHistorico(
            f"arquivo tem {len(df)} linha(s); cabeçalho pedido até a linha {fim - 1}"
        )
    linhas_cab = [[_texto(v) for v in df.iloc[i][cols]] for i in range(regra.linha, fim)]
    nomes = linhas_cab[0] if regra.acao == PROMOVER else _nomes_compostos(*linhas_cab)
    # "%" some na normalização ("% Meta R$" e "Meta R$" virariam o mesmo nome): vira "pct"
    finais, _ = normalizar_colunas([n.replace("%", " pct ") for n in nomes])
    consumidas = pd.concat(
        [
            _descartes(df.iloc[: regra.linha], cols, TITULO),
            _descartes(df.iloc[regra.linha : fim], cols, CABECALHO),
        ]
    )
    dados = df.iloc[fim:].copy()
    dados.columns = finais + [LINHA_ORIGEM]
    return dados.reset_index(drop=True), consumidas, linhas_cab


def _normalizado(serie: pd.Series) -> pd.Series:
    unicos = {v: norm_header(v) for v in serie.dropna().unique()}
    return serie.map(unicos).fillna("")


def _copias_de_cabecalho(df: pd.DataFrame, cols: List[str], refs: List[List[str]]) -> pd.Series:
    """Linhas cujas células preenchidas repetem uma linha de cabeçalho na mesma posição
    (pelo menos 2 células, 80% delas iguais, comparando normalizado)."""
    achou = pd.Series(False, index=df.index)
    for ref in refs:
        ref_norm = [norm_header(r) for r in ref]
        posicoes = [i for i, r in enumerate(ref_norm) if r]
        if len(posicoes) < 2:
            continue
        # pré-filtro barato nas 3 primeiras posições; só as candidatas são normalizadas
        cand = pd.Series(False, index=df.index)
        for i in posicoes[:3]:
            alvo = {ref[i].strip().lower(), ref_norm[i]}
            cand |= df[cols[i]].fillna("").str.strip().str.lower().isin(alvo)
        if not cand.any():
            continue
        sub = df[cand]
        iguais = pd.DataFrame(
            {i: _normalizado(sub[cols[i]]) == ref_norm[i] for i in posicoes}, index=sub.index
        ).sum(axis=1)
        cheias = (sub[[cols[i] for i in posicoes]].apply(lambda s: s.str.strip() != "")).sum(axis=1)
        achou[sub.index[(iguais >= 2) & (iguais >= 0.8 * cheias)]] = True
    return achou


def _marcar_rodape(motivo: pd.Series, n_cheias: pd.Series) -> None:
    """Linhas de uma célula só no fim do bloco (total geral sem rótulo)."""
    for i in reversed(motivo.index):
        if motivo[i] == "" and n_cheias[i] == 1:
            motivo[i] = RODAPE
        elif motivo[i] == "":
            break


_ZERO = re.compile(r"^-?0+([.,]0+)?$")


def _zeradas(limpo: pd.DataFrame) -> pd.Series:
    """Linha só com zeros e no máximo um rótulo de TEXTO (ex.: o produto). Um número
    diferente de zero, mesmo sem identificação, é informação e mantém a linha."""
    zero = limpo.apply(lambda s: s.str.match(_ZERO))
    texto = (limpo != "") & ~zero
    numero = texto & limpo.apply(lambda s: s.str.match(r"^-?[\d.,]+$"))
    return (texto.sum(axis=1) <= 1) & (numero.sum(axis=1) == 0) & (zero.sum(axis=1) >= 2)


def separar_descartes(
    df: pd.DataFrame, refs: List[List[str]], zeradas: bool = False, totais: Iterable[int] = ()
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separa linhas vazias, cópias de cabeçalho, totais, subtítulos, rodapé e, quando o
    mapa pede, as linhas-modelo zeradas e os totais sem rótulo (pela `_linha_origem`)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    if df.empty or not cols:
        return df, _vazio_descartes()
    limpo = df[cols].apply(lambda s: s.fillna("").str.strip())
    cheia = limpo != ""
    n_cheias = cheia.sum(axis=1)
    primeira = limpo.where(cheia).bfill(axis=1).iloc[:, 0].fillna("")
    motivo = pd.Series("", index=df.index)
    motivo[limpo.isin(["", "-"]).all(axis=1)] = VAZIA
    if zeradas:
        motivo[(motivo == "") & _zeradas(limpo)] = ZERADA
    motivo[(motivo == "") & _copias_de_cabecalho(df, cols, refs)] = COPIA_CABECALHO
    total = primeira.str.replace("\xa0", " ").str.lower().str.contains(_TOTAL)
    motivo[(motivo == "") & total] = TOTAL
    totais = list(totais)
    if totais:
        motivo[(motivo == "") & df[LINHA_ORIGEM].isin(totais)] = TOTAL
    if len(cols) >= 4:
        # uma célula só, de texto ou data solta (carimbo de posição); número sozinho fica
        texto = ~primeira.str.match(_NUMERO) | primeira.str.match(_DATA_SOLTA)
        motivo[(motivo == "") & (n_cheias == 1) & texto] = SUBTITULO
        _marcar_rodape(motivo, n_cheias)
    sai = motivo != ""
    return df[~sai].reset_index(drop=True), _descartes(df[sai], cols, motivo[sai])


def remover_colunas_vazias(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """(dados, removidas, vazias mantidas). Só sai a coluna vazia SEM nome (preenchimento
    do Excel); a com nome fica, para guardar o rastro do que a fonte tinha."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    vazias = [c for c in cols if not (df[c].fillna("").str.strip() != "").any()]
    removidas = [c for c in vazias if _SEM_NOME.match(c)]
    return df.drop(columns=removidas), removidas, [c for c in vazias if c not in removidas]


def _cheias(df: pd.DataFrame) -> int:
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    return int(sum((df[c].fillna("").str.strip() != "").sum() for c in cols))


def _cheias_descartes(descartes: pd.DataFrame) -> int:
    return int(sum(sum(v.strip() != "" for v in vals) for vals in descartes["valores"]))


def conferir(entrada: pd.DataFrame, resultados: List[Resultado]) -> None:
    """Nada se perde e nada se inventa: linhas e células preenchidas da entrada = 2 x
    (saída + descartes), e nenhuma linha da saída repete o cabeçalho final."""
    n_saida = sum(len(r.dados) for r in resultados)
    n_desc = sum(len(r.descartes) for r in resultados)
    if len(entrada) != 2 * (n_saida + n_desc):
        raise ErroHistorico(
            f"linhas não fecham: entrada {len(entrada)} != 2 x (saída {n_saida} + descartes {n_desc})"
        )
    cheias_entrada = _cheias(entrada)
    cheias_saida = sum(_cheias(r.dados) + _cheias_descartes(r.descartes) for r in resultados)
    if cheias_entrada != 2 * cheias_saida:
        raise ErroHistorico(
            f"células preenchidas não fecham: entrada {cheias_entrada} != 2 x {cheias_saida}"
        )
    for r in resultados:
        cols = [c for c in r.dados.columns if c != LINHA_ORIGEM]
        if len(r.dados) and _copias_de_cabecalho(r.dados, cols, [cols]).any():
            raise ErroHistorico("linha igual ao cabeçalho ficou na saída")


def dividir_coluna(df: pd.DataFrame, nomes: List[str]) -> pd.DataFrame:
    """Separa a coluna única com os campos colados por "|" (tabelas `tab_*` do BB de 2013).
    Toda linha tem o mesmo número de campos e juntar as partes devolve o texto original."""
    if df.shape[1] != 1:
        raise ErroHistorico(f"dividir: esperava 1 coluna, há {df.shape[1]}")
    texto = df.iloc[:, 0].fillna("")
    erradas = int((texto.str.count(r"\|") != len(nomes) - 1).sum())
    if erradas:
        raise ErroHistorico(f"dividir: {erradas} linha(s) sem {len(nomes)} campos")
    partes = texto.str.split("|", n=len(nomes) - 1, expand=True, regex=False)
    if not (partes.agg("|".join, axis=1) == texto).all():
        raise ErroHistorico("dividir: juntar as partes não devolve o texto original")
    partes.columns, _ = normalizar_colunas(nomes)
    return partes


def _tratar_bloco(unico: pd.DataFrame, regra: Regra, n_entrada: int) -> Resultado:
    dados, consumidas, refs = aplicar_cabecalho(unico, regra)
    dados, outros = separar_descartes(dados, refs, regra.zeradas, regra.totais)
    dados, removidas, vazias = remover_colunas_vazias(dados)
    descartes = pd.concat([consumidas, outros]).sort_values(LINHA_ORIGEM).reset_index(drop=True)
    return Resultado(dados, descartes, removidas, n_entrada, vazias)


def tratar_blocos(df: pd.DataFrame, regras: List[Regra]) -> List[Tuple[str, Resultado]]:
    """Os passos 1-5 sobre as colunas de dado de um arquivo. Com mais de uma regra, cada
    bloco vai de onde o anterior terminou até o seu `fim`. Devolve [(bloco, resultado)]."""
    regras = sorted(regras, key=lambda r: r.linha)
    if len(regras) > 1 and any(r.fim is None for r in regras[:-1]):
        raise ErroHistorico("arquivo com blocos: todo bloco, menos o último, precisa de fim")
    entrada = df.reset_index(drop=True)
    if any(r.acao == DIVIDIR for r in regras):
        if len(regras) != 1:
            raise ErroHistorico("dividir não combina com blocos")
        entrada = dividir_coluna(entrada, regras[0].colunas)  # conferência já dividida
        regras = [
            Regra(
                MANTER,
                0,
                regras[0].motivo,
                regras[0].bloco,
                zeradas=regras[0].zeradas,
                totais=regras[0].totais,
            )
        ]
    unico = desfazer_duplicacao(entrada)
    saida, ini = [], 0
    for regra in regras:
        fim = len(unico) if regra.fim is None else regra.fim
        if not ini <= regra.linha < fim <= len(unico):
            raise ErroHistorico(f"bloco {regra.bloco!r} fora do arquivo: {ini}..{fim}")
        relativa = Regra(
            regra.acao,
            regra.linha - ini,
            regra.motivo,
            regra.bloco,
            colunas=regra.colunas,
            zeradas=regra.zeradas,
            totais=regra.totais,
        )
        saida.append((regra.bloco, _tratar_bloco(unico.iloc[ini:fim], relativa, len(entrada))))
        ini = fim
    if ini != len(unico):
        raise ErroHistorico(f"linhas {ini}..{len(unico)} não pertencem a nenhum bloco")
    _conferir_totais(regras, saida)
    conferir(entrada, [r for _, r in saida])
    return saida


def _conferir_totais(regras: List[Regra], saida: List[Tuple[str, Resultado]]) -> None:
    """Toda linha de total apontada no mapa tem de ter saído como total (número errado no
    mapa não pode passar calado)."""
    pedidas = {n for r in regras for n in r.totais}
    if not pedidas:
        return
    achadas = {
        int(n)
        for _, res in saida
        for n in res.descartes.loc[res.descartes.motivo == TOTAL, LINHA_ORIGEM]
    }
    if pedidas - achadas:
        raise ErroHistorico(f"total do mapa não encontrado nas linhas {sorted(pedidas - achadas)}")


def tratar_arquivo(df: pd.DataFrame, regra: Optional[Regra] = None) -> Resultado:
    """Um arquivo com uma tabela só (o caso comum)."""
    return tratar_blocos(df, [regra or Regra()])[0][1]
