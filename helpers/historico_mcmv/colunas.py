"""Evidência para o grafo de nomes de coluna (como cada dado muda de nome no tempo)."""

import hashlib
import re
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import pandas as pd

from historico_mcmv.cleansed import TRATADO

# 4. colunas: evidência para o grafo de nomes (como cada dado muda de nome no tempo)

AMOSTRA_VALORES = 5_000  # valores distintos por coluna; basta para a sobreposição
NOTA_PROVAVEL = 0.6
PESOS = {"valores": 0.5, "nome": 0.3, "formato": 0.2}

# prefixos de tipo que mudam entre layouts sem mudar o dado
_PREFIXOS_TIPO = re.compile(
    r"^(cod|codigo|nu|num|numero|no|nom|nome|icodigo|id|txt|dsc|vlr|valor|qtd|qt|quantidade|"
    r"dt|dte|dat|data|bln)_+"
)

# o bext cola o tipo no nome (`vnome_municipio`, `csigla_uf`, `icodigo_empreendimento`)
_PREFIXO_COLADO = re.compile(
    r"^[vcidnb](?=(nome|sigla|codigo|ano|mes|faixa|instituicao|programa|valor|qtd|data|porte|"
    r"municipio|uf|situacao|descricao|percentual|numero|tipo|origem|empreendimento)(_|$))"
)

# conceito canônico (glossário: dbt/mcid/models/docs/glossario-mcid.md) -> padrão do nome
CONCEITOS = {
    "apf": r"^(cod_?|nu_?|numero_?|icodigo_?|id_?)?apf$",
    "codigo_empreendimento": r"^(i?cod(igo)?_?(do_)?|nu_)empreendimento(_bb)?$",
    "nome_empreendimento": r"^(v?nome_?(do_)?|txt_|dsc_|no_)?empreendimento$|^nomeempreendimento$",
    "uf": r"(^|_)(c?sigla_)?uf$",
    "municipio": r"^(v?nome_?(do_)?)?municipio$|^nomemunicipio$",
    "codigo_ibge_municipio": r"ibge|^codmunic",
    "quantidade_uh": r"^(qtd_|qt_|quantidade_(de_)?)?(uh|uhs|unidades)$",
    "faixa": r"^c?faixa(_de_renda)?$",
    "dt_contratacao": r"^(dat|data|dt|dte)_?(da_|de_)?contratacao(_bb)?$|^dte_assin_contrat",
    "cnpj": r"(^|_)cnpj($|_)",
}
_CONCEITOS_RX = {k: re.compile(v) for k, v in CONCEITOS.items()}


def conceito(coluna: str) -> Optional[str]:
    achados = [k for k, rx in _CONCEITOS_RX.items() if rx.search(coluna)]
    return achados[0] if len(achados) == 1 else None


def radical(coluna: str) -> str:
    """O nome sem os prefixos de tipo (`cod_`, `nu_`, `icodigo_`, `txt_`...)."""
    anterior = None
    while anterior != coluna:
        anterior, coluna = coluna, _PREFIXOS_TIPO.sub("", _PREFIXO_COLADO.sub("", coluna))
    return coluna


def _normalizar_valor(v: str) -> str:
    v = v.strip()
    return v[:-2] if re.fullmatch(r"-?\d+\.0", v) else v


def perfil(serie: pd.Series) -> dict:
    """Valores distintos (amostra, normalizados) e o formato da coluna."""
    valores = serie.dropna().astype(str).map(_normalizar_valor)
    valores = valores[valores != ""]
    if valores.empty:
        return {"valores": set(), "digitos": 0.0, "data": 0.0, "decimal": 0.0, "tamanho": 0.0}
    amostra = pd.Series(pd.unique(valores)[:AMOSTRA_VALORES], dtype="string")
    return {
        "valores": set(amostra),
        "digitos": float(amostra.str.fullmatch(r"\d+").mean()),
        "data": float(amostra.str.match(r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}").mean()),
        "decimal": float(amostra.str.fullmatch(r"-?\d+[.,]\d+").mean()),
        "tamanho": float(amostra.str.len().mean()),
    }


def nota_par(antes: str, depois: str, pa_: dict, pb: dict) -> dict:
    """Notas de 0 a 1 de que `antes` e `depois` são o mesmo dado com outro nome."""
    uniao = pa_["valores"] | pb["valores"]
    valores = len(pa_["valores"] & pb["valores"]) / len(uniao) if uniao else 0.0
    nome = SequenceMatcher(None, radical(antes), radical(depois)).ratio()
    diffs = [abs(pa_[k] - pb[k]) for k in ("digitos", "data", "decimal")]
    maior = max(pa_["tamanho"], pb["tamanho"]) or 1.0
    formato = 1 - (sum(diffs) + abs(pa_["tamanho"] - pb["tamanho"]) / maior) / 4
    total = PESOS["valores"] * valores + PESOS["nome"] * nome + PESOS["formato"] * formato
    return {
        "nota": round(total, 3),
        "nota_valores": round(valores, 3),
        "nota_nome": round(nome, 3),
        "nota_formato": round(formato, 3),
    }


def evolucao_colunas(log_cleansed: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por família x coluna: período, arquivos e layouts em que aparece."""
    t = log_cleansed[log_cleansed.status == TRATADO].copy()
    t["layout"] = t.colunas.map(lambda c: hashlib.md5("|".join(sorted(c)).encode()).hexdigest()[:8])
    linhas = t.explode("colunas").rename(columns={"colunas": "coluna"})
    return (
        linhas.groupby(["familia", "coluna"])
        .agg(
            primeira_competencia=("competencia", "min"),
            ultima_competencia=("competencia", "max"),
            n_arquivos=("saida", "nunique"),
            layouts=("layout", lambda s: sorted(set(s))),
        )
        .reset_index()
    )


def trocas_de_layout(log_cleansed: pd.DataFrame) -> List[dict]:
    """Em cada família, na ordem da competência, cada par de arquivos seguidos com colunas
    diferentes: o que saiu e o que entrou."""
    t = log_cleansed[log_cleansed.status == TRATADO].sort_values(
        ["familia", "competencia", "saida"]
    )
    trocas = []
    for familia, g in t.groupby("familia"):
        anterior = None
        for r in g.itertuples():
            if anterior is not None:
                sai = set(anterior.colunas) - set(r.colunas)
                entra = set(r.colunas) - set(anterior.colunas)
                if sai and entra:
                    trocas.append(
                        {
                            "familia": familia,
                            "competencia_troca": r.competencia,
                            "antes": anterior,
                            "depois": r,
                            "sai": sorted(sai),
                            "entra": sorted(entra),
                        }
                    )
            anterior = r
    return trocas


def candidatos_renomeacao(trocas: List[dict], ler_coluna) -> pd.DataFrame:
    """Pares (coluna que saiu, coluna que entrou) com as notas. `ler_coluna(registro,
    coluna)` devolve a série da coluna no arquivo do registro."""
    linhas, perfis = [], {}

    def _perfil(reg, coluna):
        chave = (reg.saida, coluna)
        if chave not in perfis:
            perfis[chave] = perfil(ler_coluna(reg, coluna))
        return perfis[chave]

    for troca in trocas:
        for a in troca["sai"]:
            for b in troca["entra"]:
                notas = nota_par(a, b, _perfil(troca["antes"], a), _perfil(troca["depois"], b))
                linhas.append(
                    {
                        "familia": troca["familia"],
                        "coluna_antes": a,
                        "coluna_depois": b,
                        "competencia_troca": troca["competencia_troca"],
                        "saida_antes": troca["antes"].saida,
                        "saida_depois": troca["depois"].saida,
                        **notas,
                    }
                )
    df = pd.DataFrame(linhas)
    if df.empty:
        return df
    df = df.sort_values("nota", ascending=False).drop_duplicates(
        ["familia", "coluna_antes", "coluna_depois"]
    )
    # provável: o melhor par dos dois lados e acima da nota mínima
    melhor_a = df.groupby(["familia", "coluna_antes"]).nota.transform("max") == df.nota
    melhor_b = df.groupby(["familia", "coluna_depois"]).nota.transform("max") == df.nota
    df["provavel"] = melhor_a & melhor_b & (df.nota >= NOTA_PROVAVEL)
    return df.reset_index(drop=True)


def _grupos(
    evolucao: pd.DataFrame, candidatos: pd.DataFrame
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """(família, coluna) -> (raiz do grupo de renomeação, família): união dos pares prováveis."""
    pai = {(r.familia, r.coluna): (r.familia, r.coluna) for r in evolucao.itertuples()}

    def raiz(x):
        while pai[x] != x:
            pai[x] = pai[pai[x]]
            x = pai[x]
        return x

    if not candidatos.empty:
        for r in candidatos[candidatos.provavel].itertuples():
            a, b = raiz((r.familia, r.coluna_antes)), raiz((r.familia, r.coluna_depois))
            if a != b:
                pai[a] = b
    return {k: raiz(k) for k in pai}


def conceitos_colunas(evolucao: pd.DataFrame, candidatos: pd.DataFrame) -> pd.DataFrame:
    """Proposta de nome canônico por família x coluna: o conceito do glossário quando o
    grupo de renomeação tem um; senão, o nome mais recente do grupo."""
    grupo = _grupos(evolucao, candidatos)
    ev = evolucao.copy()
    ev["grupo"] = [grupo[(r.familia, r.coluna)] for r in ev.itertuples()]
    ev["conceito_nome"] = ev.coluna.map(conceito)
    saida = []
    for _, g in ev.groupby("grupo"):
        conceitos_g = sorted(set(g.conceito_nome.dropna()))
        recente = g.sort_values(["ultima_competencia", "primeira_competencia"]).coluna.iloc[-1]
        if len(conceitos_g) == 1:
            canonico, origem = conceitos_g[0], "glossario"
        else:
            canonico, origem = recente, "renomeacao" if len(g) > 1 else "proprio"
        for r in g.itertuples():
            saida.append(
                {
                    "familia": r.familia,
                    "coluna": r.coluna,
                    "nome_canonico": canonico,
                    "origem": origem if len(conceitos_g) <= 1 else "conflito_glossario",
                    "nomes_no_grupo": sorted(set(g.coluna)),
                    "primeira_competencia": r.primeira_competencia,
                    "ultima_competencia": r.ultima_competencia,
                    "n_arquivos": r.n_arquivos,
                }
            )
    return pd.DataFrame(saida).sort_values(["nome_canonico", "familia", "primeira_competencia"])


def _id_mermaid(texto: str) -> str:
    return "n" + hashlib.md5(texto.encode()).hexdigest()[:10]


def grafo_mermaid(conceitos: pd.DataFrame) -> str:
    """Um grafo por nome canônico com mais de um nome de origem: cada nó é um nome numa
    família, com o período; as setas vão do nome mais antigo para o canônico."""
    partes = [
        "# Grafo de nomes de coluna dos dados históricos",
        "",
        "Gerado pela task `mapear_colunas` da `dados_historicos_dag`. Cada grafo é um nome "
        "canônico proposto; os nós são os nomes de origem, por família, com o período em que "
        "aparecem. Proposta para revisão: só vale depois de confirmada no de-para.",
        "",
    ]
    for canonico, g in conceitos.groupby("nome_canonico"):
        if g.coluna.nunique() < 2 and g.familia.nunique() < 2:
            continue
        partes += [f"## `{canonico}`", "", "```mermaid", "flowchart LR"]
        alvo = _id_mermaid(f"canonico:{canonico}")
        partes.append(f'    {alvo}(["{canonico}"])')
        for r in g.sort_values("primeira_competencia").itertuples():
            no = _id_mermaid(f"{r.familia}:{r.coluna}")
            rotulo = (
                f"{r.coluna}<br/>{r.familia}<br/>{r.primeira_competencia} a {r.ultima_competencia}"
            )
            partes.append(f'    {no}["{rotulo}"] --> {alvo}')
        partes += ["```", ""]
    return "\n".join(partes)
