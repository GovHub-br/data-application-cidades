"""Gera dicionário de colunas, catálogo de tabelas e campos obrigatórios.

Este script produz três artefatos a partir das bases históricas MCMV
(dump + SFTP) já tratadas:

1. ``data/dicionario_colunas.csv`` — uma linha por (tabela, coluna), com
   tipo, descrição, domínio, fonte, obrigatoriedade e completude.
2. ``data/catalogo_tabelas.csv`` — uma linha por tabela do inventário,
   com nome de negócio, frente, período, fonte e chaves.
3. ``data/campos_obrigatorios.csv`` — contrato de campos obrigatórios
   (regra de negócio + heurística de não-nulos).

Observações de implementação:

- A completude por coluna (``pct_completude``) é calculada sobre as
  amostras de 200 linhas em ``data/dados_historicos_formatados/table_samples/``
  e, portanto, é uma estimativa baseada em amostra — não reflete o dump
  completo.
- ``report_date`` é tratado como coluna de dados no dicionário (é um
  campo de negócio relevante e entra na regra de negócio de obrigatórios);
  as demais colunas de metadados do pipeline (``source_table``,
  ``institution``, ``profile``, ``content_hash``, ``sub_table_index``)
  são excluídas do dicionário.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from classificacao.tratamento import normalizar_nome_coluna

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAMPLE_DIR = DATA_DIR / "dados_historicos_formatados" / "table_samples"

OUT_DICIONARIO = DATA_DIR / "dicionario_colunas.csv"
OUT_CATALOGO = DATA_DIR / "catalogo_tabelas.csv"
OUT_CAMPOS_OBRIGATORIOS = DATA_DIR / "campos_obrigatorios.csv"

# Colunas de metadados embutidas no tratamento. `report_date` fica de fora
# (é um campo de negócio e participa da regra de negócio de obrigatórios).
METADATA_COLS_EXCLUIDAS = frozenset(
    {"source_table", "institution", "profile", "content_hash", "sub_table_index"}
)

DATA_TYPE_MAP = {
    "text": "string",
    "bigint": "integer",
    "double precision": "float",
    "timestamp without time zone": "datetime",
    "boolean": "boolean",
}

# Regras de descrição por prefixo/token do nome normalizado, em ordem de
# prioridade (as mais específicas primeiro). `prefix=True` casa apenas
# `nome.startswith(tok)` ou `_{tok}` embutido (evita falsos positivos como
# 'plano_piloto' casando com 'no_' ou 'unidade_da_federacao' com 'unidade').
_DESCRICAO_RULES: list[tuple[tuple[str, ...], str, bool]] = [
    (("data_de_movimento",), "data de movimento", False),
    (("report_date",), "data de referência do relatório", False),
    (("dt_", "dat_", "data_"), "data", True),
    (("_date",), "data", False),
    (("unidade_da_federacao", "sg_uf"), "unidade federativa", False),
    (("cod_",), "código", True),
    (("codigo", "codmunicibge", "codapf"), "código", False),
    (("vlr_", "vr_", "valor_", "total_"), "valor", True),
    (("valor",), "valor", False),
    (("qtd_", "qt_"), "quantidade de unidades", True),
    (("unidades", "unidade"), "quantidade de unidades", False),
    (("percentual", "perc", "faixa_perc", "prc_execucao"), "percentual", False),
    (("nm_", "nome_", "no_"), "nome", True),
    (("razao_social", "denominacao"), "nome", False),
    (("ds_", "desc_", "dsc_"), "descrição", True),
    (("municipio", "cidade"), "município", False),
    (("ibge",), "código IBGE", False),
    (("cnpj",), "CNPJ", False),
    (("cpf",), "CPF", False),
    (("apf",), "identificador de operação (APF)", False),
    (
        ("situacao", "status", "andamento", "concluid", "paralisad", "retomad", "fase"),
        "situação/status da obra",
        False,
    ),
    (("contrato", "contratac"), "contrato", False),
    (("empreendimento", "empreend"), "empreendimento", False),
    (("obra",), "obra", False),
    (("endereco", "logradouro", "bairro", "cep"), "endereço", False),
    (("telefone", "fone", "email"), "contato", False),
    (
        ("origem", "produto", "programa", "faixa", "subprograma"),
        "programa/produto",
        False,
    ),
    (("agente", "banco", "agencia"), "agente financeiro", False),
]

# Regras de domínio, em ordem de prioridade (localização > status > identificador).
_DOMINIO_LOCALIZACAO = (
    "municipio",
    "codigo_ibge",
    "cod_municipio",
    "codmunicibge",
    "unidade_da_federacao",
    "sg_uf",
)
_DOMINIO_STATUS = (
    "status",
    "situacao",
    "fase",
    "andamento",
    "concluid",
    "paralisad",
    "retomad",
)
_DOMINIO_IDENTIFICADOR = (
    "apf",
    "contrato",
    "empreendimento",
    "cod_",
    "codigo",
    "cnpj",
    "cpf",
    "nr_",
    "nu_",
    "idregistro",
)

# Tokens de negócio que tornam um campo obrigatório por regra de negócio.
# Inclui as variantes `dt_` do padrão tratado (dt_contratacao/dt_atualizacao),
# além dos `dat_*` citados na especificação.
_TOKEN_NEGOCIO = (
    "apf",
    "nu_apf",
    "cod_ibge",
    "codigo_ibge",
    "cod_municipio",
    "cod_contrato",
    "dat_contratacao",
    "dat_atualizacao",
    "dt_contratacao",
    "dt_atualizacao",
    "report_date",
)

# Tokens de chave primária/identificador para o catálogo.
_TOKEN_CHAVES = (
    "apf",
    "cod_",
    "codigo",
    "codmunicibge",
    "codapf",
    "empreendimento",
    "nr_",
    "nu_",
    "idregistro",
)

LIMIAR_NAO_NULOS = 95.0

# Arquivos auxiliares copiados para table_samples (não são tabelas tratadas).
_SPECIAL_STEMS = frozenset({"_classificacao", "_dedup_map", "_qualidade"})


def _eh_arquivo_auxiliar(csv_path: Path) -> bool:
    """True para _classificacao/_dedup_map/_qualidade (não são tabelas)."""
    stem = csv_path.stem
    return any(stem.startswith(s) for s in _SPECIAL_STEMS)


# Meses em português usados na limpeza do nome de negócio.
_MESES_PT = (
    "janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|"
    "setembro|outubro|novembro|dezembro"
)


def resolver_arquivo(pattern: str, label: str) -> Path:
    """Resolve o arquivo mais recente (por mtime) que casa com o glob."""
    matches = sorted(DATA_DIR.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para '{label}' em {DATA_DIR}")
    return matches[-1]


def carregar_tipos_columns(csv_path: Path) -> dict[tuple[str, str], str]:
    """Lê ``columns_*.csv`` e devolve {(table_name, nome_norm): data_type}."""
    df = pd.read_csv(csv_path, encoding="utf-8")
    tipos: dict[tuple[str, str], str] = {}
    for record in df.to_dict("records"):
        table = str(record["table_name"]).strip()
        col = normalizar_nome_coluna(str(record["column_name"]))
        tipos[(table, col)] = str(record["data_type"]).strip()
    return tipos


def _is_nulo(valor: object) -> bool:
    """Trata None, string vazia e sentinelas 'NaN'/'nan' como nulo."""
    if valor is None:
        return True
    if isinstance(valor, float) and pd.isna(valor):
        return True
    s = str(valor).strip()
    return s == "" or s.lower() in ("nan", "na", "nat", "none", "null")


def ler_amostra(csv_path: Path) -> pd.DataFrame:
    """Lê uma tabela tratada (TAB, sem índice fixo).

    A primeira coluna é descartada apenas quando é claramente um índice
    de linha (nome ``unnamed_*`` e valores majoritariamente nulos).
    """
    df = pd.read_csv(csv_path, sep="\t", encoding="utf-8", keep_default_na=False)
    primeira = str(df.columns[0]).strip().lower()
    if primeira.startswith("unnamed") and len(df) > 0:
        nulos = sum(1 for v in df.iloc[:, 0] if _is_nulo(v))
        if nulos / len(df) > 0.9:
            df = df.drop(columns=[df.columns[0]])
    return df


def calcular_completude(df: pd.DataFrame, col: str) -> float:
    """Percentual de não-nulos de uma coluna (sobre a amostra de 200 linhas)."""
    total = len(df)
    if total == 0:
        return 0.0
    nao_nulos = sum(1 for v in df[col] if not _is_nulo(v))
    return round(nao_nulos / total * 100, 2)


def gerar_descricao(nome_norm: str) -> str:
    """Descrição automática por prefixo/token do nome normalizado."""
    if nome_norm == "uf":
        return "unidade federativa"
    for tokens, descricao, eh_prefixo in _DESCRICAO_RULES:
        if eh_prefixo:
            if any(nome_norm.startswith(t) or f"_{t}" in nome_norm for t in tokens):
                return descricao
        elif any(t in nome_norm for t in tokens):
            return descricao
    return ""


def inferir_dominio(nome_norm: str) -> str:
    """Categorias de domínio para colunas conhecidas; senão vazio."""
    if any(tok in nome_norm for tok in _DOMINIO_LOCALIZACAO):
        return "localização"
    if any(tok in nome_norm for tok in _DOMINIO_STATUS):
        return "status"
    if any(tok in nome_norm for tok in _DOMINIO_IDENTIFICADOR):
        return "identificador"
    return ""


def inferir_fonte(table_name: str) -> str:
    """'dump' para prefixos bb_/caixa_, senão 'sftp'."""
    return "dump" if table_name.startswith(("bb_", "caixa_")) else "sftp"


def inferir_tipo_por_nome(nome_norm: str) -> str:
    """Tipo canônico inferido do nome quando o schema do dump não cobre."""
    if nome_norm in ("report_date", "data_de_movimento") or any(
        tok in nome_norm for tok in ("dt_", "dat_", "data_", "_date")
    ):
        return "datetime"
    if nome_norm == "uf" or "unidade_da_federacao" in nome_norm:
        return "string"
    if any(tok in nome_norm for tok in ("vlr_", "vr_", "valor", "total_", "percentual")):
        return "float"
    if any(tok in nome_norm for tok in ("qtd_", "qt_", "unidades", "unidade")):
        return "integer"
    if nome_norm.startswith(("cod_", "cod", "nr_", "nu_", "no_")):
        return "string"
    if any(tok in nome_norm for tok in ("is_", "flag", "ind_")):
        return "boolean"
    return "string"


def mapear_tipo(
    tipos_columns: dict[tuple[str, str], str], table: str, coluna: str
) -> str:
    """Tipo do dump (mapeado) com fallback para inferência por nome."""
    data_type = tipos_columns.get((table, normalizar_nome_coluna(coluna)))
    if data_type is not None:
        return DATA_TYPE_MAP.get(data_type, "string")
    return inferir_tipo_por_nome(normalizar_nome_coluna(coluna))


def gerar_nome_negocio(table_name: str) -> str:
    """Nome de negócio: remove prefixos institucionais e tokens de data."""
    nome = table_name.strip()
    nome = re.sub(r"^(bb|caixa)_", "", nome)
    nome = re.sub(r"_?far_(bb|caixa)_?", "_", nome, flags=re.IGNORECASE)
    nome = re.sub(r"_?snh_?", "_", nome)
    nome = re.sub(r"ministeriocidades|ministerio", "", nome, flags=re.IGNORECASE)
    # Tokens de data (YYYYMMDD, YYYYMM, YYYY_MM_DD, DD_MM_YYYY, dia isolado, meses pt)
    nome = re.sub(r"20\d{2}[-_]?\d{2}[-_]?\d{2}", "", nome)
    nome = re.sub(r"20\d{2}[-_]?\d{2}", "", nome)
    nome = re.sub(r"\d{8}", "", nome)
    nome = re.sub(r"\d{6}", "", nome)
    nome = re.sub(r"_(0?[1-9]|[12]\d|3[01])_", "_", nome)
    nome = re.sub(_MESES_PT, "", nome, flags=re.IGNORECASE)
    # Remove anos e fragmentos de dia/mês que sobraram isolados
    nome = re.sub(r"\b20\d{2}\b", "", nome)
    nome = re.sub(r"\b\d{1,2}\b", "", nome)
    # Limpeza final
    nome = re.sub(r"[^a-zà-ú0-9 ]", " ", nome, flags=re.IGNORECASE)
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome if nome else table_name


def colunas_chaves(colunas: list[str]) -> list[str]:
    """Colunas identificadoras (APF, cod_*, empreendimento, etc.)."""
    chaves: list[str] = []
    for col in colunas:
        col_norm = normalizar_nome_coluna(col)
        if col_norm in METADATA_COLS_EXCLUIDAS:
            continue
        if any(tok in col_norm for tok in _TOKEN_CHAVES):
            chaves.append(col_norm)
    return chaves


def gerar_dicionario(
    tipos_columns: dict[tuple[str, str], str],
) -> tuple[pd.DataFrame, dict[str, dict[tuple[str, str], float]]]:
    """Monta ``dicionario_colunas.csv`` a partir das tabelas tratadas."""
    rows: list[list[object]] = []
    completude: dict[str, dict[tuple[str, str], float]] = {}

    for csv_path in sorted(SAMPLE_DIR.glob("*.csv")):
        if _eh_arquivo_auxiliar(csv_path):
            continue  # _classificacao, _dedup_map, _qualidade
        try:
            df = ler_amostra(csv_path)
        except Exception as exc:  # noqa: BLE001 - amostra corrompida não pode derrubar o lote
            print(f"[aviso] falha ao ler {csv_path.name}: {exc}")
            continue
        if "source_table" not in df.columns:
            continue

        table = str(df["source_table"].iloc[0]).strip()
        colunas = [c for c in df.columns if str(c).strip() not in METADATA_COLS_EXCLUIDAS]
        completude[table] = {}
        for col in colunas:
            col_str = str(col).strip()
            pct = calcular_completude(df, col)
            completude[table][(table, col_str)] = pct
            rows.append(
                [
                    table,
                    col_str,
                    mapear_tipo(tipos_columns, table, col_str),
                    gerar_descricao(normalizar_nome_coluna(col_str)),
                    inferir_dominio(normalizar_nome_coluna(col_str)),
                    inferir_fonte(table),
                    False,  # obrigatorio preenchido após gerar campos_obrigatorios
                    pct,
                ]
            )

    return (
        pd.DataFrame(
            rows,
            columns=[
                "tabela",
                "coluna",
                "tipo",
                "descricao",
                "dominio",
                "fonte",
                "obrigatorio",
                "pct_completude",
            ],
        ),
        completude,
    )


def gerar_campos_obrigatorios(
    completude: dict[str, dict[tuple[str, str], float]],
) -> pd.DataFrame:
    """Contrato de campos obrigatórios: regra de negócio + não-nulos >= 95%."""
    rows: list[list[str]] = []
    for table, cols in completude.items():
        for (tabela, coluna), pct in cols.items():
            nome_norm = normalizar_nome_coluna(coluna)
            if any(tok in nome_norm for tok in _TOKEN_NEGOCIO):
                rows.append([tabela, coluna, "negocio"])
            if pct >= LIMIAR_NAO_NULOS:
                rows.append([tabela, coluna, "nao_nulos"])
    return pd.DataFrame(rows, columns=["tabela", "coluna", "regra"])


def gerar_catalogo(
    inventario: pd.DataFrame,
    tipos_columns: dict[tuple[str, str], str],
    colunas_por_tabela: dict[str, list[str]],
) -> pd.DataFrame:
    """Catálogo: uma linha por tabela do inventário."""
    rows: list[list[object]] = []
    for record in inventario.to_dict("records"):
        table = str(record["table_name"]).strip()
        colunas = colunas_por_tabela.get(table) or [
            c for t, c in tipos_columns if t == table
        ]
        chaves = ";".join(colunas_chaves(colunas))
        rows.append(
            [
                table,
                gerar_nome_negocio(table),
                _nao_nan(record["frentes_cobertas"]),
                _nao_nan(record["instituicao"]),
                _nao_nan(record["periodo_dados_inicio"]),
                _nao_nan(record["periodo_dados_fim"]),
                inferir_fonte(table),
                chaves,
                "",
                "",
            ]
        )
    return pd.DataFrame(
        rows,
        columns=[
            "tabela",
            "nome_negocio",
            "frente",
            "instituicao",
            "periodo_inicio",
            "periodo_fim",
            "fonte",
            "chaves",
            "responsavel",
            "modelo_carga",
        ],
    )


def _nao_nan(valor: object) -> str:
    """Converte valores nulos/pandas.NA para string vazia."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    s = str(valor).strip()
    return "" if s.lower() in ("nan", "na", "none", "null") else s


def main() -> None:
    columns_path = resolver_arquivo("columns_*.csv", "columns_*.csv")
    tipos_columns = carregar_tipos_columns(columns_path)

    print("Gerando dicionário de colunas...")
    dicionario, completude = gerar_dicionario(tipos_columns)
    print(f"  {len(dicionario)} linhas (tabela, coluna)")

    print("Gerando campos obrigatórios...")
    campos = gerar_campos_obrigatorios(completude)
    print(f"  {len(campos)} linhas")

    print("Marcando obrigatoriedade no dicionário...")
    obrigatorios: set[tuple[str, str]] = set()
    for record in campos.to_dict("records"):
        obrigatorios.add((str(record["tabela"]).strip(), str(record["coluna"]).strip()))
    dicionario["obrigatorio"] = [
        (t.strip(), c.strip()) in obrigatorios
        for t, c in zip(dicionario["tabela"], dicionario["coluna"])
    ]

    print("Gerando catálogo de tabelas...")
    inventario = pd.read_csv(
        DATA_DIR / "inventario_dados.csv", sep="\t", encoding="utf-8"
    )
    colunas_por_tabela: dict[str, list[str]] = {}
    for csv_path in SAMPLE_DIR.glob("*.csv"):
        if _eh_arquivo_auxiliar(csv_path):
            continue
        try:
            df = ler_amostra(csv_path)
        except Exception:  # noqa: BLE001
            continue
        if "source_table" not in df.columns:
            continue
        table = str(df["source_table"].iloc[0]).strip()
        colunas_por_tabela[table] = [
            str(c).strip()
            for c in df.columns
            if str(c).strip() not in METADATA_COLS_EXCLUIDAS
        ]
    catalogo = gerar_catalogo(inventario, tipos_columns, colunas_por_tabela)
    print(f"  {len(catalogo)} linhas")

    dicionario.to_csv(OUT_DICIONARIO, sep=",", index=False, encoding="utf-8")
    campos.to_csv(OUT_CAMPOS_OBRIGATORIOS, sep=",", index=False, encoding="utf-8")
    catalogo.to_csv(OUT_CATALOGO, sep=",", index=False, encoding="utf-8")

    print(
        "Escritos: "
        f"{OUT_DICIONARIO.name}, {OUT_CATALOGO.name}, {OUT_CAMPOS_OBRIGATORIOS.name}"
    )


if __name__ == "__main__":
    main()
