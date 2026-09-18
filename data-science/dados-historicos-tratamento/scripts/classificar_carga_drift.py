"""Classifica modelo de carga (full_refresh × incremental) e detecta drift de schema.

Lê o schema ``dados_historicos`` do PostgreSQL (colunas exatas via
``information_schema.columns`` + row count por tabela) e produz quatro
artefatos em ``data/``:

1. ``data/classificacao_carga.csv`` — cada tabela do dump com ``familia``
   (stem canônico + dimensão de variante), ``variante``, ``modelo_carga`` e
   ``evidencia`` textual da decisão.
2. ``data/drift_schema.csv`` — para famílias com >= 2 versões, difere o
   conjunto de colunas de cada versão contra a versão mais antiga (baseline)
   e registra colunas ``nova``/``removida``.
3. ``data/relatorio_carga_familias.md`` — relatório de revisão humana: por
   família, lista de versões + row count + classificação + evidência.
4. ``data/classificacao_carga_transferegov.csv`` — stub das bases novas do
   TransfereGov (schemas ``transferegov_emendas``/``transfere_gov`` não
   existem neste banco; ``modelo_carga="indeterminado"``).

Agrupamento de famílias
-----------------------
A chave de família é ``stem canônico + variante``, onde a variante captura de
forma determinística as dimensões que ``canonicalizar_stem`` apaga:

- instituição (``af_bb`` × ``af_caixa``);
- prefixo (``historico_recente`` × ``o_recente``; truncamentos ``ecente_`` /
  ``storico_recente_`` normalizados para ``historico_recente``);
- sufixo (``_entregas`` × ``_da_entrega_da_unidade``).

Sem a dimensão de variante, as famílias ``snh_pmcmv_dados_prioritarios*``
colapsavam numa só família (~84 tabelas) e geravam ~95% do drift como ruído.

Classificação de carga (trajetória de row count)
------------------------------------------------
O sinal de span de período (``periodo_dados_inicio != periodo_dados_fim``)
foi removido: o span reflete o intervalo de datas DENTRO do snapshot e não o
modo de append. A classificação usa a trajetória de row count por família,
ordenada por período:

- ``full_refresh`` (snapshot): contagens flutuam ou permanecem similares
  entre versões (cada versão é um estado completo) — inclusive quando crescem
  lentamente (acumulado de estado).
- ``incremental`` (append): contagens crescem monotonicamente E cada versão
  contém APENAS as linhas novas. Sem acesso ao conteúdo das tabelas, a
  condição "apenas linhas novas" é aproximada por: série com >= 3 versões,
  crescimento estritamente monotônico e sem salto > 25% entre versões
  consecutivas (saltos desse porte indicam recomputo de snapshot, não append).

Contagens: ``SELECT count(*)`` exato por padrão; ``--estimativa`` usa
``pg_class.reltuples`` (rápido, ~1s). A fonte usada é registrada na coluna
``evidencia`` e no relatório. Quando o banco está indisponível, o script cai
para os artefatos locais (``data/columns_*.csv`` + ``inventario_dados.csv``
— neste caso ``n_linhas`` é derivado de amostras de 200 linhas e não permite
trajetória confiável).

Uso::

    uv run python scripts/classificar_carga_drift.py
    uv run python scripts/classificar_carga_drift.py --estimativa
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Adicionar src/ ao path para importar o pacote sftp. O insert é incondicional
# e na posição 0: sem ele, o namespace package scripts/sftp/ (sem __init__.py)
# sombrearia o pacote real em src/sftp/.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sftp.normalizacao import canonicalizar_stem  # noqa: E402 — precisa do insert acima

# ── Caminhos ──────────────────────────────────────────────────────────────
DATA_DIR = ROOT / "data"
COLUMNS_CSV = DATA_DIR / "columns_202605211425_perfil_cidades_dados_historicos.csv"
INVENTARIO_CSV = DATA_DIR / "inventario_dados.csv"

OUT_CARGA = DATA_DIR / "classificacao_carga.csv"
OUT_DRIFT = DATA_DIR / "drift_schema.csv"
OUT_RELATORIO = DATA_DIR / "relatorio_carga_familias.md"
OUT_TRANSFEREGOV = DATA_DIR / "classificacao_carga_transferegov.csv"

DAGS_TRANSFEREGOV = (
    ROOT.parents[1] / "airflow_lappis" / "dags" / "data_ingest" / "transferegov_emendas"
)
DAGS_TRANSFERE_GOV = (
    ROOT.parents[1] / "airflow_lappis" / "dags" / "data_ingest" / "transfere_gov"
)

# Tokens de período no nome da tabela (design: regex YYYYMM ou YYYY).
# Busca com fronteira de dígito (``(?<!\\d)...(?!\\d)``) para evitar casar
# dentro de datas de 8 dígitos (ex.: ``208201`` em ``..._12082015``).
_RE_8DIG = re.compile(r"(?<!\d)(\d{8})(?!\d)")
_RE_YYYYMM_IN_NOME = re.compile(r"(?<!\d)((?:19|20)\d{2}(0[1-9]|1[0-2]))(?!\d)")
_RE_YYYY_IN_NOME = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")

# Dimensão de variante (separada do stem canônico)
_RE_AF = re.compile(r"_af_(bb|caixa)")
_RE_SUF_ENTREGAS = re.compile(r"_entregas$")
_RE_SUF_ENTREGA_UNIDADE = re.compile(r"_da_entrega_da_unidade(?:_af_(?:bb|caixa))?$")
_PREFIXOS_VARIANTE: tuple[tuple[re.Pattern[str], str], ...] = (
    # historico_recente + truncamentos do limite de 63 chars do PostgreSQL
    (re.compile(r"^(?:historico_recente|ecente|storico_recente)_"), "historico_recente"),
    (re.compile(r"^o_recente_"), "o_recente"),
)

_VALORES_VAZIOS = {"", "nan", "none", "nat"}

# Limiar de salto entre versões consecutivas: acima disso o crescimento é
# tratado como recomputo de snapshot, não append. (1.25 = +25% por versão)
_LIMIAR_SALTO_RELATIVO = 1.25
# Cadência máxima entre versões consecutivas de um append (~2 meses). Um salto
# maior indica versões de snapshot em datas distintas, não extração de delta.
_LIMIAR_CADENCIA_DIAS = 62


# ── Helpers genéricos ─────────────────────────────────────────────────────


def _limpar(valor: str | None) -> str:
    """Remove espaços e trata valores 'nan'/'None' produzidos por pandas."""
    if valor is None:
        return ""
    valor = str(valor).strip()
    if valor.lower() in _VALORES_VAZIOS:
        return ""
    return valor


def _parse_iso(data_str: str) -> str | None:
    """Converte ``YYYY-MM-DD`` para ISO; retorna None se inválido."""
    try:
        return datetime.strptime(data_str, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _escrever_csv(path: Path, header: list[str], rows: list[list[Any]]) -> int:
    """Grava CSV separado por vírgula (utf-8). Retorna número de linhas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


# ── Agrupamento de famílias (stem canônico + variante) ───────────────────


def extrair_variante(nome: str) -> str:
    """Extrai a dimensão de variante de um nome de tabela.

    Captura de forma determinística (ordem fixa instituição → prefixo →
    sufixo) o que ``canonicalizar_stem`` apaga: instituição (``af_bb`` /
    ``af_caixa``), prefixo (``historico_recente`` / ``o_recente``) e sufixo
    (``_entregas`` / ``_da_entrega_da_unidade``). Componentes ausentes não
    aparecem na variante; sem nenhum, retorna string vazia.
    """
    nome_l = nome.strip().strip('"').lower()

    partes: list[str] = []

    m = _RE_AF.search(nome_l)
    if m:
        partes.append(f"af_{m.group(1)}")

    for pattern, label in _PREFIXOS_VARIANTE:
        if pattern.match(nome_l):
            partes.append(label)
            break

    if _RE_SUF_ENTREGAS.search(nome_l):
        partes.append("entregas")
    elif _RE_SUF_ENTREGA_UNIDADE.search(nome_l):
        partes.append("entrega_unidade")

    return "|".join(partes)


def chave_familia(nome: str) -> str:
    """Retorna a chave de família ``stem_canônico|variante`` (ou só o stem).

    O stem vem de ``canonicalizar_stem`` (reusa ``src/sftp/normalizacao.py``);
    a variante re-adiciona a dimensão que ele apaga para que famílias
    distintas (ex.: ``af_bb`` × ``af_caixa``, ``historico_recente`` ×
    ``o_recente``, ``_entregas`` × ``_dados_prioritarios``) não colapsem.
    """
    stem = canonicalizar_stem(nome)
    variante = extrair_variante(nome)
    return f"{stem}|{variante}" if variante else stem


def _detectar_token_periodo(nome: str) -> tuple[str, str] | None:
    """Retorna ``(tipo, valor)`` do token de período no nome, ou None.

    Tipos: ``"YYYYMM"`` (ex.: ``202402``) ou ``"YYYY"`` (ex.: ``2012``).
    Usa busca com fronteira de dígito para não casar dentro de datas de 8
    dígitos (ex.: ``208201`` dentro de ``..._12082015``).
    """
    m = _RE_YYYYMM_IN_NOME.search(nome)
    if m:
        return ("YYYYMM", m.group(1))
    m = _RE_YYYY_IN_NOME.search(nome)
    if m:
        return ("YYYY", m.group(1))
    return None


def _parse_data_parts(ano: int, mes: int, dia: int) -> str | None:
    """Valida uma data e retorna ISO; aceita só anos 2000–2030."""
    if not (2000 <= ano <= 2030):
        return None
    try:
        return datetime(ano, mes, dia).date().isoformat()
    except ValueError:
        return None


def _extrair_periodo_nome(nome: str) -> str | None:
    """Extrai um período ISO ordenável do nome (8 dígitos, YYYYMM, YYYY)."""
    for m in _RE_8DIG.finditer(nome):
        s = m.group(1)
        for ano, mes, dia in (
            (int(s[0:4]), int(s[4:6]), int(s[6:8])),
            (int(s[4:8]), int(s[2:4]), int(s[0:2])),
        ):
            iso = _parse_data_parts(ano, mes, dia)
            if iso is not None:
                return iso

    m_ym = _RE_YYYYMM_IN_NOME.search(nome)
    if m_ym:
        s = m_ym.group(1)
        return datetime(int(s[0:4]), int(s[4:6]), 1).date().isoformat()

    m_y = _RE_YYYY_IN_NOME.search(nome)
    if m_y:
        s = m_y.group(1)
        return datetime(int(s), 1, 1).date().isoformat()

    return None


def _extrair_periodo_versao(
    nome: str,
    inv_row: dict[str, Any] | None,
) -> str | None:
    """Extrai uma data ISO ordenável para a versão.

    Prioridade: ``report_date`` do inventário (modo fallback local); depois o
    período derivado do nome (8 dígitos, YYYYMM, YYYY).
    """
    report_date = _limpar((inv_row or {}).get("report_date"))
    if report_date:
        iso = _parse_iso(report_date)
        if iso is not None:
            return iso
    return _extrair_periodo_nome(nome)


# ── Classificação de carga (trajetória de row count) ─────────────────────


def _montar_evidencia(
    modelo: str,
    trajetoria: str,
    fonte_contagem: str,
    nome_periodo: tuple[str, str] | None,
    criterio: str,
) -> str:
    partes = [
        f"classificacao={criterio}",
        f"trajetoria={trajetoria}",
        f"fonte_contagem={fonte_contagem}",
    ]
    if nome_periodo is not None:
        partes.append(f"nome_periodo={nome_periodo[0]}:{nome_periodo[1]}")
    return "; ".join(partes)


def _periodo_para_data(periodo: str) -> date | None:
    """Converte período ISO (YYYY-MM-DD, YYYY-MM, YYYY) para ``date``."""
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(periodo, fmt).date()
        except ValueError:
            continue
    return None


def _cadencia_consistente(
    periodos: list[str], max_gap_dias: int = _LIMIAR_CADENCIA_DIAS
) -> bool:
    """Verifica se os períodos formam uma série regular de extrações.

    Um append tem versões em cadência de extração (dias/semanas/meses
    consecutivos); um salto grande entre versões indica snapshots em datas
    distintas (recomputo), não deltas de append. Período ausente torna a
    verificação impossível (retorna False).
    """
    datas: list[date] = []
    for p in periodos:
        d = _periodo_para_data(p)
        if d is None:
            return False
        datas.append(d)
    for d1, d2 in zip(datas, datas[1:]):
        if (d2 - d1).days > max_gap_dias:
            return False
    return True


def classificar_por_trajetoria(
    versoes: list[tuple[str, str, int]],
    fonte_contagem: str,
    nome_periodo: tuple[str, str] | None,
) -> tuple[str, str]:
    """Classifica ``full_refresh`` × ``incremental`` pela trajetória de contagens.

    ``versoes`` deve vir ordenada por ``(periodo, tabela)``; cada item é
    ``(periodo_iso, nome_tabela, n_linhas)``.

    - ``full_refresh``: contagens flutuam ou permanecem similares entre
      versões (cada versão é um estado completo) — inclusive crescimento
      lento de acumulado de estado.
    - ``incremental``: contagens crescem monotonicamente E cada versão
      contém apenas as linhas novas (aproximado por: >= 3 versões, estritamente
      monotônico, sem salto > ``_LIMIAR_SALTO_RELATIVO`` entre versões).
    """
    trajetoria = "->".join(str(n_linhas) for _periodo, _tabela, n_linhas in versoes)

    if len(versoes) < 2:
        return "full_refresh", _montar_evidencia(
            "full_refresh", trajetoria, fonte_contagem, nome_periodo, "versao_unica"
        )

    contagens = [n_linhas for _periodo, _tabela, n_linhas in versoes]
    periodos = [periodo for periodo, _tabela, _n_linhas in versoes]
    monotona = all(contagens[i] < contagens[i + 1] for i in range(len(contagens) - 1))
    if not monotona:
        return "full_refresh", _montar_evidencia(
            "full_refresh",
            trajetoria,
            fonte_contagem,
            nome_periodo,
            "nao_monotona(flutua_ou_constante)",
        )

    saltos = [contagens[i + 1] / contagens[i] for i in range(len(contagens) - 1)]
    if (
        len(contagens) >= 3
        and all(salto <= _LIMIAR_SALTO_RELATIVO for salto in saltos)
        and _cadencia_consistente(periodos)
    ):
        return "incremental", _montar_evidencia(
            "incremental",
            trajetoria,
            fonte_contagem,
            nome_periodo,
            "append_monotono_estavel",
        )

    return "full_refresh", _montar_evidencia(
        "full_refresh",
        trajetoria,
        fonte_contagem,
        nome_periodo,
        "crescimento_nao_consistente_com_append",
    )


# ── Drift de schema ───────────────────────────────────────────────────────


def _chave_ordenacao_versao(
    versao: tuple[str, str | None],
) -> tuple[int, str, str]:
    """Ordena versões por período; versões sem período vêm antes (base)."""
    nome, periodo = versao
    return (0 if periodo is not None else 1, periodo or "", nome)


def detectar_drift(
    cols_by_table: dict[str, list[str]],
    inv_by_table: dict[str, dict[str, Any]],
) -> tuple[list[list[str]], Counter, Counter]:
    """Computa drift de colunas por família de tabelas (stem + variante).

    Baseline = versão mais antiga da família. Para cada versão posterior,
    colunas ausentes no baseline são ``nova``; colunas do baseline ausentes na
    versão são ``removida``.

    Returns
    -------
    rows : list of [familia, versao, coluna, status]
    novas : Counter com colunas ``nova`` mais comuns
    removidas : Counter com colunas ``removida`` mais comuns
    """
    familias: dict[str, list[tuple[str, str | None]]] = {}
    for tabela in cols_by_table:
        familia = chave_familia(tabela)
        familias.setdefault(familia, []).append(
            (tabela, _extrair_periodo_versao(tabela, inv_by_table.get(tabela)))
        )

    rows: list[list[str]] = []
    novas: Counter = Counter()
    removidas: Counter = Counter()

    for familia, versoes in familias.items():
        if len(versoes) < 2:
            continue
        versoes = sorted(versoes, key=_chave_ordenacao_versao)
        baseline = set(cols_by_table[versoes[0][0]])
        for versao, _periodo in versoes[1:]:
            colunas = set(cols_by_table[versao])
            for coluna in sorted(colunas - baseline):
                rows.append([familia, versao, coluna, "nova"])
                novas[coluna] += 1
            for coluna in sorted(baseline - colunas):
                rows.append([familia, versao, coluna, "removida"])
                removidas[coluna] += 1

    return rows, novas, removidas


# ── Stub TransfereGov ─────────────────────────────────────────────────────


def _extrair_tabelas_dag(texto: str) -> set[str]:
    """Extrai nomes de tabela gravados em um DAG.

    Reconhece o 2º argumento posicional de ``insert_data(...)``,
    ``table_name="..."`` e ``alter_table(..., "...")``.
    """
    tabelas: set[str] = set()
    for m in re.finditer(r'insert_data\s*\(\s*[^,]+,\s*"([a-z_0-9]+)"', texto):
        tabelas.add(m.group(1))
    for m in re.finditer(r'table_name\s*=\s*"([a-z_0-9]+)"', texto):
        tabelas.add(m.group(1))
    for m in re.finditer(r'alter_table\s*\(\s*[^,]+,\s*"([a-z_0-9]+)"', texto):
        tabelas.add(m.group(1))
    return tabelas


def gerar_stub_transferegov() -> list[list[str]]:
    """Enumera tabelas TransfereGov a partir dos DAGs de ingestão.

    Retorna linhas ``[tabela, servico, modelo_carga]`` com
    ``modelo_carga="indeterminado"`` (confirmação requer acesso ao banco —
    os schemas ``transferegov_emendas``/``transfere_gov`` não existem neste
    banco).
    """
    registros: set[tuple[str, str]] = set()
    for diretorio in (DAGS_TRANSFEREGOV, DAGS_TRANSFERE_GOV):
        if not diretorio.exists():
            print(f"AVISO: diretório de DAGs não encontrado: {diretorio}")
            continue
        for dag_file in sorted(diretorio.glob("*.py")):
            texto = dag_file.read_text(encoding="utf-8")
            servico = diretorio.name  # transferegov_emendas | transfere_gov
            for tabela in _extrair_tabelas_dag(texto):
                registros.add((tabela, servico))

    return [[tabela, servico, "indeterminado"] for tabela, servico in sorted(registros)]


# ── Carregamento de entradas (DB + fallback local) ────────────────────────


def carregar_colunas_db(schema: str) -> dict[str, list[str]]:
    """Lê colunas reais do schema via ``information_schema.columns``.

    Todas as tabelas do schema aparecem no dicionário; tabelas sem colunas
    (ex.: criadas vazias) entram com lista vazia.
    """
    from sqlalchemy import text

    from classificacao.db.connection import get_engine

    engine = get_engine()
    cols_by_table: dict[str, list[str]] = {
        tabela: [] for tabela in _listar_tabelas_db(schema)
    }
    stmt = text(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = :schema "
        "ORDER BY table_name, ordinal_position"
    )
    with engine.connect() as conn:
        for table_name, column_name in conn.execute(stmt, {"schema": schema}):
            cols_by_table[table_name].append(column_name)
    return cols_by_table


def _listar_tabelas_db(schema: str) -> list[str]:
    from sqlalchemy import text

    from classificacao.db.connection import get_engine

    engine = get_engine()
    stmt = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt, {"schema": schema})]


def carregar_contagens_db(
    schema: str,
    usar_estimativa: bool,
) -> tuple[dict[str, int], str]:
    """Conta linhas de cada tabela do schema.

    Por padrão usa ``SELECT count(*)`` exato por tabela. Com
    ``usar_estimativa=True`` usa ``pg_class.reltuples`` (rápido; neste banco
    ``pg_stat_user_tables.n_live_tup`` é 0 porque nenhum ANALYZE foi rodado).

    Returns
    -------
    (contagens, fonte) — ``fonte`` é ``"exato_count_star"`` ou
    ``"estimativa_reltuples"``, registrada na evidência e no relatório.
    """
    from sqlalchemy import text

    from classificacao.db.connection import get_engine

    engine = get_engine()

    if usar_estimativa:
        stmt = text(
            "SELECT c.relname AS tabela, c.reltuples::bigint AS n_linhas "
            "FROM pg_class c "
            "JOIN pg_namespace ns ON ns.oid = c.relnamespace "
            "WHERE ns.nspname = :schema AND c.relkind = 'r' "
            "ORDER BY c.relname"
        )
        contagens: dict[str, int] = {}
        with engine.connect() as conn:
            for tabela, n_linhas in conn.execute(stmt, {"schema": schema}):
                contagens[tabela] = int(n_linhas or 0)
        return contagens, "estimativa_reltuples"

    contagens = {}
    for tabela in _listar_tabelas_db(schema):
        preparer = engine.dialect.identifier_preparer
        tabela_ql = preparer.quote(tabela)
        schema_ql = preparer.quote(schema)
        stmt = text(f"SELECT count(*) FROM {schema_ql}.{tabela_ql}")
        with engine.connect() as conn:
            contagens[tabela] = int(conn.scalar(stmt) or 0)
    return contagens, "exato_count_star"


def carregar_colunas(path: Path) -> dict[str, list[str]]:
    """Lê ``table_name,column_name,data_type`` e agrupa colunas por tabela."""
    cols_by_table: dict[str, list[str]] = {}
    with path.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            cols_by_table.setdefault(row["table_name"], []).append(row["column_name"])
    return cols_by_table


def carregar_inventario(path: Path) -> dict[str, dict[str, Any]]:
    """Lê inventário (TAB) e indexa por ``table_name``."""
    inv_by_table: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            inv_by_table[row["table_name"]] = dict(row)
    return inv_by_table


def buscar_inventario(
    tabela: str,
    inv_by_table: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Busca inventário por nome exato; fallback por prefixo.

    O dump PostgreSQL trunca nomes em 63 caracteres (ex.: a tabela do inventário
    ``bb_2012_05_maio_z_relatorio_caixa_3105`` aparece no dump como
    ``..._3105a``).
    """
    if tabela in inv_by_table:
        return inv_by_table[tabela]
    for nome in inv_by_table:
        if tabela.startswith(nome) or nome.startswith(tabela):
            return inv_by_table[nome]
    return None


def _carregar_entradas(
    usar_estimativa: bool,
) -> tuple[
    dict[str, list[str]],
    dict[str, int],
    str,
    dict[str, dict[str, Any]] | None,
    str,
]:
    """Carrega colunas e contagens — DB primeiro, fallback local em seguida.

    Returns
    -------
    ``(cols_by_table, contagens, fonte_contagem, inv_by_table, origem)``.
    ``origem`` é ``"db"`` ou ``"local"``; ``inv_by_table`` é None no modo DB.
    """
    from classificacao.db.connection import get_schema_source

    try:
        schema = get_schema_source()
        cols_by_table = carregar_colunas_db(schema)
        contagens, fonte = carregar_contagens_db(schema, usar_estimativa)
        print(f"Banco acessível (schema '{schema}'): {len(cols_by_table)} tabelas.")
        print(f"Fonte de contagem: {fonte}")
        return cols_by_table, contagens, fonte, None, "db"
    except Exception as exc:  # noqa: BLE001 — fallback deliberado
        print(
            f"AVISO: banco indisponível ({exc.__class__.__name__}: {exc}). "
            "Usando artefatos locais.",
            file=sys.stderr,
        )

    if not COLUMNS_CSV.exists():
        print(f"ERRO: nenhuma entrada disponível (DB e {COLUMNS_CSV}).", file=sys.stderr)
        sys.exit(1)

    cols_by_table = carregar_colunas(COLUMNS_CSV)
    inv_by_table = carregar_inventario(INVENTARIO_CSV)
    # n_linhas do inventário é derivado de amostras de 200 linhas, não é o
    # tamanho real — registrado para a trajetória ficar "plana" e a decisão
    # cair em full_refresh por falta de evidência.
    contagens = {
        tabela: int(float(_limpar(row.get("n_linhas")) or 0))
        for tabela, row in inv_by_table.items()
    }
    return cols_by_table, contagens, "inventario_csv", inv_by_table, "local"


# ── Relatório de revisão ──────────────────────────────────────────────────


def _formata_trajetoria(
    versoes: list[tuple[str, str, int]],
) -> str:
    return " → ".join(str(n_linhas) for _p, _t, n_linhas in versoes)


def gerar_relatorio_familias(
    carga_rows: list[list[str]],
    contagens: dict[str, int],
    fonte_contagem: str,
    origem: str,
) -> str:
    """Gera o relatório Markdown de revisão humana por família."""
    familias: dict[str, list[tuple[str, str, int]]] = {}
    for tabela, familia, _variante, _modelo, _evidencia in carga_rows:
        familias.setdefault(familia, []).append(
            (
                str(_extrair_periodo_versao(tabela, None)) or "",
                tabela,
                contagens.get(tabela, 0),
            )
        )
    for familia in familias:
        familias[familia].sort(key=lambda v: (v[0], v[1]))

    multi = {k: v for k, v in familias.items() if len(v) >= 2}
    unica = {k: v for k, v in familias.items() if len(v) == 1}

    dist = Counter(row[3] for row in carga_rows)
    total = len(carga_rows)
    n_familias = len(familias)

    linhas: list[str] = []
    linhas.append("# Relatório de revisão — classificação de carga por família")
    linhas.append("")
    linhas.append("Artefato gerado por `scripts/classificar_carga_drift.py` para revisão")
    linhas.append("humana das fronteiras de família e da classificação full×incremental.")
    linhas.append("")
    linhas.append("## Fonte de dados")
    linhas.append("")
    if origem == "db":
        linhas.append(
            "- Colunas: banco de dados (`information_schema.columns`, "
            "schema `dados_historicos`)"
        )
    else:
        linhas.append(
            "- Colunas: artefato local `data/columns_*.csv` "
            "(fallback — banco indisponível)"
        )
    linhas.append(f"- Contagens: `{fonte_contagem}`")
    if fonte_contagem == "inventario_csv":
        linhas.append(
            "  - **Atenção:** `n_linhas` do inventário vem de amostras de 200 linhas; "
            "a trajetória não é confiável neste modo."
        )
    linhas.append("")
    linhas.append("## Resumo")
    linhas.append("")
    linhas.append(f"- Tabelas: **{total}**")
    linhas.append(
        f"- Famílias: **{n_familias}** (multi-versão: **{len(multi)}**, "
        f"versão única: **{len(unica)}**)",
    )
    linhas.append(f"- Distribuição `modelo_carga`: **{dict(dist)}**")
    linhas.append("")

    linhas.append("## Famílias multi-versão (revisão de trajetória)")
    linhas.append("")
    linhas.append(
        "Legenda de classificação: `nao_monotona(...)` = snapshot (flutua/constante); "
        "`crescimento_nao_consistente_com_append` = monotônico mas com salto "
        "(recomputo de snapshot); `append_monotono_estavel` = candidato a incremental "
        "(≥ 3 versões, crescimento estável)."
    )
    linhas.append("")

    for familia in sorted(multi):
        versoes = multi[familia]
        modelo = next(row[3] for row in carga_rows if row[1] == familia)
        evidencia = next(row[4] for row in carga_rows if row[1] == familia)
        linhas.append(f"### {familia} — `{modelo}`")
        linhas.append("")
        linhas.append(f"- Versões: **{len(versoes)}**")
        linhas.append(f"- Trajetória de contagens: `{_formata_trajetoria(versoes)}`")
        linhas.append(f"- Evidência: {evidencia}")
        linhas.append("")
        linhas.append("| período | tabela | n_linhas |")
        linhas.append("|---|---|---|")
        for periodo, tabela, n_linhas in versoes:
            linhas.append(f"| {periodo or '—'} | `{tabela}` | {n_linhas} |")
        linhas.append("")

    linhas.append("## Famílias com versão única")
    linhas.append("")
    linhas.append("| família | tabela | n_linhas | modelo_carga |")
    linhas.append("|---|---|---|---|")
    for familia in sorted(unica):
        periodo, tabela, n_linhas = unica[familia][0]
        modelo = next(row[3] for row in carga_rows if row[1] == familia)
        linhas.append(f"| `{familia}` | `{tabela}` | {n_linhas} | {modelo} |")
    linhas.append("")

    return "\n".join(linhas)


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--estimativa",
        action="store_true",
        help="usa pg_class.reltuples em vez de SELECT count(*) exato",
    )
    args = parser.parse_args()

    cols_by_table, contagens, fonte_contagem, inv_by_table, origem = _carregar_entradas(
        args.estimativa
    )

    # 1) Classificação de carga (por família, ordenada por período)
    familias: dict[str, list[tuple[str, str, int]]] = {}
    for tabela in cols_by_table:
        familia = chave_familia(tabela)
        periodo = _extrair_periodo_versao(
            tabela, inv_by_table.get(tabela) if inv_by_table else None
        )
        familias.setdefault(familia, []).append(
            (periodo or "", tabela, contagens.get(tabela, 0))
        )
    for familia in familias:
        familias[familia].sort(key=lambda v: (v[0], v[1]))

    carga_rows: list[list[str]] = []
    for tabela in sorted(cols_by_table):
        familia = chave_familia(tabela)
        variante = extrair_variante(tabela)
        nome_periodo = _detectar_token_periodo(tabela)
        modelo, evidencia = classificar_por_trajetoria(
            familias[familia], fonte_contagem, nome_periodo
        )
        carga_rows.append([tabela, familia, variante, modelo, evidencia])

    # 2) Drift de schema
    inv_para_drift = inv_by_table if inv_by_table is not None else {}
    drift_rows, novas, removidas = detectar_drift(cols_by_table, inv_para_drift)

    # 3) Relatório de revisão
    relatorio = gerar_relatorio_familias(carga_rows, contagens, fonte_contagem, origem)

    # 4) Stub TransfereGov
    transferegov_rows = gerar_stub_transferegov()

    n_carga = _escrever_csv(
        OUT_CARGA,
        ["tabela", "familia", "variante", "modelo_carga", "evidencia"],
        carga_rows,
    )
    n_drift = _escrever_csv(
        OUT_DRIFT, ["familia", "versao", "coluna", "status"], drift_rows
    )
    OUT_RELATORIO.write_text(relatorio, encoding="utf-8")
    n_tg = _escrever_csv(
        OUT_TRANSFEREGOV,
        ["tabela", "servico", "modelo_carga"],
        transferegov_rows,
    )

    # Resumo
    dist = Counter(row[3] for row in carga_rows)
    familias_n: dict[str, int] = {}
    for _tabela, familia, _variante, _modelo, _evidencia in carga_rows:
        familias_n[familia] = familias_n.get(familia, 0) + 1
    top_familias = sorted(familias_n.items(), key=lambda kv: (-kv[1], kv[0]))[:5]

    print(f"Saídas gravadas em {DATA_DIR}:")
    print(f"  {OUT_CARGA.name}: {n_carga} tabelas")
    print(f"  {OUT_DRIFT.name}: {n_drift} linhas de drift")
    print(f"  {OUT_RELATORIO.name}: relatório de revisão gerado")
    print(f"  {OUT_TRANSFEREGOV.name}: {n_tg} tabelas (stub)")
    print(f"Distribuição modelo_carga: {dict(dist)}")
    print(f"Nº de famílias: {len(familias_n)}")
    print("Top 5 famílias por nº de tabelas:")
    for familia, n in top_familias:
        print(f"  {familia}: {n}")
    print("Colunas 'nova' mais comuns:")
    for coluna, n in novas.most_common(5):
        print(f"  {n}  {coluna!r}")
    print("Colunas 'removida' mais comuns:")
    for coluna, n in removidas.most_common(5):
        print(f"  {n}  {coluna!r}")


if __name__ == "__main__":
    main()
