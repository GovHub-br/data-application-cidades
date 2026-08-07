"""Perfilamento e documentação automática das bases brutas (camada bronze).

Lê estrutura e estatísticas das tabelas de um Postgres e monta um YAML único no
padrão `sources` do dbt, pensado para alimentar RAG e BI via OpenMetadata.

Só produz fatos verificáveis (tipos, nulos, cardinalidade, min/max, exemplos).
Nenhuma descrição em linguagem natural é inventada: quando existe descrição
escrita à mão nos YAMLs do dbt, ela é reaproveitada; senão o campo fica ausente.

A saída é `airflow_lappis/dags/dbt/bronze.yml` — versionado, junto do
`.user.yml`, acima dos projetos ipea/mcid/mir.

Roda pela linha de comando (a partir da raiz do repositório):

    poetry run python airflow_lappis/plugins/documentador_bronze.py

ou pela DAG `bronze_documentacao_dag`, que grava no mesmo arquivo — o
diretório `dags/` é montado do repositório para dentro do container.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import yaml
from psycopg2 import sql

LIMITE_AMOSTRA = 10_000

# Núcleo curado da camada bruta. Os schemas de dump (sftp, sftp_v2,
# dados_historicos) ficam de fora da v1: são ~4.300 tabelas que colapsam em
# poucas centenas de estruturas e exigem decisão de escopo à parte.
SCHEMAS_PADRAO: Tuple[str, ...] = (
    "__dados_brutos",
    "conjuntura_bronze",
    "ibge",
    "fgv",
    "bacen",
    "abecip",
    "fipe",
    "infomoney",
    "novo_caged",
)

# Tipos que não suportam DISTINCT/min/max direto no Postgres.
TIPOS_OPACOS = frozenset(
    {"json", "jsonb", "xml", "bytea", "point", "polygon", "tsvector"}
)
TIPOS_NUMERICOS = frozenset(
    {"smallint", "integer", "bigint", "numeric", "real", "double precision", "money"}
)
TIPOS_TEMPORAIS = frozenset(
    {
        "date",
        "time without time zone",
        "time with time zone",
        "timestamp without time zone",
        "timestamp with time zone",
    }
)
TIPOS_TEXTO = frozenset({"text", "character varying", "character", "name", "citext"})

# Colunas cujo VALOR não pode sair daqui: o YAML é versionado e alimenta RAG.
# Estatística agregada (nulos, cardinalidade) continua sendo publicada.
#
# Identificador de pessoa e credencial: sensível sempre, sem exceção de contexto.
PADROES_SENSIVEIS_FORTES = re.compile(
    r"(^|_)("
    r"cpf|nis|pis|pasep|rg|titulo_eleitor|passaporte|cns|"
    r"nome_mae|nome_pai|mae|pai|"
    r"email|e_mail|telefone|celular|fone|"
    r"nascimento|dt_nascimento|data_nascimento|"
    r"senha|password|token|secret|"
    r"conta|conta_corrente|agencia|cartao|pix"
    r")($|_)",
    re.IGNORECASE,
)

# Sensível só quando o sujeito é uma pessoa — ver CONTEXTOS_NAO_PESSOAIS abaixo.
PADROES_SENSIVEIS_FRACOS = re.compile(
    r"(^|_)("
    r"nome|sobrenome|nome_completo|contato|"
    r"endereco|logradouro|complemento|numero_casa|cep"
    r")($|_)",
    re.IGNORECASE,
)

# `nome`/`endereco` sozinhos pegam demais: `municipio_nome` e `uf_nome` não são
# dado pessoal, e são exatamente as colunas categóricas cujos exemplos mais
# ajudam o RAG. Quando o nome da coluna traz um destes contextos, ela escapa do
# filtro — o sujeito é um lugar ou uma instituição, não uma pessoa.
# Plural irregular precisa entrar explícito: `regioes` não contém `regiao`.
# Os plurais em -s (municipios, cidades) já casam pelo stem.
CONTEXTOS_NAO_PESSOAIS = re.compile(
    r"(municipio|cidade|uf|estado|regi(?:ao|oes|onal)|"
    r"mesorregi(?:ao|oes)|microrregi(?:ao|oes)|pais|localidade|"
    r"bairro|distrito|territorio|"
    r"orgao|entidade|empresa|construtora|incorporadora|banco|agente_financeiro|"
    r"instituicao|ministerio|secretaria|fundo|programa|projeto|empreendimento|"
    r"obra|acao|produto|servico|tipologia|situacao|status|modalidade|categoria|"
    r"fonte|origem|arquivo|tabela|coluna|schema|campo|tipo|classe|grupo)",
    re.IGNORECASE,
)

# Tabela cujo sujeito é gente: aqui o contexto acima não vale como salvo-conduto,
# porque `nome`/`endereco` são mesmo da pessoa.
INDICIOS_DE_PESSOA = re.compile(
    r"(beneficiari|pessoa|cliente|servidor|funcionari|morador|candidat|"
    r"familia|titular|proponente|mutuari|cadunico|cadastro_unico|socio|"
    r"responsavel|dependente|conjuge|comprador|adquirente)",
    re.IGNORECASE,
)

# Defesa em profundidade: mesmo com nome inocente, valor com cara de CPF,
# CNPJ ou e-mail marca a coluna como sensível.
VALOR_SENSIVEL = re.compile(
    r"(\b\d{3}\.\d{3}\.\d{3}-\d{2}\b)"
    r"|(\b\d{11}\b)"
    r"|(\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b)"
    r"|([\w.+-]+@[\w-]+\.[\w.]+)"
)

MAX_EXEMPLOS = 5
MAX_CARDINALIDADE_EXEMPLOS = 50

# O bronze.yml nasce versionado, na raiz dos projetos dbt (junto do `.user.yml`).
# Fica acima de ipea/mcid/mir de propósito: nenhum `model-paths` alcança este
# nível, então o dbt não lê o arquivo como um `sources` duplicado.
RAIZ_DBT_PADRAO = "airflow_lappis/dags/dbt"
NOME_ARQUIVO = "bronze.yml"


def diretorio_de_trabalho() -> Path:
    """Onde ficam os parciais trocados entre as tasks — fora do repositório.

    Só o `bronze.yml` final é artefato do projeto; os JSONs intermediários são
    andaime e não devem sujar a árvore versionada.
    """
    return Path(tempfile.gettempdir()) / "documentador_bronze"


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- tipos


@dataclass
class Coluna:
    """Uma coluna, com metadados do catálogo e estatísticas da amostra."""

    nome: str
    tipo: str
    nullable: bool
    padrao: Optional[str] = None
    comentario: Optional[str] = None
    chave_primaria: bool = False
    sensivel: bool = False
    estatisticas: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Tabela:
    """Uma tabela (ou uma família de tabelas irmãs) já perfilada."""

    schema: str
    nome: str
    familia: str
    membros: List[str]
    linhas_estimadas: int
    linhas_amostradas: int
    comentario: Optional[str]
    colunas: List[Coluna]

    def impressao(self) -> str:
        """Impressão digital da estrutura, usada para pular tabela inalterada."""
        base = "|".join(f"{c.nome}:{c.tipo}" for c in self.colunas)
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def tabela_para_json(tab: Tabela) -> Dict[str, Any]:
    """Serializa para o parcial trocado entre as tasks do Airflow."""
    return asdict(tab)


def tabela_de_json(bruto: Dict[str, Any]) -> Tabela:
    """Reconstrói a partir do parcial, preservando os objetos Coluna.

    `asdict` achata as colunas em dicts; sem esta volta, a task de consolidação
    receberia dicionários onde espera Coluna.
    """
    dados = dict(bruto)
    dados["colunas"] = [Coluna(**c) for c in dados.get("colunas", [])]
    return Tabela(**dados)


# ------------------------------------------------------------------- utilidades


def familia_de(nome: str) -> str:
    """Normaliza dígitos para agrupar snapshots irmãos.

    `caixa_af_gehis_andamento_obra_m17` e `..._m182` viram a mesma família.
    Sem isso o YAML teria ~2.000 entradas quase idênticas competindo entre si
    na recuperação do RAG.
    """
    return re.sub(r"\d+", "#", nome)


def agrupar_por_familia(nomes: Sequence[str]) -> Dict[str, List[str]]:
    """Agrupa nomes de tabela por família, preservando a ordem de entrada."""
    grupos: Dict[str, List[str]] = {}
    for nome in nomes:
        grupos.setdefault(familia_de(nome), []).append(nome)
    return grupos


def nome_sensivel(nome: str, tabela: str = "") -> bool:
    """Decide, pelo nome da coluna e da tabela, se ela carrega dado pessoal.

    Identificador e credencial valem sempre. Termos genéricos como `nome` e
    `endereco` só valem quando o sujeito é uma pessoa — daí o contexto:
    `municipio_nome` é lugar, e a coluna `nome` de `api_ibge_uf` também é,
    porque a tabela diz do que se trata.

    Quando a tabela é de pessoas (`beneficiarios`, `proponentes`), o contexto
    não livra nada: ali `nome` é nome de gente.
    """
    if PADROES_SENSIVEIS_FORTES.search(nome):
        return True
    if not PADROES_SENSIVEIS_FRACOS.search(nome):
        return False
    if tabela and INDICIOS_DE_PESSOA.search(tabela):
        return True
    return not CONTEXTOS_NAO_PESSOAIS.search(f"{tabela}_{nome}" if tabela else nome)


def coluna_sensivel(nome: str, exemplos: Iterable[Any], tabela: str = "") -> bool:
    """Decide se a coluna carrega dado pessoal, por nome ou por formato do valor."""
    if nome_sensivel(nome, tabela):
        return True
    return any(VALOR_SENSIVEL.search(str(v)) for v in exemplos if v is not None)


def _categoria(tipo: str) -> str:
    if tipo in TIPOS_OPACOS:
        return "opaco"
    if tipo in TIPOS_NUMERICOS:
        return "numerico"
    if tipo in TIPOS_TEMPORAIS:
        return "temporal"
    if tipo in TIPOS_TEXTO:
        return "texto"
    if tipo.endswith("[]") or tipo == "ARRAY":
        return "opaco"
    return "outro"


# ------------------------------------------------------------------- catálogo


def listar_tabelas(conn: Any, schema: str) -> List[str]:
    """Nomes das tabelas base de um schema, em ordem alfabética."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = %s and table_type = 'BASE TABLE'
            order by table_name
            """,
            (schema,),
        )
        return [r[0] for r in cur.fetchall()]


def linhas_estimadas(conn: Any, schema: str, tabela: str) -> int:
    """Contagem aproximada via `reltuples`.

    COUNT(*) exato não se paga: há tabelas de dezenas de GB no DW.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select coalesce(c.reltuples, 0)::bigint
            from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = %s and c.relname = %s
            """,
            (schema, tabela),
        )
        linha = cur.fetchone()
    return max(int(linha[0]), 0) if linha else 0


def comentario_tabela(conn: Any, schema: str, tabela: str) -> Optional[str]:
    """COMMENT ON TABLE, se alguém tiver preenchido no banco."""
    with conn.cursor() as cur:
        cur.execute(
            "select obj_description(format('%%s.%%s', %s, %s)::regclass, 'pg_class')",
            (sql_ident(schema), sql_ident(tabela)),
        )
        linha = cur.fetchone()
    return linha[0] if linha and linha[0] else None


def sql_ident(nome: str) -> str:
    """Escapa identificador para interpolação textual segura."""
    return '"' + nome.replace('"', '""') + '"'


def metadados_colunas(conn: Any, schema: str, tabela: str) -> List[Coluna]:
    """Tipo, nullability, default, comentário e PK de cada coluna."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES',
                c.column_default,
                col_description(
                    format('%%I.%%I', c.table_schema, c.table_name)::regclass,
                    c.ordinal_position
                ),
                coalesce(pk.e, false)
            from information_schema.columns c
            left join (
                select ku.column_name, true as e
                from information_schema.table_constraints tc
                join information_schema.key_column_usage ku
                  on ku.constraint_name = tc.constraint_name
                 and ku.table_schema = tc.table_schema
                where tc.constraint_type = 'PRIMARY KEY'
                  and tc.table_schema = %s and tc.table_name = %s
            ) pk on pk.column_name = c.column_name
            where c.table_schema = %s and c.table_name = %s
            order by c.ordinal_position
            """,
            (schema, tabela, schema, tabela),
        )
        return [
            Coluna(
                nome=nome,
                tipo=tipo,
                nullable=nullable,
                padrao=padrao,
                comentario=comentario,
                chave_primaria=pk,
            )
            for nome, tipo, nullable, padrao, comentario, pk in cur.fetchall()
        ]


# ------------------------------------------------------------------- perfil


def _agregados_da_coluna(col: Coluna) -> List[sql.Composable]:
    """Monta as agregações cabíveis para o tipo da coluna."""
    ident = sql.Identifier(col.nome)
    partes: List[sql.Composable] = [
        sql.SQL("count({}) as {}").format(ident, sql.Identifier(f"{col.nome}__nn"))
    ]
    cat = _categoria(col.tipo)
    if cat == "opaco":
        return partes
    partes.append(
        sql.SQL("count(distinct {}) as {}").format(
            ident, sql.Identifier(f"{col.nome}__dist")
        )
    )
    if cat in ("numerico", "temporal"):
        partes.append(
            sql.SQL("min({})::text as {}").format(
                ident, sql.Identifier(f"{col.nome}__min")
            )
        )
        partes.append(
            sql.SQL("max({})::text as {}").format(
                ident, sql.Identifier(f"{col.nome}__max")
            )
        )
    elif cat == "texto":
        partes.append(
            sql.SQL("min(length({})) as {}").format(
                ident, sql.Identifier(f"{col.nome}__lmin")
            )
        )
        partes.append(
            sql.SQL("max(length({})) as {}").format(
                ident, sql.Identifier(f"{col.nome}__lmax")
            )
        )
    return partes


def perfilar(
    conn: Any,
    schema: str,
    tabela: str,
    colunas: List[Coluna],
    limite: int = LIMITE_AMOSTRA,
) -> int:
    """Preenche `estatisticas` das colunas com uma única query sobre a amostra.

    Uma query por tabela (não por coluna): com ~180 tabelas de dezenas de
    colunas, o caminho ingênuo faria milhares de varreduras.

    Returns:
        Quantidade de linhas efetivamente amostradas.
    """
    if not colunas:
        return 0

    agregados: List[sql.Composable] = [sql.SQL("count(*) as __n")]
    for col in colunas:
        agregados.extend(_agregados_da_coluna(col))

    query = sql.SQL(
        "with amostra as (select * from {}.{} limit {}) select {} from amostra"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(tabela),
        sql.Literal(limite),
        sql.SQL(", ").join(agregados),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        nomes = [d[0] for d in cur.description]
        valores = cur.fetchone()
    bruto = dict(zip(nomes, valores or []))

    total = int(bruto.get("__n") or 0)
    for col in colunas:
        nao_nulo = bruto.get(f"{col.nome}__nn")
        est: Dict[str, Any] = {"amostra": total}
        if nao_nulo is not None and total:
            est["nulos_pct"] = round((total - int(nao_nulo)) * 100.0 / total, 2)
        for chave, sufixo in (
            ("distintos", "__dist"),
            ("min", "__min"),
            ("max", "__max"),
            ("tamanho_min", "__lmin"),
            ("tamanho_max", "__lmax"),
        ):
            valor = bruto.get(f"{col.nome}{sufixo}")
            if valor is not None:
                est[chave] = valor
        col.estatisticas = est
    return total


def _parece_categorica(col: Coluna) -> bool:
    """Coluna com poucos valores repetidos — é onde o exemplo ajuda o RAG.

    Só o teto de cardinalidade não basta: numa amostra pequena, uma coluna de
    identificador ou de medida tem tantos valores distintos quanto linhas.
    Exigir repetição descarta esses casos.
    """
    distintos = int(col.estatisticas.get("distintos") or 0)
    amostra = int(col.estatisticas.get("amostra") or 0)
    if distintos <= 0 or amostra <= 0 or distintos > MAX_CARDINALIDADE_EXEMPLOS:
        return False
    return distintos / amostra <= 0.5


def coletar_exemplos(
    conn: Any,
    schema: str,
    tabela: str,
    colunas: List[Coluna],
    limite: int = LIMITE_AMOSTRA,
) -> None:
    """Coleta valores de exemplo só para coluna categórica e não sensível.

    Roda depois de `perfilar` porque depende da cardinalidade já medida: só faz
    sentido exemplificar coluna de baixa cardinalidade, e é ali que o exemplo
    ajuda o RAG a desambiguar. Colunas sensíveis nunca chegam aqui.
    """
    candidatas = [
        c
        for c in colunas
        if _categoria(c.tipo) in ("texto", "outro")
        and not nome_sensivel(c.nome, tabela)
        and _parece_categorica(c)
    ]
    for col in candidatas:
        consulta = sql.SQL(
            "with amostra as (select {c} as v from {s}.{t} limit {lim}) "
            "select v from amostra where v is not null "
            "group by v order by count(*) desc limit {top}"
        ).format(
            c=sql.Identifier(col.nome),
            s=sql.Identifier(schema),
            t=sql.Identifier(tabela),
            lim=sql.Literal(limite),
            top=sql.Literal(MAX_EXEMPLOS),
        )
        with conn.cursor() as cur:
            cur.execute(consulta)
            exemplos = [r[0] for r in cur.fetchall()]
        if coluna_sensivel(col.nome, exemplos, tabela):
            col.sensivel = True
            continue
        if exemplos:
            col.estatisticas["exemplos"] = [str(e)[:120] for e in exemplos]

    # Marca as demais que o nome já denuncia, mesmo sem ter buscado exemplo.
    for col in colunas:
        if nome_sensivel(col.nome, tabela):
            col.sensivel = True


def contar_linhas(
    conn: Any, schema: str, tabela: str, amostradas: int, limite: int
) -> int:
    """Contagem de linhas, exata quando sai de graça e estimada quando não.

    Se a amostra veio com menos linhas que o limite pedido, ela varreu a tabela
    inteira — então `amostradas` É a contagem exata, sem custo nenhum. Só acima
    do limite recorremos ao `reltuples`, que é aproximado e vale -1 em tabela
    que nunca passou por ANALYZE.
    """
    if amostradas < limite:
        return amostradas
    return linhas_estimadas(conn, schema, tabela)


def representante_da_familia(membros: Sequence[str]) -> str:
    """Escolhe a tabela mais recente da família.

    Ordenação alfabética erra em sufixo numérico (`_m2` viria depois de `_m12`),
    então os trechos de dígito são comparados como número.
    """

    def chave(nome: str) -> List[Any]:
        return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", nome) if p != ""]

    return max(membros, key=chave)


def perfilar_tabela(
    conn: Any,
    schema: str,
    tabela: str,
    membros: Sequence[str],
    limite: int = LIMITE_AMOSTRA,
) -> Tabela:
    """Perfila uma tabela representante e devolve o registro consolidado."""
    colunas = metadados_colunas(conn, schema, tabela)
    amostradas = perfilar(conn, schema, tabela, colunas, limite)
    coletar_exemplos(conn, schema, tabela, colunas, limite)
    return Tabela(
        schema=schema,
        nome=tabela,
        familia=familia_de(tabela),
        membros=list(membros),
        linhas_estimadas=contar_linhas(conn, schema, tabela, amostradas, limite),
        linhas_amostradas=amostradas,
        comentario=None,
        colunas=colunas,
    )


def perfilar_schema(
    conn: Any,
    schema: str,
    limite: int = LIMITE_AMOSTRA,
    anterior: Optional[Dict[str, Any]] = None,
) -> List[Tabela]:
    """Perfila um schema inteiro, uma tabela por família.

    Args:
        anterior: perfil da execução passada, para pular tabela cuja estrutura
            não mudou (chave: `schema.familia`, valor: dict com `impressao`).
    """
    tabelas = listar_tabelas(conn, schema)
    grupos = agrupar_por_familia(tabelas)
    logger.info(
        "[documentador_bronze] %s: %d tabelas em %d famílias",
        schema,
        len(tabelas),
        len(grupos),
    )

    resultado: List[Tabela] = []
    for familia, membros in grupos.items():
        # Nas famílias de snapshot (`_m1`.._m182`) a mais recente é a que
        # interessa perfilar: é a que reflete a estrutura e os dados atuais.
        representante = representante_da_familia(membros)
        try:
            registro = perfilar_tabela(conn, schema, representante, membros, limite)
        except psycopg2.Error as e:
            conn.rollback()
            logger.warning(
                "[documentador_bronze] falhou em %s.%s: %s", schema, representante, e
            )
            continue
        chave = f"{schema}.{familia}"
        previo = (anterior or {}).get(chave)
        if previo and previo.get("impressao") == registro.impressao():
            logger.info("[documentador_bronze] %s inalterada", chave)
        resultado.append(registro)
    return resultado


# ----------------------------------------------- descrições já escritas à mão


def carregar_descricoes_dbt(raiz_dbt: Path) -> Dict[str, str]:
    """Indexa as descrições escritas à mão nos YAMLs do dbt.

    Chaves aceitas, da mais específica para a mais genérica:
        "schema.tabela", "tabela", "schema.tabela.coluna", "tabela.coluna"

    A ideia é não reescrever o que a equipe já documentou com cuidado: o texto
    existente vira a descrição da entrada correspondente no bronze.yml.
    """
    indice: Dict[str, str] = {}
    if not raiz_dbt.exists():
        return indice

    for arquivo in sorted(raiz_dbt.rglob("*.yml")):
        if "target" in arquivo.parts or "dbt_packages" in arquivo.parts:
            continue
        try:
            doc = yaml.safe_load(arquivo.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            logger.warning("[documentador_bronze] YAML ilegível: %s", arquivo)
            continue
        if not isinstance(doc, dict):
            continue
        _indexar_sources(doc, indice)
        _indexar_modelos(doc, indice)
    logger.info("[documentador_bronze] %d descrições reaproveitadas do dbt", len(indice))
    return indice


def _guardar(indice: Dict[str, str], chave: str, texto: Any) -> None:
    if isinstance(texto, str) and texto.strip():
        indice.setdefault(chave, " ".join(texto.split()))


def _indexar_sources(doc: Dict[str, Any], indice: Dict[str, str]) -> None:
    for src in doc.get("sources") or []:
        if not isinstance(src, dict):
            continue
        schema = src.get("schema") or src.get("name")
        for tab in src.get("tables") or []:
            if not isinstance(tab, dict) or not tab.get("name"):
                continue
            nome = tab["name"]
            _guardar(indice, f"{schema}.{nome}", tab.get("description"))
            _guardar(indice, nome, tab.get("description"))
            for col in tab.get("columns") or []:
                if isinstance(col, dict) and col.get("name"):
                    _guardar(
                        indice, f"{schema}.{nome}.{col['name']}", col.get("description")
                    )
                    _guardar(indice, f"{nome}.{col['name']}", col.get("description"))


def _indexar_modelos(doc: Dict[str, Any], indice: Dict[str, str]) -> None:
    for mdl in doc.get("models") or []:
        if not isinstance(mdl, dict) or not mdl.get("name"):
            continue
        nome = mdl["name"]
        _guardar(indice, nome, mdl.get("description"))
        for col in mdl.get("columns") or []:
            if isinstance(col, dict) and col.get("name"):
                _guardar(indice, f"{nome}.{col['name']}", col.get("description"))


# ------------------------------------------------------------------- YAML


def _coluna_para_dict(
    col: Coluna, tabela: str, schema: str, descricoes: Dict[str, str]
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"tipo": col.tipo, "nullable": col.nullable}
    if col.chave_primaria:
        meta["chave_primaria"] = True
    if col.sensivel:
        meta["sensivel"] = True
        meta["motivo_sensivel"] = "possível dado pessoal — valores omitidos"
    meta.update(col.estatisticas)

    saida: Dict[str, Any] = {"name": col.nome}
    texto = (
        col.comentario
        or descricoes.get(f"{schema}.{tabela}.{col.nome}")
        or descricoes.get(f"{tabela}.{col.nome}")
    )
    if texto:
        saida["description"] = texto
    saida["meta"] = meta
    return saida


def _tabela_para_dict(tab: Tabela, descricoes: Dict[str, str]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "linhas_estimadas": tab.linhas_estimadas,
        "linhas_amostradas": tab.linhas_amostradas,
        "impressao": tab.impressao(),
    }
    if len(tab.membros) > 1:
        meta["familia"] = tab.familia
        meta["membros"] = len(tab.membros)
        meta["tabelas_da_familia"] = tab.membros
        meta["perfilada"] = tab.nome

    saida: Dict[str, Any] = {"name": tab.nome}
    texto = (
        tab.comentario
        or descricoes.get(f"{tab.schema}.{tab.nome}")
        or descricoes.get(tab.nome)
    )
    if texto:
        saida["description"] = texto
    saida["meta"] = meta
    saida["columns"] = [
        _coluna_para_dict(c, tab.nome, tab.schema, descricoes) for c in tab.colunas
    ]
    return saida


def montar_documento(
    por_schema: Dict[str, List[Tabela]], descricoes: Dict[str, str]
) -> Dict[str, Any]:
    """Monta o dicionário no formato `sources` do dbt."""
    sources = []
    for schema in sorted(por_schema):
        tabelas = por_schema[schema]
        sources.append(
            {
                "name": schema,
                "schema": schema,
                "meta": {
                    "tabelas_documentadas": len(tabelas),
                    "camada": "bronze",
                },
                "tables": [
                    _tabela_para_dict(t, descricoes)
                    for t in sorted(tabelas, key=lambda t: t.nome)
                ],
            }
        )
    return {
        "version": 2,
        "_gerado_por": "documentador_bronze",
        "_gerado_em": datetime.now(timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds"),
        "_amostra_por_tabela": LIMITE_AMOSTRA,
        "sources": sources,
    }


def escrever_yaml(documento: Dict[str, Any], destino: Path) -> Path:
    """Grava o YAML final, criando o diretório se preciso."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(
        yaml.safe_dump(documento, allow_unicode=True, sort_keys=False, width=100),
        encoding="utf-8",
    )
    logger.info("[documentador_bronze] escrito %s", destino)
    return destino


def carregar_anterior(caminho: Path) -> Dict[str, Any]:
    """Lê o bronze.yml da execução passada para permitir pular o inalterado.

    O próprio artefato é o estado — não precisa de tabela de controle.
    """
    if not caminho.exists():
        return {}
    try:
        doc = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    estado: Dict[str, Any] = {}
    for src in doc.get("sources") or []:
        schema = src.get("name")
        for tab in src.get("tables") or []:
            meta = tab.get("meta") or {}
            familia = meta.get("familia") or familia_de(tab.get("name", ""))
            estado[f"{schema}.{familia}"] = {"impressao": meta.get("impressao")}
    return estado


# --------------------------------------------------------------------------- CLI


def carregar_env(caminho: Path) -> None:
    """Carrega um arquivo .env no ambiente, sem sobrescrever o que já existe.

    Conveniência só do uso local pela linha de comando: no Airflow as
    credenciais vêm da Connection. O parsing é feito aqui, e não via `source`
    no shell, porque as senhas do .env contêm caracteres que o shell interpreta.
    """
    if not caminho.exists():
        return
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def conexao_do_ambiente() -> Any:
    """Abre conexão usando as variáveis DB_DW_*_MCID do ambiente."""
    return psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=os.environ.get("DB_DW_PORT_MCID", "5432"),
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ.get("DB_DW_DBNAME_MCID", "cidades"),
        connect_timeout=15,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schemas", nargs="*", default=list(SCHEMAS_PADRAO))
    parser.add_argument(
        "--saida",
        default=RAIZ_DBT_PADRAO,
        help="diretório onde o bronze.yml é gravado (padrão: raiz dos projetos dbt)",
    )
    parser.add_argument("--limite", type=int, default=LIMITE_AMOSTRA)
    parser.add_argument("--raiz-dbt", default=RAIZ_DBT_PADRAO)
    parser.add_argument(
        "--parcial",
        action="store_true",
        help="grava um JSON por schema num diretório de trabalho, em vez do YAML final",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="arquivo com as credenciais DB_DW_*_MCID (uso local)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    carregar_env(Path(args.env_file))
    destino = Path(args.saida) / NOME_ARQUIVO
    anterior = carregar_anterior(destino)

    conn = conexao_do_ambiente()
    try:
        por_schema = {
            schema: perfilar_schema(conn, schema, args.limite, anterior)
            for schema in args.schemas
        }
    finally:
        conn.close()

    if args.parcial:
        for schema, tabelas in por_schema.items():
            alvo = diretorio_de_trabalho() / f"{schema}.json"
            alvo.parent.mkdir(parents=True, exist_ok=True)
            alvo.write_text(
                json.dumps([tabela_para_json(t) for t in tabelas], ensure_ascii=False),
                encoding="utf-8",
            )
        return 0

    descricoes = carregar_descricoes_dbt(Path(args.raiz_dbt))
    escrever_yaml(montar_documento(por_schema, descricoes), destino)
    total = sum(len(v) for v in por_schema.values())
    print(f"OK: {total} tabelas documentadas em {destino}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
