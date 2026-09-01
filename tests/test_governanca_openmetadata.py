"""Regras de catalogação que decidem o que aparece no OpenMetadata.

São testadas aqui porque erram em silêncio: uma etiqueta errada não quebra o
sync, só publica a tabela com a certificação de outra camada. O caso do
`empreendimento_far` é o exemplo — o schema é declarado `mixed`, e usar a
camada do schema daria `Tier1` e `Uso.Consumivel` às tabelas bronze dele.
"""

from __future__ import annotations

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts" / "governance"))

import governanca_comum as comum  # noqa: E402
import sincronizar_governanca as gov  # noqa: E402
import sincronizar_openmetadata as estrutura  # noqa: E402

DOMINIOS = comum.carregar("dominios.yml")


def fqns(etiquetas: list[dict]) -> set[str]:
    return {e["tagFQN"] for e in etiquetas}


# ── camada, tier, certificação e permissão de uso ───────────────────────────
def test_gold_e_consumivel_e_certificada_como_tier1() -> None:
    aplicadas = fqns(gov.etiquetas_da_camada(DOMINIOS, "conjuntura", "gold"))
    assert aplicadas == {
        "dbtTags.mcid",
        "dbtTags.conjuntura",
        "dbtTags.gold",
        "Tier.Tier1",
        "Uso.Consumivel",
    }
    # a certificação não é etiqueta nesta instância; vai no campo próprio
    assert gov.certificacao_da_camada(DOMINIOS, "gold") == "Certification.Gold"


def test_bronze_entra_no_catalogo_mas_nao_e_consumivel() -> None:
    aplicadas = fqns(gov.etiquetas_da_camada(DOMINIOS, "conjuntura", "bronze"))
    assert "Uso.NaoConsumivel" in aplicadas
    assert "Tier.Tier3" in aplicadas
    assert gov.certificacao_da_camada(DOMINIOS, "bronze") == "Certification.Bronze"


def test_silver_e_apoio_interno_e_nao_destino_de_consumo() -> None:
    aplicadas = fqns(gov.etiquetas_da_camada(DOMINIOS, "conjuntura", "silver"))
    assert "Uso.ApoioInterno" in aplicadas
    assert "Uso.Consumivel" not in aplicadas


def test_bronze_do_far_nao_herda_a_camada_do_schema() -> None:
    """O schema `empreendimento_far` é `mixed`, mas os modelos dele não são.

    Se a camada viesse do schema, toda tabela bronze do FAR sairia certificada
    como Gold e marcada como consumível.
    """
    camadas = gov.camadas_por_modelo()
    if not camadas:
        return  # o catálogo semântico não foi gerado neste ambiente
    bronzes = [nome for nome, camada in camadas.items() if camada == "bronze"]
    assert bronzes, "o catálogo precisa ter ao menos um modelo bronze"
    for nome in bronzes:
        aplicadas = fqns(
            gov.etiquetas_da_camada(DOMINIOS, "empreendimento_far", "bronze")
        )
        assert "Uso.NaoConsumivel" in aplicadas, nome


def test_toda_etiqueta_aplicada_esta_declarada() -> None:
    """Nada é pendurado sem existir: era o que fazia a etiquetagem falhar."""
    declaradas = {
        f"{c['name']}.{t['name']}"
        for c in DOMINIOS["classificacoes"]
        for t in c["etiquetas"]
    }
    nativas = set(DOMINIOS["classificacoes_nativas"])
    for produto in ("conjuntura", "empreendimento_far", "entidades_fds"):
        for camada in ("bronze", "silver", "gold", "mixed"):
            for fqn in fqns(gov.etiquetas_da_camada(DOMINIOS, produto, camada)):
                assert fqn in declaradas or fqn.split(".")[0] in nativas, fqn


# ── mescla de etiquetas ─────────────────────────────────────────────────────
def test_mescla_preserva_classificacao_que_nao_governamos() -> None:
    atuais = [comum.etiqueta("PII.Sensitive"), comum.etiqueta("dbtTags.silver")]
    desejadas = [comum.etiqueta("dbtTags.gold")]
    resultado = fqns(gov.mesclar_etiquetas(atuais, desejadas))  # sem vocabulário nosso
    assert "PII.Sensitive" in resultado  # não é nossa: fica
    assert "dbtTags.silver" not in resultado  # é nossa e saiu do YAML: some
    assert "dbtTags.gold" in resultado


def test_mescla_nao_duplica() -> None:
    atuais = [comum.etiqueta("PII.NonSensitive")]
    desejadas = [comum.etiqueta("PII.NonSensitive"), comum.etiqueta("Uso.Consumivel")]
    resultado = gov.mesclar_etiquetas(atuais, desejadas)
    assert len(resultado) == len(fqns(resultado))


# ── idempotência ────────────────────────────────────────────────────────────
def test_entidade_ja_conforme_nao_gera_patch() -> None:
    """`rodar duas vezes` tem de terminar com `atualizados=0`."""
    atual = {
        "displayName": "Conjuntura Habitacional",
        "description": "Indicadores do setor habitacional.",
        "owners": [{"id": "abc", "type": "team", "name": "mcid-data-engineering"}],
    }
    desejado = {
        "displayName": "Conjuntura Habitacional",
        "description": "Indicadores do setor habitacional.",
        "owners": [{"id": "abc", "type": "team", "name": "mcid-data-engineering"}],
    }
    assert comum.operacoes_de_diferenca(atual, desejado) == []


def test_diferenca_ignora_campos_que_a_api_acrescenta() -> None:
    """A API devolve href e displayName nas referências; não é divergência."""
    atual = {
        "domains": [
            {
                "id": "1",
                "type": "domain",
                "name": "Habitacao",
                "fullyQualifiedName": "MCid.Habitacao",
                "href": "https://exemplo/api/v1/domains/1",
                "deleted": False,
            }
        ]
    }
    desejado = {
        "domains": [
            {
                "id": "1",
                "type": "domain",
                "name": "Habitacao",
                "fullyQualifiedName": "MCid.Habitacao",
            }
        ]
    }
    assert comum.operacoes_de_diferenca(atual, desejado) == []


def test_campo_que_mudou_gera_patch() -> None:
    operacoes = comum.operacoes_de_diferenca(
        {"description": "antiga"}, {"description": "nova"}
    )
    assert operacoes == [{"op": "add", "path": "/description", "value": "nova"}]


# ── tipos de coluna ─────────────────────────────────────────────────────────
def test_numerico_preserva_precisao_e_escala() -> None:
    coluna = estrutura.om_column(
        {"name": "valor", "data_type": "numeric(15,2)", "description": "x"}
    )
    assert coluna["dataType"] == "NUMERIC"
    assert coluna["precision"] == 15
    assert coluna["scale"] == 2


def test_texto_sem_limite_nao_recebe_comprimento_inventado() -> None:
    coluna = estrutura.om_column(
        {"name": "nome", "data_type": "text", "description": "x"}
    )
    assert coluna["dataType"] == "TEXT"
    assert "dataLength" not in coluna


def test_varchar_com_limite_declara_o_limite_real() -> None:
    coluna = estrutura.om_column(
        {"name": "uf", "data_type": "character varying(2)", "description": "x"}
    )
    assert coluna["dataType"] == "VARCHAR"
    assert coluna["dataLength"] == 2


def test_timestamp_com_fuso_e_distinguido_de_sem_fuso() -> None:
    com = estrutura.om_column(
        {"name": "a", "data_type": "timestamp with time zone", "description": "x"}
    )
    sem = estrutura.om_column(
        {"name": "b", "data_type": "timestamp without time zone", "description": "x"}
    )
    assert com["dataType"] == "TIMESTAMPZ"
    assert sem["dataType"] == "TIMESTAMP"


def test_array_declara_o_tipo_interno() -> None:
    coluna = estrutura.om_column(
        {"name": "ids", "data_type": "integer[]", "description": "x"}
    )
    assert coluna["dataType"] == "ARRAY"
    assert coluna["arrayDataType"] == "INT"


def test_posicao_fisica_e_publicada_quando_conhecida() -> None:
    coluna = estrutura.om_column(
        {"name": "a", "data_type": "text", "description": "x", "ordinal": 3}
    )
    assert coluna["ordinalPosition"] == 3
    sem_posicao = estrutura.om_column(
        {"name": "a", "data_type": "text", "description": "x", "ordinal": None}
    )
    assert "ordinalPosition" not in sem_posicao


# ── declaração dos schemas ──────────────────────────────────────────────────
def test_todo_schema_tem_descricao_curada_e_nome_de_exibicao() -> None:
    for schema in comum.carregar("schemas.yml")["schemas"]:
        assert schema.get("description"), schema["name"]
        assert schema.get("display_name"), schema["name"]
        assert schema.get("owner_key") in comum.carregar("dominios.yml")["proprietarios"]


def test_termo_relacionado_aponta_para_termo_conhecido() -> None:
    """Relação para termo inexistente vira falha no sync; melhor pegar aqui."""
    termos = comum.carregar("termos_mcid.yml")
    declarados = {t["fqn"] for t in termos["termos"]}
    existentes = set(termos.get("aplicacao_de_termos_existentes") or {})
    for termo in termos["termos"]:
        for outro in termo.get("related_terms") or []:
            assert outro in declarados or outro in existentes, outro


def test_schema_de_camada_mista_nao_recebe_permissao_de_uso() -> None:
    """`mixed` descreve schema com bronze, silver e gold juntos.

    Marcá-lo consumível contradiria as tabelas bronze que ele contém, que saem
    explicitamente como não consumíveis.
    """
    aplicadas = fqns(gov.etiquetas_da_camada(DOMINIOS, "empreendimento_far", "mixed"))
    assert not [f for f in aplicadas if f.startswith("Uso.")]
    assert "Tier.Tier1" in aplicadas  # a criticidade do schema continua declarada


def test_certificacao_nao_vai_junto_das_etiquetas() -> None:
    """Nesta instância a certificação é campo próprio (`/certification`).

    Mandada dentro de `/tags`, o OpenMetadata devolve 200 e descarta em
    silêncio — foi assim que a primeira aplicação saiu com Tier e permissão de
    uso, mas sem certificação nenhuma.
    """
    for camada in ("bronze", "silver", "gold", "mixed"):
        aplicadas = fqns(gov.etiquetas_da_camada(DOMINIOS, "conjuntura", camada))
        assert not [f for f in aplicadas if f.startswith("Certification.")], camada


def test_cada_camada_declara_a_sua_certificacao() -> None:
    assert gov.certificacao_da_camada(DOMINIOS, "gold") == "Certification.Gold"
    assert gov.certificacao_da_camada(DOMINIOS, "silver") == "Certification.Silver"
    assert gov.certificacao_da_camada(DOMINIOS, "bronze") == "Certification.Bronze"
    assert gov.certificacao_da_camada(DOMINIOS, "inexistente") is None


def test_relacao_entre_termos_usa_o_formato_da_api() -> None:
    """`relatedTerms` é `TermRelation` (term + relationType), não referência solta."""
    entidade = {"id": "1", "name": "Funding", "fullyQualifiedName": "MCID.X.Funding"}
    relacao = comum.relacao_de_termo(entidade)
    assert set(relacao) == {"term", "relationType"}
    assert relacao["term"]["type"] == "glossaryTerm"


def test_relacao_ja_aplicada_nao_gera_patch() -> None:
    """Sem tratar o embrulho `term`, os dois lados reduziriam a vazio e o
    patch nunca sairia — a relação ficaria eternamente por aplicar."""
    entidade = {"id": "1", "name": "Funding", "fullyQualifiedName": "MCID.X.Funding"}
    desejado = [comum.relacao_de_termo(entidade)]
    assert (
        comum.operacoes_de_diferenca(
            {"relatedTerms": desejado}, {"relatedTerms": desejado}
        )
        == []
    )
    outro = {"id": "2", "name": "Outro", "fullyQualifiedName": "MCID.X.Outro"}
    assert comum.operacoes_de_diferenca(
        {"relatedTerms": [comum.relacao_de_termo(outro)]}, {"relatedTerms": desejado}
    )


def test_coluna_com_tipo_desatualizado_e_detectada() -> None:
    """O `VARCHAR(65535)` da carga antiga tem de ser reconhecido como divergente."""
    atual = [
        {
            "name": "periodo",
            "dataType": "VARCHAR",
            "dataLength": 65535,
            "description": "x",
        }
    ]
    desejada = [{"name": "periodo", "dataType": "TEXT", "description": "x"}]
    assert estrutura.colunas_divergem(atual, desejada)


def test_colunas_iguais_nao_geram_reescrita() -> None:
    iguais = [
        {
            "name": "valor",
            "dataType": "NUMERIC",
            "precision": 15,
            "scale": 2,
            "description": "Valor em reais.",
        }
    ]
    assert not estrutura.colunas_divergem(iguais, [dict(c) for c in iguais])


def test_coluna_nova_ou_removida_e_divergencia() -> None:
    base = [{"name": "a", "dataType": "TEXT", "description": "x"}]
    assert estrutura.colunas_divergem(
        base, base + [{"name": "b", "dataType": "TEXT", "description": "y"}]
    )
    assert estrutura.colunas_divergem(
        base + [{"name": "b", "dataType": "TEXT", "description": "y"}], base
    )


def test_descricao_escapada_pela_instancia_nao_conta_como_divergencia() -> None:
    """O OpenMetadata grava `=`, `'` e `\"` como entidade HTML.

    Sem normalizar, toda descrição com um desses caracteres divergiria para
    sempre: o sync reescreve, a instância reescapa, e na execução seguinte a
    diferença reaparece. Seis tabelas do FAR e do FDS caíam nisso.
    """
    gravado = "Filtra ic_credito&#61;&#39;0&#39; e o gráfico &#34;Evolução&#34;"
    curado = "Filtra ic_credito='0' e o gráfico \"Evolução\""
    assert comum.mesmo_texto(gravado, curado)
    assert (
        comum.operacoes_de_diferenca({"description": gravado}, {"description": curado})
        == []
    )


def test_coluna_so_com_descricao_escapada_nao_e_reescrita() -> None:
    atual = [
        {"name": "obs", "dataType": "TEXT", "description": "vale ic&#61;&#39;1&#39;"}
    ]
    desejada = [{"name": "obs", "dataType": "TEXT", "description": "vale ic='1'"}]
    assert not estrutura.colunas_divergem(atual, desejada)


def test_descricao_realmente_diferente_ainda_e_divergencia() -> None:
    assert not comum.mesmo_texto("texto antigo", "texto novo")


def test_relacao_entre_termos_e_declarada_nos_dois_sentidos() -> None:
    """A relação é bidirecional no OpenMetadata e o patch substitui a lista.

    Declarando só `Bronze -> Silver`, o vínculo morria assim que Silver
    recebesse a dele: o patch de Silver não continha Bronze e apagava o
    recíproco. A auditoria pegou exatamente esse caso.
    """
    declarados = [
        {"fqn": "MCID.Arquitetura.Bronze", "related_terms": ["MCID.Arquitetura.Silver"]},
        {"fqn": "MCID.Arquitetura.Silver", "related_terms": ["MCID.Arquitetura.Gold"]},
        {"fqn": "MCID.Arquitetura.Gold"},
    ]
    mapa = gov._relacoes_reciprocas(declarados)
    assert mapa["MCID.Arquitetura.Silver"] == [
        "MCID.Arquitetura.Bronze",
        "MCID.Arquitetura.Gold",
    ]
    assert mapa["MCID.Arquitetura.Gold"] == ["MCID.Arquitetura.Silver"]
    assert mapa["MCID.Arquitetura.Bronze"] == ["MCID.Arquitetura.Silver"]


# ── propriedade do vocabulário de glossário ─────────────────────────────────
def test_termo_pendurado_por_pessoa_nao_e_removido() -> None:
    """Só tiramos termo que o repo declara.

    O schema `conjuntura_continuo_mart` tinha `MCID.IndicadoresConjunturais`
    aplicado na mão. O sync o removia toda execução: nunca convergia e desfazia
    curadoria de quem trabalha na instância.
    """
    nosso = {"MCID.FundosEFontes.FGTS"}
    atuais = [
        comum.etiqueta("MCID.IndicadoresConjunturais", "Glossary"),  # de uma pessoa
        comum.etiqueta("MCID.FundosEFontes.FGTS", "Glossary"),  # nosso, saiu do YAML
    ]
    resultado = fqns(gov.mesclar_etiquetas(atuais, [], nosso))
    assert "MCID.IndicadoresConjunturais" in resultado
    assert "MCID.FundosEFontes.FGTS" not in resultado


def test_vocabulario_declarado_cobre_as_tres_secoes() -> None:
    termos = comum.carregar("termos_mcid.yml")
    vocab = gov.vocabulario_declarado(termos)
    assert "MCID.Governanca.Safra" in vocab  # seção `termos`
    assert "MCID.Atores.CAIXA" in vocab  # seção `aplicacao_de_termos_existentes`
    assert "MCID.IndicadoresConjunturais" not in vocab  # eixo, nunca declarado por nós


def test_termo_nosso_continua_sendo_aplicado() -> None:
    nosso = {"MCID.Governanca.Safra"}
    desejadas = [comum.etiqueta("MCID.Governanca.Safra", "Glossary")]
    assert "MCID.Governanca.Safra" in fqns(gov.mesclar_etiquetas([], desejadas, nosso))


# ── FQN de container do lake ────────────────────────────────────────────────
def test_fqn_de_container_cita_segmento_com_ponto() -> None:
    """Sem citar, a busca não acha as folhas e o sync as recria toda execução."""
    import sincronizar_lake as lake

    fqn = lake.fqn_do_container(
        "Cidades - MinIO",
        ("data-lake-mcid", "staging", "infomoney", "acoes_imob.parquet"),
    )
    assert fqn == 'Cidades - MinIO.data-lake-mcid.staging.infomoney."acoes_imob.parquet"'


def test_fqn_de_container_sem_ponto_fica_intacto() -> None:
    import sincronizar_lake as lake

    fqn = lake.fqn_do_container(
        "Cidades - MinIO", ("data-lake-mcid", "staging", "abecip")
    )
    assert fqn == "Cidades - MinIO.data-lake-mcid.staging.abecip"


def test_mapa_de_arquivos_do_lake_bate_com_a_bronze() -> None:
    """Toda fonte declarada tem que ser lida por alguma Bronze, e vice-versa."""
    import sincronizar_lake as lake

    arquivos = lake.carregar_arquivos()
    assert arquivos, "nenhuma fonte com meta.caminho encontrada"
    sem_bronze = [a.fonte for a in arquivos if not a.bronze]
    assert not sem_bronze, f"fontes do lake sem model bronze: {sem_bronze}"


def test_termo_de_produto_usa_fqn_completo() -> None:
    """FQN de termo aninhado não é eixo + folha.

    `MCMVFAR` fica sob `MCMV`, não direto sob `ProgramasHabitacionais`.
    Concatenar eixo e folha devolve 404 e derruba o patch inteiro do produto.
    """
    dominios = comum.carregar("dominios.yml")
    for produto in dominios["produtos"]:
        for fqn in produto.get("termos") or []:
            assert fqn.startswith("MCID."), fqn
            assert "MCID.ProgramasHabitacionais.MCMVFAR" != fqn
            assert "MCID.ProgramasHabitacionais.MCMVEntidades" != fqn


# ── linhagem de coluna (HU-27) ──────────────────────────────────────────────
def test_linhagem_de_coluna_segue_a_expressao_e_nao_o_nome() -> None:
    """`data_referencia` vem de `periodo` — nomes diferentes, origem real.

    Uma heurística de nome igual erraria os dois casos: diria que
    `data_referencia` não tem origem e que `custo_medio_m2` vem de nada.
    """
    import linhagem_colunas as lin

    sql = lin.sql_analisavel(
        "with base as (select * from {{ ref('silver_x') }})\n"
        "select periodo, to_date(periodo, 'YYYYMM') as data_referencia,\n"
        "  max(case when variavel_id = 48 then valor end) as custo_medio_m2\n"
        "from base group by periodo"
    )
    schema = {"silver_x": {"periodo": "TEXT", "valor": "TEXT", "variavel_id": "TEXT"}}
    assert lin._origens("data_referencia", sql, schema, "gold_x") == {
        ("silver_x", "periodo")
    }
    assert lin._origens("custo_medio_m2", sql, schema, "gold_x") == {
        ("silver_x", "valor"),
        ("silver_x", "variavel_id"),
    }


def test_linhagem_de_coluna_nao_liga_o_model_a_si_mesmo() -> None:
    import linhagem_colunas as lin

    sql = "select a from gold_x"
    assert lin._origens("a", sql, {"gold_x": {"a": "TEXT"}}, "gold_x") == set()


def test_ref_do_dbt_vira_nome_de_model_e_o_resto_do_jinja_sai() -> None:
    import linhagem_colunas as lin

    limpo = lin.sql_analisavel(
        "{{ config(materialized='table') }}\nselect * from {{ ref('silver_y') }}"
    )
    assert "silver_y" in limpo
    assert "{{" not in limpo and "config" not in limpo


def test_agrupamento_junta_colunas_na_mesma_aresta() -> None:
    """Uma aresta carrega todas as colunas que a atravessam."""
    import linhagem_colunas as lin

    vinculos = [
        lin.Vinculo("gold_x", "a", {("silver_y", "p")}),
        lin.Vinculo("gold_x", "b", {("silver_y", "q")}),
    ]
    arestas = lin.agrupar_por_aresta(vinculos)
    assert list(arestas) == [("silver_y", "gold_x")]
    assert len(arestas[("silver_y", "gold_x")]) == 2


# ── serviço compartilhado (Airflow) ─────────────────────────────────────────
def test_airflow_e_declarado_compartilhado_e_sem_titularidade_do_mcid() -> None:
    """86 DAGs, 22 deste repositório. Marcar o serviço como nosso é falso."""
    orq = comum.carregar("servicos.yml")["orquestracao"]
    assert orq.get("compartilhado") is True
    assert "owner_key" not in orq and "domain" not in orq and "tags" not in orq
    # a titularidade desce para a DAG, onde é verdadeira
    assert orq["dags"]["owner_key"] == "mcid_data_engineering"
    assert orq["dags"]["domain"] == "MCid"


def test_limpeza_tira_so_a_marcacao_do_mcid() -> None:
    """Numa instância compartilhada não se apaga o que não se declarou."""
    import sincronizar_lake as lake

    class DonosFalsos:
        def resolver(self, chave):
            return [{"id": "nosso-time"}]

    atual = {
        "owners": [{"id": "nosso-time"}, {"id": "time-de-outro-orgao"}],
        "domains": [{"fullyQualifiedName": "MCid"}, {"fullyQualifiedName": "MinC"}],
        "tags": [{"tagFQN": "dbtTags.mcid"}, {"tagFQN": "PII.NonSensitive"}],
    }
    r = lake._limpar_marcacao_do_mcid(None, DonosFalsos(), atual)
    assert r["owners"] == [{"id": "time-de-outro-orgao"}]
    assert r["domains"] == [{"fullyQualifiedName": "MinC"}]
    assert r["tags"] == [{"tagFQN": "PII.NonSensitive"}]


# ── restrições vindas dos testes do dbt (HU-11) ─────────────────────────────
def test_unique_mais_not_null_vira_chave_primaria() -> None:
    """O dbt não tem `primary key`; tem teste. A chave se lê dos dois juntos."""
    import restricoes_dbt as r

    rest = r.Restricoes(unicas={"apf"}, nao_nulas={"apf", "mes"})
    assert rest.chave_primaria == ["apf"]
    # a chave vai em `tableConstraints`; na coluna ela é NOT_NULL, senão a
    # instância recusa a tabela por marcar a chave em dois lugares
    assert rest.restricao_da_coluna("apf") == "NOT_NULL"
    assert rest.restricao_da_coluna("mes") == "NOT_NULL"
    assert rest.restricao_da_coluna("outra") is None


def test_unique_sozinho_nao_e_chave() -> None:
    import restricoes_dbt as r

    rest = r.Restricoes(unicas={"codigo"}, nao_nulas=set())
    assert rest.chave_primaria == []
    assert rest.restricao_da_coluna("codigo") == "UNIQUE"


def test_chave_sobre_coluna_omitida_nao_e_publicada() -> None:
    """`fds_panorama_entidade` tem chave em `cnpj_eo`, que o catálogo omite.

    Declarar a chave sobre uma coluna que a tabela publicada não mostra faria o
    catálogo apontar para um campo que ele mesmo esconde.
    """
    import restricoes_dbt as r

    rest = r.Restricoes(unicas={"cnpj_eo"}, nao_nulas={"cnpj_eo"})
    assert r.constraints_da_tabela(rest, {"apf", "uf"}) == []
    assert r.constraints_da_tabela(rest, {"cnpj_eo"}) == [
        {"constraintType": "PRIMARY_KEY", "columns": ["cnpj_eo"]}
    ]


def test_coluna_publicada_carrega_a_restricao() -> None:
    import restricoes_dbt as r

    rest = r.Restricoes(unicas={"apf"}, nao_nulas={"apf"})
    coluna = estrutura.om_column(
        {"name": "apf", "data_type": "text", "description": "x"}, rest
    )
    assert coluna["constraint"] == "NOT_NULL"


def test_chave_nao_aparece_nos_dois_lugares() -> None:
    """A instância recusa a tabela inteira se a chave vier duplicada."""
    import restricoes_dbt as r

    rest = r.Restricoes(unicas={"apf"}, nao_nulas={"apf"})
    coluna = estrutura.om_column(
        {"name": "apf", "data_type": "text", "description": "x"}, rest
    )
    assert coluna["constraint"] != "PRIMARY_KEY"
    assert r.constraints_da_tabela(rest, {"apf"})[0]["constraintType"] == "PRIMARY_KEY"
