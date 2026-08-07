"""Testes do documentador de bases brutas.

O foco é a classificação de dado pessoal: o `bronze.yml` é versionado e
alimenta RAG, então um falso negativo aqui publica dado de cidadão. O falso
positivo também custa — tira do RAG justamente as colunas categóricas que
ajudam a desambiguar.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "airflow_lappis" / "plugins")
)

from documentador_bronze import (  # noqa: E402
    Coluna,
    Tabela,
    agrupar_por_familia,
    carregar_descricoes_dbt,
    coluna_sensivel,
    contar_linhas,
    familia_de,
    montar_documento,
    nome_sensivel,
    representante_da_familia,
    tabela_de_json,
    tabela_para_json,
)


@pytest.mark.parametrize(
    "nome",
    [
        "cpf",
        "nu_cpf",
        "cpf_beneficiario",
        "nis",
        "pis",
        "rg",
        "titulo_eleitor",
        "nome",
        "nome_completo",
        "nome_mae",
        "sobrenome",
        "email",
        "e_mail_pessoal",
        "telefone",
        "celular_contato",
        "dt_nascimento",
        "data_nascimento",
        "endereco",
        "logradouro",
        "cep",
        "senha",
        "password",
        "token",
        "conta_corrente",
        "agencia",
        "cartao",
    ],
)
def test_marca_dado_pessoal(nome: str) -> None:
    assert nome_sensivel(nome), f"{nome} deveria ser tratada como dado pessoal"


@pytest.mark.parametrize(
    "nome",
    [
        # Lugar e instituição não são pessoa — e é aqui que o exemplo mais ajuda o RAG.
        "municipio_nome",
        "uf_nome",
        "regiao_nome",
        "mesorregiao_nome",
        "microrregiao_nome",
        "localidade_nome",
        "pais_nome",
        "nome_empreendimento",
        "empresa_nome",
        "construtora_nome",
        "orgao_nome",
        "programa_nome",
        "banco_nome",
        "situacao_nome",
        "tipo_nome",
        "endereco_empreendimento",
        "municipio_cep",
        # Pessoa jurídica é informação pública.
        "cnpj",
        "razao_social",
        # Colunas comuns do domínio.
        "apf",
        "valor_contratado",
        "quantidade_uh",
        "percentual_execucao_fisica",
    ],
)
def test_nao_marca_o_que_nao_e_pessoal(nome: str) -> None:
    assert not nome_sensivel(nome), f"{nome} não é dado pessoal"


@pytest.mark.parametrize(
    "valor",
    ["123.456.789-01", "12345678901", "fulano@exemplo.com.br", "12.345.678/0001-99"],
)
def test_valor_denuncia_coluna_de_nome_inocente(valor: str) -> None:
    """Nome genérico não salva: o formato do conteúdo também marca a coluna."""
    assert coluna_sensivel("identificador", [valor])


def test_valor_comum_nao_marca() -> None:
    assert not coluna_sensivel("uf_nome", ["SP", "MG", "BA"])
    assert not coluna_sensivel("situacao", ["Contratado", "Distratado"])


def test_familia_agrupa_snapshots_irmaos() -> None:
    """182 snapshots mensais viram uma entrada só, não 182 quase idênticas."""
    assert familia_de("caixa_af_gehis_andamento_obra_m17") == familia_de(
        "caixa_af_gehis_andamento_obra_m182"
    )
    assert familia_de("api_ibge_uf") != familia_de("api_ibge_municipios")


def test_agrupamento_preserva_membros() -> None:
    grupos = agrupar_por_familia(["obra_m1", "obra_m2", "obra_m10", "consolidado"])
    assert grupos["obra_m#"] == ["obra_m1", "obra_m2", "obra_m10"]
    assert grupos["consolidado"] == ["consolidado"]


@pytest.mark.parametrize(
    "tabela",
    ["api_ibge_uf", "api_ibge_regioes", "api_ibge_municipios", "tabela_de_orgaos"],
)
def test_tabela_de_referencia_libera_coluna_nome(tabela: str) -> None:
    """`nome` numa tabela de UF é nome de lugar — e é ótimo sinal pro RAG."""
    assert not nome_sensivel("nome", tabela)


@pytest.mark.parametrize(
    "tabela",
    [
        "beneficiarios",
        "proponentes",
        "cadastro_unico_pessoas",
        "mutuarios_far",
        "responsavel_familiar",
        "socios_empresa",
    ],
)
def test_tabela_de_pessoas_anula_o_contexto(tabela: str) -> None:
    """Nem `municipio_nome` de tabela de gente escapa: o risco não compensa."""
    assert nome_sensivel("nome", tabela)
    assert nome_sensivel("endereco", tabela)


def test_tabela_de_pessoa_vence_contexto_de_lugar() -> None:
    """`empreendimento` no nome não libera uma tabela de beneficiários."""
    assert nome_sensivel("nome", "beneficiarios_do_empreendimento")


def test_sem_contexto_de_tabela_mantem_o_comportamento_anterior() -> None:
    assert nome_sensivel("nome")
    assert not nome_sensivel("municipio_nome")


def test_representante_e_a_mais_recente_da_familia() -> None:
    """Ordem alfabética colocaria `_m2` depois de `_m12`; o sufixo é número."""
    assert representante_da_familia(["obra_m1", "obra_m2", "obra_m12"]) == "obra_m12"
    assert representante_da_familia(["x_m9", "x_m10"]) == "x_m10"
    assert representante_da_familia(["consolidado"]) == "consolidado"


def test_contagem_exata_quando_amostra_nao_encheu() -> None:
    """Amostra menor que o limite significa que varreu tudo — contagem é exata.

    Evita depender de `reltuples`, que vale -1 em tabela sem ANALYZE.
    """
    assert contar_linhas(None, "s", "t", amostradas=500, limite=10_000) == 500
    assert contar_linhas(None, "s", "t", amostradas=0, limite=10_000) == 0


def _tabela(nome: str = "tabela_teste") -> Tabela:
    return Tabela(
        schema="ibge",
        nome=nome,
        familia=familia_de(nome),
        membros=[nome],
        linhas_estimadas=100,
        linhas_amostradas=100,
        comentario=None,
        colunas=[
            Coluna(
                nome="uf_nome",
                tipo="text",
                nullable=True,
                estatisticas={"amostra": 100, "distintos": 27, "exemplos": ["SP"]},
            ),
            Coluna(
                nome="cpf",
                tipo="text",
                nullable=True,
                sensivel=True,
                estatisticas={"amostra": 100, "distintos": 100},
            ),
        ],
    )


def test_serializacao_preserva_colunas() -> None:
    """O parcial trafega entre tasks do Airflow — não pode achatar as colunas."""
    original = _tabela()
    voltou = tabela_de_json(tabela_para_json(original))
    assert voltou == original
    assert isinstance(voltou.colunas[0], Coluna)
    assert voltou.impressao() == original.impressao()


def test_impressao_muda_quando_estrutura_muda() -> None:
    """A impressão é o que permite pular tabela inalterada na execução seguinte."""
    antes = _tabela()
    depois = _tabela()
    depois.colunas.append(Coluna(nome="nova", tipo="text", nullable=True))
    assert antes.impressao() != depois.impressao()


def test_documento_nunca_publica_valor_de_coluna_sensivel() -> None:
    doc = montar_documento({"ibge": [_tabela()]}, {})
    colunas = doc["sources"][0]["tables"][0]["columns"]
    por_nome = {c["name"]: c for c in colunas}
    assert "exemplos" in por_nome["uf_nome"]["meta"]
    assert "exemplos" not in por_nome["cpf"]["meta"]
    assert por_nome["cpf"]["meta"]["sensivel"] is True


def test_documento_reaproveita_descricao_escrita_a_mao() -> None:
    doc = montar_documento(
        {"ibge": [_tabela()]},
        {
            "ibge.tabela_teste": "Descrição curada pela equipe.",
            "tabela_teste.uf_nome": "Sigla da unidade federativa.",
        },
    )
    tabela = doc["sources"][0]["tables"][0]
    assert tabela["description"] == "Descrição curada pela equipe."
    coluna = {c["name"]: c for c in tabela["columns"]}["uf_nome"]
    assert coluna["description"] == "Sigla da unidade federativa."


def test_carrega_descricoes_dos_yamls_do_dbt(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "sources.yml").write_text(
        """
version: 2
sources:
  - name: raw
    schema: __dados_brutos
    tables:
      - name: consolidado
        description: Dados do GFAR consolidado.
        columns:
          - name: apf
            description: APF normalizado para 8 dígitos.
""",
        encoding="utf-8",
    )
    indice = carregar_descricoes_dbt(tmp_path)
    assert indice["__dados_brutos.consolidado"] == "Dados do GFAR consolidado."
    assert indice["consolidado.apf"] == "APF normalizado para 8 dígitos."


def test_ignora_artefatos_do_dbt(tmp_path: Path) -> None:
    """target/ tem manifest gerado; indexar de lá traria lixo duplicado."""
    alvo = tmp_path / "target"
    alvo.mkdir()
    (alvo / "sources.yml").write_text(
        "version: 2\nsources:\n  - name: x\n    tables:\n"
        "      - name: t\n        description: nao deve entrar\n",
        encoding="utf-8",
    )
    assert carregar_descricoes_dbt(tmp_path) == {}
