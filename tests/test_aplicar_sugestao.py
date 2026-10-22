"""Edição do YAML do dbt a partir de uma sugestão do OpenMetadata.

O que importa aqui é o diff: a sugestão troca as linhas do campo e nada mais,
nem comentário, nem indentação, nem a ordem das chaves.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts" / "governance"))

import aplicar_sugestao as ap  # noqa: E402
import corpo_pr_sugestao  # noqa: E402

SCHEMA = """\
version: 2

# comentário do arquivo
models:

  - name: ouro_a
    description: Texto antigo.
    # comentário sobre as colunas
    columns:
      - name: apf
        description: >
          APF antigo em bloco,
          com duas linhas.
        tests:
          - not_null
      - name: uf
        description: "Unidade da federação,
          entre aspas."

  - name: ouro_b
    description: Sem colunas.
    config:
      meta:
        documentacao:
          grao: Uma linha por UF.
"""

INDENTLESS = """\
version: 2
models:
- name: ouro_rural
  description: 'Texto em aspas simples
    quebrado.'
  columns:
  - name: apf
    description: APF.
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    raiz = tmp_path / "models"
    (raiz / "far" / "ouro").mkdir(parents=True)
    (raiz / "far" / "ouro" / "schema.yml").write_text(SCHEMA, encoding="utf-8")
    (raiz / "rural").mkdir()
    (raiz / "rural" / "schema.yml").write_text(INDENTLESS, encoding="utf-8")
    return raiz


def diff(antes: str, depois: str) -> tuple[list[str], list[str]]:
    a, d = antes.splitlines(), depois.splitlines()
    return [x for x in a if x not in d], [x for x in d if x not in a]


def test_troca_descricao_do_model_sem_tocar_no_resto(models: Path) -> None:
    arquivo = ap.aplicar("ouro_a", None, "Texto novo.", models)
    assert diff(SCHEMA, arquivo.read_text()) == (
        ["    description: Texto antigo."],
        ["    description: Texto novo."],
    )


def test_troca_descricao_em_bloco_de_coluna(models: Path) -> None:
    arquivo = ap.aplicar("ouro_a", "apf", "APF normalizado para 8 dígitos.", models)
    removidas, novas = diff(SCHEMA, arquivo.read_text())
    assert removidas == [
        "        description: >",
        "          APF antigo em bloco,",
        "          com duas linhas.",
    ]
    assert novas == ["        description: APF normalizado para 8 dígitos."]


def test_troca_descricao_entre_aspas_em_varias_linhas(models: Path) -> None:
    arquivo = ap.aplicar("ouro_a", "uf", "Sigla da UF.", models)
    assert "        description: Sigla da UF." in arquivo.read_text()
    assert "entre aspas." not in arquivo.read_text()


def test_comentario_depois_da_descricao_fica(models: Path) -> None:
    arquivo = ap.aplicar("ouro_a", None, "Outro.", models)
    assert "    # comentário sobre as colunas" in arquivo.read_text()


def test_texto_longo_ou_com_dois_pontos_vira_bloco_dobrado(models: Path) -> None:
    texto = "Valor contratado: soma das parcelas " + "liberadas no período " * 6
    arquivo = ap.aplicar("ouro_a", None, texto, models)
    conteudo = arquivo.read_text()
    assert "    description: >-" in conteudo
    assert all(len(linha) <= ap.LARGURA for linha in conteudo.splitlines())


def test_coluna_nova_entra_no_fim_da_lista(models: Path) -> None:
    arquivo = ap.aplicar("ouro_a", "municipio", "Município do empreendimento.", models)
    removidas, novas = diff(SCHEMA, arquivo.read_text())
    assert removidas == []
    assert novas == [
        "      - name: municipio",
        "        description: Município do empreendimento.",
    ]


def test_model_sem_columns_ganha_a_lista(models: Path) -> None:
    arquivo = ap.aplicar("ouro_b", "uf", "Sigla da UF.", models)
    assert "    columns:\n      - name: uf\n        description: Sigla da UF.\n" in (
        arquivo.read_text()
    )


def test_lista_sem_indentacao_estilo_rural(models: Path) -> None:
    arquivo = ap.aplicar("ouro_rural", "cpf_hash", "Hash do CPF.", models)
    removidas, novas = diff(INDENTLESS, arquivo.read_text())
    assert removidas == []
    assert novas == ["  - name: cpf_hash", "    description: Hash do CPF."]
    arquivo = ap.aplicar("ouro_rural", None, "Texto novo.", models)
    assert "  description: Texto novo.\n  columns:" in arquivo.read_text()


def test_model_sem_entrada_no_yaml_e_criado_ao_lado_do_sql(models: Path) -> None:
    (models / "far" / "ouro" / "ouro_c.sql").write_text("select 1", encoding="utf-8")
    arquivo = ap.aplicar("ouro_c", None, "Tabela nova.", models)
    assert arquivo == models / "far" / "ouro" / "schema.yml"
    assert arquivo.read_text().endswith(
        "\n  - name: ouro_c\n    description: Tabela nova.\n"
    )


def test_descricao_em_bloco_doc_edita_o_markdown(models: Path) -> None:
    schema = models / "far" / "ouro" / "schema.yml"
    schema.write_text(
        SCHEMA.replace(
            "description: Texto antigo.", "description: '{{ doc(\"ouro_a\") }}'"
        ),
        encoding="utf-8",
    )
    md = models / "far" / "docs.md"
    md.write_text("{% docs ouro_a %}\nAntigo.\n{% enddocs %}\n", encoding="utf-8")

    assert ap.aplicar("ouro_a", None, "Novo pelo bloco.", models) == md
    assert md.read_text() == "{% docs ouro_a %}\nNovo pelo bloco.\n{% enddocs %}\n"


def test_model_inexistente_e_recusado(models: Path) -> None:
    with pytest.raises(ap.SugestaoInvalida, match="não existe"):
        ap.aplicar("ouro_fantasma", None, "x", models)


def test_nome_da_tabela_pelo_fqn() -> None:
    assert ap.nome_da_tabela("Cidades.cidades.ouro.ouro_a") == "ouro_a"
    assert ap.nome_da_tabela('Cidades.cidades.ouro."tab.ela"') == "tab.ela"
    with pytest.raises(ap.SugestaoInvalida):
        ap.nome_da_tabela("Cidades.cidades.ouro")


def test_corpo_do_pr_cita_a_sugestao_sem_mencionar_ninguem() -> None:
    texto = corpo_pr_sugestao.corpo(
        {
            "task_id": 7,
            "fqn": "Cidades.cidades.ouro.ouro_a",
            "coluna": "apf",
            "sugestao": "Avise @time-dados.\nSegunda linha.",
            "autor": "especialista",
        },
        "dbt/mcid/models/far/ouro/schema.yml",
        "https://om.exemplo",
    )
    assert "> Avise @​time-dados.\n> Segunda linha." in texto
    assert "https://om.exemplo/table/Cidades.cidades.ouro.ouro_a" in texto
    assert "coluna `apf`" in texto
