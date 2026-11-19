"""O .mdb chega à staging mascarado, mesmo ficando com PII no raw/.

O mascaramento não reescreve Access (mdbtools é read-only); o raw_para_staging
passa o CSV exportado de cada tabela por `mascarar_csv()` antes do parquet. O
`CCI_CCA` do GEAVO chegou ao banco com data de nascimento e CEP reais por falta
disso.
"""

import csv
import importlib
from pathlib import Path

import pytest

from tests.conftest import fresh_import

SCRIPTS = Path(__file__).parents[1] / "scripts"


def _carregar_modulo(monkeypatch, segredo: str = "segredo-de-teste"):
    return fresh_import(monkeypatch, "mascaramento.tabular", MASKING_HMAC_SECRET=segredo)


def _escrever(path: Path, linhas: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        csv.writer(f, lineterminator="\n").writerows(linhas)


def _ler(path: Path) -> list[list[str]]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_cci_analitico_preserva_nascimento_e_mascara_cep(tmp_path, monkeypatch):
    modulo = _carregar_modulo(monkeypatch)
    origem, destino = tmp_path / "cci.csv", tmp_path / "cci.mascarado.csv"
    _escrever(
        origem,
        [
            [
                "AnoMesContratacao",
                "Nome_Programa",
                "DT_Nascimento",
                "IM_CEP",
                "VR_Financ",
            ],
            ["202601", "Minha Casa Minha Vida", "03/02/1985", "70000-000", "150000,00"],
            ["202602", "Minha Casa Minha Vida", "", "71000-000", "98000,00"],
        ],
    )

    alvos, total, alterados = modulo.mascarar_csv(str(origem), str(destino), ",", "utf-8")

    acoes = {a["column"]: a["action"] for a in alvos}
    # a data de nascimento fica, mas prova que o CEP é de pessoa física
    assert acoes == {"IM_CEP": "redact"}
    assert (total, alterados) == (2, 2)
    linhas = _ler(destino)
    assert linhas[0] == _ler(origem)[0]
    assert linhas[1] == [
        "202601",
        "Minha Casa Minha Vida",
        "03/02/1985",
        "***",
        "150000,00",
    ]
    assert linhas[2] == ["202602", "Minha Casa Minha Vida", "", "***", "98000,00"]


def test_cpf_recebe_o_mesmo_token_do_raw(tmp_path, monkeypatch):
    modulo = _carregar_modulo(monkeypatch)
    origem, destino = tmp_path / "pf.csv", tmp_path / "pf.mascarado.csv"
    _escrever(origem, [["NU_CPF_MUTUARIO", "UF"], ["123.456.789-09", "DF"]])

    modulo.mascarar_csv(str(origem), str(destino), ",", "utf-8")

    token = _ler(destino)[1][0]
    assert token == importlib.import_module("mascaramento.regras")._hmac_token("123.456.789-09")
    assert len(token) == 16 and "123" not in token


def test_sem_segredo_nao_grava_cpf(tmp_path, monkeypatch):
    modulo = _carregar_modulo(monkeypatch, segredo="")
    origem, destino = tmp_path / "pf.csv", tmp_path / "pf.mascarado.csv"
    _escrever(origem, [["NU_CPF_MUTUARIO"], ["12345678909"]])

    with pytest.raises(RuntimeError, match="MASKING_HMAC_SECRET"):
        modulo.mascarar_csv(str(origem), str(destino), ",", "utf-8")
    assert not destino.exists()


def test_tabela_sem_pii_passa_igual(tmp_path, monkeypatch):
    modulo = _carregar_modulo(monkeypatch)
    origem, destino = tmp_path / "obra.csv", tmp_path / "obra.mascarado.csv"
    _escrever(origem, [["APF", "Nome_Programa", "Pc_Obra"], ["123", "FAR", "45,5"]])

    alvos, _, _ = modulo.mascarar_csv(str(origem), str(destino), ",", "utf-8")

    assert alvos == []
    assert destino.read_bytes() == origem.read_bytes()


def test_raw_para_staging_mascara_o_mdb_antes_do_parquet():
    """A ordem importa: o parquet é escrito do CSV, então a máscara vem antes."""
    codigo = (SCRIPTS / "conversao" / "processamento.py").read_text(encoding="utf-8")
    ramo_mdb = codigo[codigo.index("if ext in SUPPORTED_MDB:") :]
    assert ramo_mdb.index("mascarar_csv(") < ramo_mdb.index("_converter_tabular(")
