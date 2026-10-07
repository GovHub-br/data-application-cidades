"""Testes do módulo ``normalizacao_dominio`` (domínio: localização + status).

Cobre normalização de UF/município/código IBGE (com/sem acento, não-mapeado),
mapeamento de status, carregadores de referência/mapa e o pipeline
``normalizar_localizacao`` (sem descarte de linhas, com sinalização de
não-mapeado). Usa a referência oficial ``data/referencia_ibge.csv`` (quando
presente) e DataFrames sintéticos — sem banco, sem rede.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from classificacao.normalizacao_dominio import (
    carregar_mapa_status,
    carregar_referencia_ibge,
    detectar_colunas_status,
    normalizar_codigo_ibge,
    normalizar_localizacao,
    normalizar_municipio,
    normalizar_status,
    normalizar_uf,
)

PROJETO = Path(__file__).resolve().parents[1]
CAMINHO_REFERENCIA = PROJETO / "data" / "referencia_ibge.csv"
CAMINHO_MAPA = PROJETO / "data" / "mapa_status.csv"

REFERENCIA_DISPONIVEL = CAMINHO_REFERENCIA.exists()
MAPA_DISPONIVEL = CAMINHO_MAPA.exists()


# ──────────────────────────────────────────────────────────────────────
# normalizar_uf
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("SP", "SP"),
        ("sp", "SP"),
        ("São Paulo", "SP"),
        ("Sao Paulo - SP", "SP"),
        ("sao paulo - sp", "SP"),
        ("Minas Gerais", "MG"),
        ("DISTRITO FEDERAL", "DF"),
        ("RJ", "RJ"),
    ],
)
def test_normalizar_uf_mapeia(valor: str, esperado: str) -> None:
    assert normalizar_uf(valor) == esperado


@pytest.mark.parametrize(
    "valor",
    ["ZZ", "Brasil", "", "   ", "São Paulo - Capital", "XX - SP", None],
)
def test_normalizar_uf_nao_mapeado(valor: str | None) -> None:
    assert normalizar_uf(valor) is None


# ──────────────────────────────────────────────────────────────────────
# normalizar_municipio / normalizar_codigo_ibge (contra referência real)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.skipif(not REFERENCIA_DISPONIVEL, reason="referencia_ibge.csv ausente")
class TestMunicipioReferencia:
    def test_municipio_com_acento(self) -> None:
        assert normalizar_municipio("São Paulo", "SP") == "São Paulo"

    def test_municipio_sem_acento(self) -> None:
        assert normalizar_municipio("SAO PAULO", "SP") == "São Paulo"

    def test_municipio_remove_sufixo_uf(self) -> None:
        assert normalizar_municipio("Sao Paulo - SP") == "São Paulo"

    def test_municipio_unico_sem_uf(self) -> None:
        # "Brasiléia" existe em um único UF (AC)
        assert normalizar_municipio("BRASILEIA") == "Brasiléia"

    def test_municipio_ambiguo_sem_uf_retorna_none(self) -> None:
        # "Água Boa" existe em MG e MT — sem UF é ambíguo
        assert normalizar_municipio("Água Boa") is None

    def test_municipio_ambiguo_com_uf_desambigua(self) -> None:
        assert normalizar_municipio("Água Boa", "MT") == "Água Boa"

    def test_municipio_ambiguo_uf_sem_match_retorna_none(self) -> None:
        # Não há "Água Boa" em SP; com UF divergente permanece ambíguo
        assert normalizar_municipio("Água Boa", "SP") is None

    def test_municipio_nao_existente(self) -> None:
        assert normalizar_municipio("Cidade Inexistente XYZ") is None

    def test_municipio_nulo(self) -> None:
        assert normalizar_municipio(None) is None
        assert normalizar_municipio("") is None
        assert normalizar_municipio("   ") is None


@pytest.mark.skipif(not REFERENCIA_DISPONIVEL, reason="referencia_ibge.csv ausente")
class TestCodigoIbgeReferencia:
    def test_codigo_valido(self) -> None:
        assert normalizar_codigo_ibge("3550308") == "3550308"

    def test_codigo_inteiro(self) -> None:
        assert normalizar_codigo_ibge(3550308) == "3550308"

    def test_codigo_float_renderizado(self) -> None:
        assert normalizar_codigo_ibge("3550308.0") == "3550308"

    def test_codigo_menor_que_7_digitos_sem_match(self) -> None:
        # "410315" zero-pad -> "0410315", inexistente na referência
        assert normalizar_codigo_ibge("410315") is None

    def test_codigo_inexistente(self) -> None:
        assert normalizar_codigo_ibge("9999999") is None

    def test_codigo_invalido(self) -> None:
        assert normalizar_codigo_ibge("abc") is None
        assert normalizar_codigo_ibge("123456789") is None
        assert normalizar_codigo_ibge("") is None
        assert normalizar_codigo_ibge(None) is None


# ──────────────────────────────────────────────────────────────────────
# carregar_referencia_ibge
# ──────────────────────────────────────────────────────────────────────


def test_carregar_referencia_ibge_padrao() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    ref = carregar_referencia_ibge()
    assert ref is not None
    assert set(ref.columns) == {"codigo_ibge", "nome_municipio", "uf"}
    assert len(ref) == 5570
    assert ref["codigo_ibge"].str.len().eq(7).all()


def test_carregar_referencia_ibge_sintetica(tmp_path: Path) -> None:
    arquivo = tmp_path / "ref.csv"
    arquivo.write_text(
        "codigo_ibge,nome_municipio,uf\n"
        "3550308,São Paulo,SP\n"
        "5100201,Água Boa,MT\n"
        "5100201,Água Boa,MG\n"
        "123,Brasiléia,AC\n",
        encoding="utf-8",
    )
    ref = carregar_referencia_ibge(arquivo)
    assert ref is not None
    # "123" é zero-padded para 7 dígitos
    assert "0000123" in set(ref["codigo_ibge"])


def test_carregar_referencia_ibge_ausente(tmp_path: Path) -> None:
    assert carregar_referencia_ibge(tmp_path / "nao_existe.csv") is None


# ──────────────────────────────────────────────────────────────────────
# normalizar_localizacao (DataFrames sintéticos)
# ──────────────────────────────────────────────────────────────────────


def _df_localizacao() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uf": ["SP", "sp", "São Paulo", "XX", None],
            "municipio": ["São Paulo", "SAO PAULO", "Água Boa", "Água Boa", ""],
            "codigo_ibge": ["3550308", "3550308", "5100201", "9999999", None],
        }
    )


def test_normalizar_localizacao_nao_descarta_linhas() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    df = _df_localizacao()
    out = normalizar_localizacao(df)
    assert len(out) == len(df)
    # colunas originais preservadas
    assert "uf" in out.columns and "municipio" in out.columns
    assert "codigo_ibge" in out.columns


def test_normalizar_localizacao_colunas_canonicas() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    out = normalizar_localizacao(_df_localizacao())
    assert out["uf_canonico"].tolist() == ["SP", "SP", "SP", pd.NA, pd.NA]
    assert out["codigo_ibge_canonico"].tolist() == [
        "3550308",
        "3550308",
        "5100201",
        pd.NA,
        pd.NA,
    ]
    # município usa UF por linha: linha 2 (Água Boa, uf SP) é ambíguo e vira NA
    assert out["municipio_canonico"].tolist() == [
        "São Paulo",
        "São Paulo",
        pd.NA,
        pd.NA,
        pd.NA,
    ]


def test_normalizar_localizacao_sinaliza_nao_mapeado() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    out = normalizar_localizacao(_df_localizacao())
    assert "_localizacao_nao_mapeada" in out.columns
    # linhas 2 (município ambíguo) e 3 (UF + IBGE inválidos) são não-mapeadas;
    # linha 4 tem apenas valores vazios -> não é "não-mapeada"
    assert out["_localizacao_nao_mapeada"].tolist() == [False, False, True, True, False]

    cobertura = out.attrs["_localizacao_cobertura"]
    assert cobertura["n_linhas"] == 5
    assert cobertura["n_nao_mapeadas"] == 2
    assert set(cobertura["colunas_detectadas"]) == {"uf", "municipio", "codigo_ibge"}


def test_normalizar_localizacao_idempotente() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    out1 = normalizar_localizacao(_df_localizacao())
    out2 = normalizar_localizacao(out1)
    pd.testing.assert_frame_equal(out1, out2)


def test_normalizar_localizacao_sem_colunas_de_localizacao() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = normalizar_localizacao(df)
    # sem colunas de localização: nenhuma coluna canônica nem flag
    assert "_localizacao_nao_mapeada" not in out.columns
    assert "uf_canonico" not in out.columns
    assert out.attrs["_localizacao_cobertura"]["colunas_detectadas"] == {}


def test_normalizar_localizacao_municipio_sem_coluna_uf() -> None:
    if not REFERENCIA_DISPONIVEL:
        pytest.skip("referencia_ibge.csv ausente")
    df = pd.DataFrame(
        {"municipio": ["Brasiléia", "Água Boa", "SAO PAULO"], "x": [1, 2, 3]}
    )
    out = normalizar_localizacao(df)
    # sem UF: único match é aceito; ambíguo vira NA
    assert out["municipio_canonico"].tolist() == ["Brasiléia", pd.NA, "São Paulo"]


# ──────────────────────────────────────────────────────────────────────
# normalizar_status / carregar_mapa_status
# ──────────────────────────────────────────────────────────────────────


_MAPA_SINTETICO = {
    "paralisada": "paralisada",
    "parada": "paralisada",
    "em obras": "em_obras",
    "concluida": "concluida",
    "desconhecido": None,
}


def test_normalizar_status_mapeia_valores() -> None:
    serie = pd.Series(
        ["PARALISADA", "EM OBRAS", "Concluída", "CONCLUIDA", "desconhecido"]
    )
    out = normalizar_status(serie, _MAPA_SINTETICO)
    assert out.tolist() == [
        "paralisada",
        "em_obras",
        "concluida",
        "concluida",
        pd.NA,
    ]


def test_normalizar_status_preserva_nao_mapeado() -> None:
    serie = pd.Series(["CONCLUIDA_COM_VLR_A_LIBERAR", "OBRA FISICA CONCLUIDA"])
    out = normalizar_status(serie, _MAPA_SINTETICO)
    # não-destrutivo: valores fora do mapa são preservados
    assert out.tolist() == ["CONCLUIDA_COM_VLR_A_LIBERAR", "OBRA FISICA CONCLUIDA"]


def test_normalizar_status_nulos_vazios() -> None:
    serie = pd.Series(["", None, float("nan"), "parada"])
    out = normalizar_status(serie, _MAPA_SINTETICO)
    assert out.tolist() == [pd.NA, pd.NA, pd.NA, "paralisada"]


def test_carregar_mapa_status_padrao() -> None:
    if not MAPA_DISPONIVEL:
        pytest.skip("mapa_status.csv ausente")
    mapa = carregar_mapa_status()
    assert len(mapa) >= 15
    assert mapa.get("parada") == "paralisada"
    assert mapa.get("em obras") == "em_obras"
    assert mapa.get("concluída") == "concluida"
    # classe pendente -> None
    assert mapa.get("desconhecido") is None


def test_carregar_mapa_status_sintetico(tmp_path: Path) -> None:
    arquivo = tmp_path / "mapa.csv"
    arquivo.write_text(
        "valor_bruto,valor_canonico,classe\n"
        "paralisada,paralisada,valido\n"
        "em obras,em_obras,valido\n"
        "desconhecido,,pendente\n",
        encoding="utf-8",
    )
    mapa = carregar_mapa_status(arquivo)
    assert mapa["paralisada"] == "paralisada"
    assert mapa["em obras"] == "em_obras"
    assert mapa["desconhecido"] is None


def test_carregar_mapa_status_ausente(tmp_path: Path) -> None:
    assert carregar_mapa_status(tmp_path / "nao_existe.csv") == {}


def test_detectar_colunas_status() -> None:
    df = pd.DataFrame(
        {
            "situacao_obra": ["x"],
            "situacao_contrato": ["y"],
            "no_situacao_obra": ["z"],
            "municipio": ["a"],
            "status_ok": ["w"],
        }
    )
    assert detectar_colunas_status(df) == [
        "situacao_obra",
        "situacao_contrato",
        "no_situacao_obra",
        "status_ok",
    ]


def test_detectar_colunas_status_ignora_canonicas() -> None:
    df = pd.DataFrame({"situacao_obra": ["x"], "situacao_obra_canonico": ["y"]})
    assert detectar_colunas_status(df) == ["situacao_obra"]