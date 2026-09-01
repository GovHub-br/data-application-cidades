"""Encoding do lake: detecção e reparo de mojibake.

Contexto: um único byte inválido no meio de um arquivo utf-8 fazia o
detectar_encoding cair para cp1252/latin-1, e aí TODO acento do arquivo — cabeçalho
incluído — virava mojibake na staging. 739 arquivos do lake foram afetados.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from lake_utils import (  # noqa: E402
    corrigir_mojibake_texto,
    detectar_encoding,
    norm_header,
)


def test_corrigir_mojibake_texto_reverte_utf8_duplicado() -> None:
    assert corrigir_mojibake_texto("SÃ£o Domingos do Norte") == "São Domingos do Norte"
    assert corrigir_mojibake_texto("José") == "José"


def test_corrigir_mojibake_texto_e_conservador() -> None:
    # sem marcador de mojibake, devolve igual
    assert corrigir_mojibake_texto("ASCII puro") == "ASCII puro"
    assert corrigir_mojibake_texto("") == ""
    # com marcador mas sem round-trip válido, não adivinha
    assert corrigir_mojibake_texto("Ã") == "Ã"


def test_norm_header_repara_antes_de_tirar_acento() -> None:
    # sem o reparo, "MunicÃ­pio" viraria "municapio" (o bug real na bronze do BB)
    assert norm_header("MunicÃ­pio") == "municipio"
    assert norm_header("CÃ³digo IBGE do MunicÃ­pio") == "codigo_ibge_do_municipio"
    assert norm_header("SituaÃ§Ã£o do Empreendimento") == "situacao_do_empreendimento"
    assert norm_header("ObservaÃ§Ãµes") == "observacoes"
    # header já correto continua funcionando
    assert norm_header("Município") == "municipio"
    assert norm_header("% Exec") == "exec"


def test_detectar_encoding_utf8_com_byte_invalido_continua_utf8() -> None:
    # o caso que quebrou o lake: utf-8 legítimo com um byte solto no meio
    sujo = (
        "Município;Situação\nSão Paulo;Conclu".encode("utf-8")
        + b"\x92"
        + "do\nBrasília;Obras\n".encode("utf-8")
    )
    assert detectar_encoding(sujo) == "utf-8"


def test_detectar_encoding_nao_regride_nos_outros_casos() -> None:
    assert detectar_encoding("Município;São Paulo\n".encode("utf-8")) == "utf-8"
    assert detectar_encoding("Município;São Paulo\n".encode("cp1252")) == "cp1252"
    assert detectar_encoding(b"a;b\n1;2\n") == "cp1252"
    # byte indefinido em cp1252, no meio do sample
    assert detectar_encoding(b"Municipio\nSao\x81Paulo;Obras\nRio;X\n") == "latin-1"
    # aspas curvas do Windows são cp1252 válido, não utf-8
    assert detectar_encoding(b"Municipio\nSao Paulo;\x93Obras\x94\nRio;X\n") == "cp1252"
