"""Contrato de privacidade analítica do CadÚnico."""

import importlib
import os
import sys
from pathlib import Path


def _carregar_modulo():
    defaults = {
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": "teste",
        "MINIO_SECRET_KEY": "teste",
        "MINIO_BUCKET": "teste",
        "DB_DW_HOST_MCID": "postgres",
        "DB_DW_USER_MCID": "teste",
        "DB_DW_PASSWORD_MCID": "teste",
        "DB_DW_DBNAME_MCID": "teste",
        "MASKING_HMAC_SECRET": "segredo-de-teste",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)

    scripts = Path(__file__).parents[1] / "scripts"
    sys.path.insert(0, str(scripts))
    # o pacote lê as variáveis de ambiente no import: cada teste parte de um import novo
    for nome in [m for m in sys.modules if m.split(".")[0] == "mascaramento"]:
        del sys.modules[nome]
    return importlib.import_module("mascaramento.regras")


def test_cadunico_preserva_perfil_e_protege_identificadores():
    modulo = _carregar_modulo()
    header = [
        "NU_CPF_PESSOA",
        "NU_NIS_PESSOA",
        "NO_PESSOA",
        "NO_APELIDO_PESSOA",
        "DT_NASC_PESSOA",
        "CO_RACA_COR_PESSOA",
        "CO_DEFICIENCIA_MEMB",
        "ETNIA_DESCRICAO",
    ]

    targets, has_pf = modulo.classificar(header, None)
    por_coluna = {target["column"]: target["action"] for target in targets}

    assert has_pf is True
    assert por_coluna["NU_CPF_PESSOA"] == "hmac"
    assert por_coluna["NU_NIS_PESSOA"] == "hmac"
    assert por_coluna["NO_PESSOA"] == "redact"
    assert por_coluna["NO_APELIDO_PESSOA"] == "redact"
    assert "DT_NASC_PESSOA" not in por_coluna
    assert "ETNIA_DESCRICAO" not in por_coluna
    assert "CO_RACA_COR_PESSOA" not in por_coluna
    assert "CO_DEFICIENCIA_MEMB" not in por_coluna


def test_perfil_sem_identificador_ainda_protege_endereco():
    modulo = _carregar_modulo()
    header = ["NO_MUTUARIO", "DT_NASCIMENTO", "IC_BENEF_PCD", "NO_LOGRADOURO", "CO_CEP"]

    targets, has_pf = modulo.classificar(header, None)
    por_coluna = {target["column"]: target["action"] for target in targets}

    assert has_pf is True
    assert por_coluna == {
        "NO_MUTUARIO": "redact",
        "NO_LOGRADOURO": "redact",
        "CO_CEP": "redact",
    }


def test_situacao_de_rua_nao_e_endereco():
    modulo = _carregar_modulo()
    header = [
        "NU_CPF_PESSOA",
        "IN_DORMIR_RUA_MEMB",
        "QT_DORMIR_FREQ_RUA_MEMB",
        "CO_TEMPO_RUA_MEMB",
        "NO_LOGRADOURO_FAM",
    ]

    targets, _ = modulo.classificar(header, None)
    por_coluna = {target["column"]: target["action"] for target in targets}

    assert por_coluna == {"NU_CPF_PESSOA": "hmac", "NO_LOGRADOURO_FAM": "redact"}


def test_cpf_tem_o_mesmo_token_em_qualquer_formato():
    modulo = _carregar_modulo()
    token = modulo._hmac_token("1234567890")

    assert modulo._hmac_token("01234567890") == token
    assert modulo._hmac_token("012.345.678-90") == token
    assert modulo._hmac_token(" 1234567890 ") == token
    assert modulo._hmac_token("NAO INFORMADO") != token


def test_documentos_do_cadunico_sao_redigidos():
    modulo = _carregar_modulo()
    header = [
        "NU_IDENTIDADE_PESSOA",
        "NU_TITULO_ELEITOR_PESSOA",
        "NU_ZONA_TIT_ELEITOR_PESSOA",
        "NU_CART_TRAB_PREV_SOC_PESSOA",
        "NU_SERIE_TRAB_PREV_SOC_PESSOA",
        "DT_EMISSAO_CART_TRAB_PESSOA",
        "CO_MATRICULA_CERTIDAO",
        "CO_LIVRO_CERTIDAO",
        "CO_FOLHA_CERTIDAO",
        "CO_CERTIDAO_CIVIL_PESSOA",
        "IC_CERTIDAO_MATRICULA",
    ]

    targets, has_pf = modulo.classificar(header, None)
    redigidas = {t["column"] for t in targets if t["action"] == "redact"}

    assert has_pf is True
    assert redigidas == {
        "NU_IDENTIDADE_PESSOA",
        "NU_TITULO_ELEITOR_PESSOA",
        "NU_CART_TRAB_PREV_SOC_PESSOA",
        "NU_SERIE_TRAB_PREV_SOC_PESSOA",
        "CO_MATRICULA_CERTIDAO",
        "CO_LIVRO_CERTIDAO",
        "CO_FOLHA_CERTIDAO",
    }
