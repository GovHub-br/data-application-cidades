"""Contrato de privacidade analítica do CadÚnico."""

import pytest

from tests.conftest import fresh_import


def _carregar_modulo():
    # the package copies the env at import; nothing else reads it, so it can be undone
    with pytest.MonkeyPatch.context() as mp:
        return fresh_import(mp, "mascaramento.regras", MASKING_HMAC_SECRET="segredo-de-teste")


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


def test_familia_protege_entrevistador_e_texto_livre():
    modulo = _carregar_modulo()
    header = [
        "CO_FAMILIAR_FAM",
        "NO_LOCALIDADE_FAM",
        "NO_LOGRADOURO_FAM",
        "NU_LOGRADOURO_FAM",
        "DS_COMPLEMENTO_FAM",
        "NU_CEP_LOGRADOURO_FAM",
        "NO_UNIDADE_TERRITORIAL_FAM",
        "DS_REFERENCIA_LOCAL_FAM",
        "NO_ENTREVISTADOR_FAM",
        "NU_CPF_ENTREVISTADOR_FAM",
        "DS_OBS_ENTREVISTADOR_FAM",
        "NO_COMUNIDADE_QUILOMBOLA_FAM",
        "VL_DESP_ENERGIA_FAM",
        "NO_ESTAB_ASSIST_SAUDE_FAM",
        "NO_CENTRO_ASSIST_FAM",
    ]

    targets, has_pf = modulo.classificar(header, None)
    por_coluna = {target["column"]: target["action"] for target in targets}

    assert has_pf is True
    # localidade, comunidade, posto de saúde e CRAS não identificam a família
    assert por_coluna == {
        "NO_LOGRADOURO_FAM": "redact",
        "NU_LOGRADOURO_FAM": "redact",
        "DS_COMPLEMENTO_FAM": "redact",
        "NU_CEP_LOGRADOURO_FAM": "redact",
        "DS_REFERENCIA_LOCAL_FAM": "redact",
        "NO_ENTREVISTADOR_FAM": "redact",
        "NU_CPF_ENTREVISTADOR_FAM": "hmac",
        "DS_OBS_ENTREVISTADOR_FAM": "redact",
    }


def test_observacao_sem_pessoa_fisica_fica():
    modulo = _carregar_modulo()
    header = ["NO_EMPREENDIMENTO", "DS_OBSERVACAO", "OBS", "VR_CONTRATO"]

    targets, has_pf = modulo.classificar(header, None)

    assert (targets, has_pf) == ([], False)


def test_referencia_de_data_nao_e_texto_livre():
    modulo = _carregar_modulo()
    header = ["NU_CPF", "MES_REFERENCIA", "DT_REFERENCIA", "ANO_REFERENCIA", "OBS"]

    targets, _ = modulo.classificar(header, None)
    por_coluna = {target["column"]: target["action"] for target in targets}

    assert por_coluna == {"NU_CPF": "hmac", "OBS": "redact"}
