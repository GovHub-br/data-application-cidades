"""Contrato de privacidade analítica do CadÚnico."""

import importlib.util
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
    path = scripts / "mascarar_minio.py"
    spec = importlib.util.spec_from_file_location("mascarar_minio_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_cadunico_preserva_codigos_analiticos_e_protege_identificadores():
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
    assert por_coluna["DT_NASC_PESSOA"] == "redact"
    assert por_coluna["ETNIA_DESCRICAO"] == "redact"
    assert "CO_RACA_COR_PESSOA" not in por_coluna
    assert "CO_DEFICIENCIA_MEMB" not in por_coluna
