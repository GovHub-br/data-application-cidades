import pandas as pd
import pytest

from domain.exceptions import SchemaValidationError
from schema_validator import SchemaValidator


def test_schema_valido_nao_levanta_erro() -> None:
    df = pd.DataFrame({"id": ["1"], "nome": ["a"]})
    SchemaValidator(["id", "nome"]).validate(df)


def test_schema_com_coluna_extra_nao_levanta_erro() -> None:
    """Coluna a mais não é problema — só falta é."""
    df = pd.DataFrame({"id": ["1"], "nome": ["a"], "extra": ["x"]})
    SchemaValidator(["id", "nome"]).validate(df)


def test_schema_com_coluna_faltando_levanta_erro() -> None:
    df = pd.DataFrame({"id": ["1"]})
    with pytest.raises(SchemaValidationError, match="nome"):
        SchemaValidator(["id", "nome"]).validate(df)
