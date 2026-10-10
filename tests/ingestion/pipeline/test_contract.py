"""Contrato dos passos do pipeline.

As tasks só trocam caminhos (XCom pequeno): cada passo recebe e devolve string com o
prefixo de uma partição, nunca dado. Os testes de comportamento estão em
test_steps.py.
"""

import inspect

from ingestion.pipeline import steps


def test_steps_exchange_only_prefixes() -> None:
    extract = inspect.signature(steps.extract_to_raw)
    convert = inspect.signature(steps.convert_to_staging)

    # várias ingestões por execução (uma por entrega, na extração incremental)
    assert extract.return_annotation in (list[str], "list[str]")
    assert list(convert.parameters)[1] == "raw_prefixes"
    assert convert.return_annotation in (str, "str")
