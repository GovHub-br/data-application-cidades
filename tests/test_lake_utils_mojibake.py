import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from lake_utils import corrigir_mojibake_texto


def test_corrigir_mojibake_texto_reverte_utf8_duplicado() -> None:
    assert corrigir_mojibake_texto("SÃ£o Domingos do Norte") == "São Domingos do Norte"
    assert corrigir_mojibake_texto("José") == "José"
