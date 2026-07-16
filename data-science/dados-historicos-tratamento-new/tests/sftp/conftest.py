"""Configuração de path para testes do pacote sftp."""

import sys
from pathlib import Path

# Adicionar src/ ao path para permitir import do pacote sftp
_src_dir = Path(__file__).resolve().parent.parent.parent / "src"
_src_str = str(_src_dir)

# Garantir que src/ está ANTES de tests/ no sys.path (pytest pode inverter a ordem)
if _src_str in sys.path:
    sys.path.remove(_src_str)
sys.path.insert(0, _src_str)
