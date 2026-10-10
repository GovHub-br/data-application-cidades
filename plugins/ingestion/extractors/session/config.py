"""Configuração de um fluxo `http_session`, declarada no topo da DAG."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class HttpSession:
    """Os passos do fluxo, as Variables que ele usa e os adapters da sessão.

    - `steps`: passos executados em ordem (`Request`, `Download`, `SetHeaders`,
      `DropHeaders`); ao menos um `Download`.
    - `variables`: nome no modelo → nome da Variable do Airflow, lida só dentro da
      task (`{"senha": "dados_fgv_password"}` permite `"{senha}"` nos passos).
    - `mounts`: prefixo de URL → `HTTPAdapter` (TLS legado de um host, por exemplo).
    """

    steps: Sequence[Any]
    variables: Mapping[str, str] = field(default_factory=dict)
    mounts: Mapping[str, Any] = field(default_factory=dict)
