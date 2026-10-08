"""Configuração dos extratores: só literais, declarados no topo da DAG.

Nenhum segredo entra aqui, só o nome da Connection (`conn_id`), resolvida pelo hook
dentro da task. Cada estratégia lê os campos de que precisa.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class HttpRequest:
    """Uma chamada HTTP; `name` vira o nome do arquivo na raw."""

    name: str
    endpoint: str
    method: str = "GET"
    params: Mapping[str, Any] = field(default_factory=dict)
    json: Any = None
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MailQuery:
    """E-mail da ingestão do dia: remetente, assunto e quais anexos guardar."""

    sender: str
    subject: str
    attachment_pattern: str = r".*"
    folder: str = "INBOX"


@dataclass(frozen=True)
class ExtractorConfig:
    """O que extrair e de onde. `source` escolhe a estratégia no ExtractorFactory.

    - `requests`: chamadas das estratégias HTTP (`api`, `http_file`).
    - `url_resolver`: descobre a URL do arquivo em tempo de execução (link que muda
      a cada edição); recebe o hook já conectado.
    - `adapter`: HTTPAdapter montado na sessão, para fontes com TLS fora do padrão.
    - `mail`: busca da estratégia `email`.
    """

    source: str
    conn_id: str
    requests: tuple[HttpRequest, ...] = ()
    url_resolver: Callable[[Any], str] | None = None
    adapter: Any = None
    mail: MailQuery | None = None
