"""Estratégia `email`: anexos do e-mail do dia, como chegaram (zip, csv...)."""

import json
import tempfile
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.imap.hooks.imap import ImapHook
from airflow.sdk import Connection, Variable

from ingestion.extractors.base_extractor import Extractor, RawFile, describe_file
from ingestion.extractors.config_extractor import ExtractorConfig, MailQuery
from ingestion.extractors.extractor_errors import SourceNotFoundError
from ingestion.extractors.extractor_registry import ExtractorFactory
from ingestion.layout import TIMEZONE

# O IMAP (RFC 3501) só aceita o mês abreviado em inglês; strftime("%b") segue o locale.
_MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


@ExtractorFactory.register("email")
class EmailAttachmentExtractor(Extractor):
    """Baixa os anexos do e-mail do dia da ingestão (em Brasília) pela Connection IMAP.

    O anexo é guardado como veio: um zip continua zip, e abri-lo é da conversão. Sem
    e-mail ou sem anexo que case com o padrão, SourceNotFoundError. O imaplib traz
    cada anexo inteiro para a memória antes de gravar; é do protocolo, e anexos de
    e-mail são limitados pela caixa. O SUBJECT do IMAP casa por trecho do assunto,
    como a busca de hoje (imap_tools); o padrão do anexo restringe o resto.
    """

    hook_class: Any = ImapHook

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if config.mail is None:
            raise ValueError("a estratégia email precisa da busca em config.mail")
        if config.mail.sender is None and config.mail.credentials_variable is None:
            raise ValueError("informe o sender do e-mail ou a credentials_variable")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        query: MailQuery = self.config.mail  # type: ignore[assignment]
        # Diretório novo a cada extração: sobra de outra execução no mesmo work_dir
        # não pode passar por anexo de hoje.
        work_dir.mkdir(parents=True, exist_ok=True)
        target = Path(tempfile.mkdtemp(prefix="attachments-", dir=work_dir))
        credentials = (
            read_json_variable(query.credentials_variable)
            if query.credentials_variable
            else None
        )
        sender = query.sender or (credentials or {}).get("sender_email", "")
        hook = self.hook_class(imap_conn_id=self.config.conn_id)
        if credentials:
            hook.get_connection = lambda _conn_id: _connection(
                self.config.conn_id, credentials
            )
        with hook as client:
            client.download_mail_attachments(
                name=query.attachment_pattern,
                local_output_directory=str(target),
                check_regex=True,
                mail_folder=query.folder,
                mail_filter=self._mail_filter(query.subject, sender),
                not_found_mode="ignore",
                overwrite=False,
            )
        attachments = sorted(path for path in target.iterdir() if path.is_file())
        if not attachments:
            raise SourceNotFoundError(
                f"nenhum anexo {query.attachment_pattern!r} no e-mail "
                f"{query.subject!r} de {sender} em {self._day_label()}"
            )
        for path in attachments:
            yield describe_file(path)

    def _mail_filter(self, subject: str, sender: str) -> str:
        # SINCE d BEFORE d+1 é o ON d da RFC 3501; servidores (o GreenMail, por
        # exemplo) que não casam o ON casam este par.
        day = self._day()
        return (
            f"(SINCE {_imap_date(day)} BEFORE {_imap_date(day + timedelta(days=1))} "
            f'FROM "{sender}" SUBJECT "{subject}")'
        )

    def _day(self) -> date:
        return self.ingestion_time.astimezone(TIMEZONE).date()

    def _day_label(self) -> str:
        return _imap_date(self._day())


def _imap_date(day: date) -> str:
    return f"{day.day:02d}-{_MONTHS[day.month - 1]}-{day.year}"


def read_json_variable(name: str) -> dict[str, str]:
    """Variable JSON do Airflow, lida em runtime (gravada como texto ou já objeto)."""
    value = Variable.get(name)
    parsed: dict[str, str] = json.loads(value) if isinstance(value, str) else value
    return parsed


def _connection(conn_id: str, credentials: dict[str, str]) -> Connection:
    """Connection IMAP montada a partir da Variable de credenciais (SSL, porta 993)."""
    return Connection(
        conn_id=conn_id,
        conn_type="imap",
        host=credentials["imap_server"],
        login=credentials["email"],
        password=credentials["password"],
    )
