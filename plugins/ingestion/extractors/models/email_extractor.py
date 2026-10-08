"""Estratégia `email`: anexos do e-mail do dia, como chegaram (zip, csv...)."""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.providers.imap.hooks.imap import ImapHook

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
    e-mail são limitados pela caixa.
    """

    hook_class: Any = ImapHook

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if config.mail is None:
            raise ValueError("a estratégia email precisa da busca em config.mail")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        query: MailQuery = self.config.mail  # type: ignore[assignment]
        target = work_dir / "attachments"
        target.mkdir(parents=True, exist_ok=True)
        with self.hook_class(imap_conn_id=self.config.conn_id) as hook:
            hook.download_mail_attachments(
                name=query.attachment_pattern,
                local_output_directory=str(target),
                check_regex=True,
                mail_folder=query.folder,
                mail_filter=self._mail_filter(query),
                not_found_mode="ignore",
                overwrite=False,
            )
        attachments = sorted(path for path in target.iterdir() if path.is_file())
        if not attachments:
            raise SourceNotFoundError(
                f"nenhum anexo {query.attachment_pattern!r} no e-mail "
                f"{query.subject!r} de {query.sender} em {self._day_label()}"
            )
        for path in attachments:
            yield describe_file(path)

    def _mail_filter(self, query: MailQuery) -> str:
        return f'(ON {self._day_label()} FROM "{query.sender}" SUBJECT "{query.subject}")'

    def _day_label(self) -> str:
        day = self.ingestion_time.astimezone(TIMEZONE).date()
        return f"{day.day:02d}-{_MONTHS[day.month - 1]}-{day.year}"
