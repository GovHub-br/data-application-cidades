"""Extrator `email` contra um servidor IMAP de verdade (GreenMail, em container).

Pulado sem `AIRFLOW_CONN_IMAP_TEST` (a Connection do IMAP de teste) e
`INGESTION_TEST_SMTP` (`host:porta` do SMTP do mesmo servidor, usado para semear
a caixa), ou se algum dos dois não responder.
"""

import os
import smtplib
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractorConfig,
    ExtractorFactory,
    MailQuery,
    SourceNotFoundError,
)
from tests.ingestion.conftest import reachable
from tests.ingestion.extractors.conftest import zip_of

pytestmark = pytest.mark.integration


@pytest.fixture
def smtp_address() -> tuple[str, int]:
    if not os.environ.get("AIRFLOW_CONN_IMAP_TEST"):
        pytest.skip("AIRFLOW_CONN_IMAP_TEST não definida")
    host, _, port = os.environ.get("INGESTION_TEST_SMTP", "").partition(":")
    if not host or not reachable(host, int(port or 25)):
        pytest.skip("INGESTION_TEST_SMTP não definida ou sem resposta")
    return host, int(port)


def test_downloads_todays_attachment_from_a_real_mailbox(
    smtp_address: tuple[str, int], tmp_path: Path
) -> None:
    subject = f"dotacao_execucao_{uuid.uuid4().hex[:8]}"
    report = zip_of({"dotacao.csv": "a\tb\n1\t2\n".encode("utf-16")})
    message = EmailMessage()
    message["From"] = "tesouro@exemplo.gov.br"
    message["To"] = "ingestao@localhost"
    message["Subject"] = subject
    message.set_content("relatório do dia")
    message.add_attachment(
        report, maintype="application", subtype="zip", filename="dotacao.zip"
    )
    with smtplib.SMTP(*smtp_address) as smtp:
        smtp.send_message(message)

    def extract(subject: str) -> list[tuple[str, bytes]]:
        config = ExtractorConfig(
            source="email",
            conn_id="imap_test",
            mail=MailQuery(
                sender="tesouro@exemplo.gov.br",
                subject=subject,
                attachment_pattern=r".*\.zip$",
            ),
        )
        extractor = ExtractorFactory.create(
            config, ingestion_time=datetime.now(timezone.utc)
        )
        return [(p.name, p.path.read_bytes()) for p in extractor.extract(tmp_path)]

    assert extract(subject) == [("dotacao.zip", report)]
    with pytest.raises(SourceNotFoundError):
        extract(f"{subject}_inexistente")
