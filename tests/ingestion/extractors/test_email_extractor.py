"""O que é próprio do extrator `email`, além do contrato."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractorConfig,
    ExtractorFactory,
    MailQuery,
    SourceNotFoundError,
)
from tests.ingestion.extractors.conftest import FakeImapHook

QUERY = MailQuery(
    sender="tesouro@exemplo.gov.br",
    subject="dotacao_execucao_outras_fontes_mcid",
    attachment_pattern=r".*\.zip$",
)


def _extract(tmp_path: Path, when: datetime, query: MailQuery = QUERY) -> list[str]:
    config = ExtractorConfig(source="email", conn_id="imap_tesouro", mail=query)
    extractor = ExtractorFactory.create(config, ingestion_time=when)
    return [part.name for part in extractor.extract(tmp_path)]


def test_searches_the_ingestion_day_in_brasilia_with_english_month(
    fake_imap: type[FakeImapHook], tmp_path: Path
) -> None:
    # 02h30 UTC do dia 9 ainda é dia 8 em Brasília; o IMAP só entende mês em inglês.
    # SINCE d BEFORE d+1 é o ON d da RFC 3501, com suporte mais amplo nos servidores.
    fake_imap.attachments = {"r.zip": b"PK"}

    _extract(tmp_path, datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc))

    [call] = fake_imap.calls
    assert call["conn_id"] == "imap_tesouro"
    assert call["mail_filter"] == (
        '(SINCE 08-Oct-2026 BEFORE 09-Oct-2026 FROM "tesouro@exemplo.gov.br" '
        'SUBJECT "dotacao_execucao_outras_fontes_mcid")'
    )
    assert call["mail_folder"] == "INBOX"
    assert call["check_regex"] is True


def test_keeps_only_attachments_matching_the_pattern(
    fake_imap: type[FakeImapHook], tmp_path: Path
) -> None:
    fake_imap.attachments = {"relatorio.zip": b"PK", "assinatura.png": b"\x89PNG"}

    assert _extract(tmp_path, datetime(2026, 10, 8, 12, tzinfo=timezone.utc)) == [
        "relatorio.zip"
    ]


def test_does_not_overwrite_attachments_with_the_same_name(
    fake_imap: type[FakeImapHook], tmp_path: Path
) -> None:
    # Dois e-mails no dia com "relatorio.zip": os dois precisam chegar à raw.
    fake_imap.attachments = {"relatorio.zip": b"PK"}

    _extract(tmp_path, datetime(2026, 10, 8, 12, tzinfo=timezone.utc))

    assert fake_imap.calls[0]["overwrite"] is False


def test_config_without_mail_query_is_rejected() -> None:
    with pytest.raises(ValueError, match="mail"):
        ExtractorFactory.create(
            ExtractorConfig(source="email", conn_id="imap_tesouro"),
            ingestion_time=datetime(2026, 10, 8, 12, tzinfo=timezone.utc),
        )


def test_day_window_crosses_month_and_year(
    fake_imap: type[FakeImapHook], tmp_path: Path
) -> None:
    fake_imap.attachments = {"r.zip": b"PK"}

    _extract(tmp_path, datetime(2026, 12, 31, 15, tzinfo=timezone.utc))

    assert fake_imap.calls[0]["mail_filter"].startswith(
        "(SINCE 31-Dec-2026 BEFORE 01-Jan-2027 "
    )


def test_leftovers_in_work_dir_are_not_taken_as_todays_attachments(
    fake_imap: type[FakeImapHook], tmp_path: Path
) -> None:
    fake_imap.attachments = {"relatorio.zip": b"PK"}
    _extract(tmp_path, datetime(2026, 10, 8, 12, tzinfo=timezone.utc))

    fake_imap.attachments = {}
    with pytest.raises(SourceNotFoundError):
        _extract(tmp_path, datetime(2026, 10, 9, 12, tzinfo=timezone.utc))


CREDENTIALS = {
    "imap_server": "imap.exemplo.gov.br",
    "email": "ingestao@exemplo.gov.br",
    "password": "segredo",
    "sender_email": "tesouro@exemplo.gov.br",
}


def test_credentials_and_sender_can_come_from_a_variable(
    fake_imap: type[FakeImapHook], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ingestion.extractors.models import email_extractor

    read: list[str] = []

    def fake_variable(name: str) -> dict[str, str]:
        read.append(name)
        return CREDENTIALS

    monkeypatch.setattr(email_extractor, "read_json_variable", fake_variable)
    fake_imap.attachments = {"r.zip": b"PK"}
    query = MailQuery(
        subject="dotacao_execucao_outras_fontes_mcid",
        attachment_pattern=r".*\.zip$",
        credentials_variable="email_credentials",
    )

    _extract(tmp_path, datetime(2026, 10, 8, 12, tzinfo=timezone.utc), query)

    [call] = fake_imap.calls
    connection = call["connection"]
    assert read == ["email_credentials"]
    assert (connection.conn_type, connection.host) == ("imap", "imap.exemplo.gov.br")
    assert (connection.login, connection.password) == (
        "ingestao@exemplo.gov.br",
        "segredo",
    )
    assert 'FROM "tesouro@exemplo.gov.br"' in call["mail_filter"]


def test_sender_is_required_somewhere(fake_imap: type[FakeImapHook]) -> None:
    with pytest.raises(ValueError, match="sender"):
        ExtractorFactory.create(
            ExtractorConfig(
                source="email", conn_id="imap_tesouro", mail=MailQuery(subject="x")
            ),
            ingestion_time=datetime(2026, 10, 8, 12, tzinfo=timezone.utc),
        )
