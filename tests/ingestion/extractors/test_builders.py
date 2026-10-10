"""Construtores de extrator: a estratégia visível no DatasetSpec da DAG."""

from ingestion.extractors import (
    ExtractorConfig,
    HttpRequest,
    MailQuery,
    ObjectQuery,
    RemoteFiles,
    api,
    email,
    http_file,
    http_session,
    object_storage,
    sftp,
)

REQUEST = (HttpRequest(name="a", endpoint="/a"),)


def test_literal_arguments_give_the_extractor_config_itself() -> None:
    assert api(base_url="https://x", requests=REQUEST) == ExtractorConfig(
        source="api", base_url="https://x", requests=REQUEST
    )
    assert http_file(conn_id="fonte", requests=REQUEST) == ExtractorConfig(
        source="http_file", conn_id="fonte", requests=REQUEST
    )
    mail = MailQuery(subject="Relatório")
    assert email(mail=mail, conn_id="imap") == ExtractorConfig(
        source="email", mail=mail, conn_id="imap"
    )
    objects = ObjectQuery(prefix="raw/abecip/", pattern=".*")
    assert object_storage(objects=objects) == ExtractorConfig(
        source="object_storage", objects=objects
    )
    remote = RemoteFiles(root="/home", pattern="x")
    assert sftp(conn_id="sftp_mcid", remote=remote) == ExtractorConfig(
        source="sftp", conn_id="sftp_mcid", remote=remote
    )
    flow = object()
    assert http_session(flow=flow) == ExtractorConfig(source="http_session", session=flow)


def test_an_argument_that_depends_on_the_entry_is_resolved_with_it() -> None:
    build = api(
        base_url="https://x",
        requests=lambda entrada: [HttpRequest(name=entrada["dataset"], endpoint="/b")],
    )

    assert not isinstance(build, ExtractorConfig)
    assert build({"dataset": "sinapi"}) == ExtractorConfig(
        source="api",
        base_url="https://x",
        requests=(HttpRequest(name="sinapi", endpoint="/b"),),
    )
