"""The new SFTP ingestion (plugins/datalake): extract, transform and report, with fakes."""

import io
import re
import stat
import zipfile
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from datalake import contrato
from tests.conftest import fresh_import

LOAD = "20261111T050000Z"
CPF = "52998224725"


class FakeSftp:
    def __init__(self, files: dict):
        self.files, self.downloads = files, []

    def list_directory_with_attr(self, folder):
        names = {
            p[len(folder) + 1 :].split("/")[0] for p in self.files if p.startswith(folder + "/")
        }
        out = []
        for name in names:
            path = f"{folder}/{name}"
            if path in self.files:
                out.append(
                    SimpleNamespace(
                        filename=name,
                        st_mode=stat.S_IFREG,
                        st_size=len(self.files[path]),
                        st_mtime=100,
                    )
                )
            else:
                out.append(
                    SimpleNamespace(filename=name, st_mode=stat.S_IFDIR, st_size=0, st_mtime=0)
                )
        return out

    def retrieve_file(self, path, local):
        self.downloads.append(path)
        with open(local, "wb") as f:
            f.write(self.files[path])


class FakeControl:
    def __init__(self, events, seen=(), hashes=()):
        self.events, self.seen, self.known = events, set(seen), set(hashes)
        self.rows = []

    def ingested(self):
        return self.seen

    def hashes(self):
        return self.known

    def ingest(self, path, key, size, mtime, file_hash, status, error=None):
        self.rows.append((path, key, status))

    def mask(self, key, rec, source_hash, landed_hash):
        self.events.append(("mask", key, rec["status"]))


@pytest.fixture
def sftp_mod(monkeypatch):
    return fresh_import(
        monkeypatch, "datalake.sftp", "datalake", "mascaramento", MASKING_HMAC_SECRET="s"
    )


def run(mod, files, **control):
    from lake_utils import md5_arquivo

    events, landed = [], {}

    def upload(path, key):
        events.append(("upload", key))
        with open(path, "rb") as f:
            landed[key] = f.read()

    ctl = FakeControl(events, **control)
    sftp = FakeSftp(files)
    result = mod.extract(sftp, upload, ctl, LOAD, md5_arquivo, folders=("/home/fabrica/GEFUS",))
    return result, landed, events, ctl, sftp


def test_masks_before_upload_and_the_original_never_lands(sftp_mod):
    csv = f"CPF;NOME\n{CPF};Ana\n".encode()
    result, landed, events, ctl, _ = run(sftp_mod, {"/home/fabrica/GEFUS/X.csv": csv})
    key = f"raw/sftp/fabrica/GEFUS/load_id={LOAD}/X.csv"
    assert result["keys"] == [key] and not result["errors"]
    assert events == [("mask", key, "masked"), ("upload", key)]
    assert CPF.encode() not in landed[key]
    assert ctl.rows == [("/home/fabrica/GEFUS/X.csv", key, "success")]


def test_a_file_that_cannot_be_masked_does_not_land(sftp_mod):
    result, landed, _, ctl, _ = run(sftp_mod, {"/home/fabrica/GEFUS/X.txt": b"sem colunas\n"})
    assert not landed and result["errors"] and "header" in result["errors"][0]
    assert ctl.rows[0][2] == "error"


def test_xls_seen_and_duplicates_are_not_landed(sftp_mod):
    files = {
        "/home/fabrica/GEFUS/velho.xls": b"x",
        "/home/fabrica/GEFUS/visto.csv": b"A\n1\n",
        "/home/fabrica/GEFUS/copia.csv": b"A\n2\n",
    }
    import hashlib

    dup = hashlib.md5(b"A\n2\n").hexdigest()
    result, landed, _, _, sftp = run(
        sftp_mod, files, seen={("/home/fabrica/GEFUS/visto.csv", 100)}, hashes={dup}
    )
    assert not landed
    assert result["unsupported"] == ["/home/fabrica/GEFUS/velho.xls"]
    assert result["duplicates"] == ["/home/fabrica/GEFUS/copia.csv"]
    assert sftp.downloads == ["/home/fabrica/GEFUS/copia.csv"]  # .xls and seen: no download


def test_zip_members_land_masked_below_the_zip(sftp_mod):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("dentro/A.csv", f"CPF;NOME\n{CPF};Ana\n")
        z.writestr("leia.pdf", b"%PDF")
    result, landed, _, _, _ = run(sftp_mod, {"/home/fabrica/GEFUS/P.zip": buf.getvalue()})
    key = f"raw/sftp/fabrica/GEFUS/load_id={LOAD}/P.zip/dentro/A.csv"
    assert result["keys"] == [key] and CPF.encode() not in landed[key]


def test_a_missing_folder_is_an_error_and_the_others_go_on(sftp_mod):
    from lake_utils import md5_arquivo

    sftp = FakeSftp({"/home/fabrica/GEFUS/X.csv": b"A;B\n1;2\n"})
    real = sftp.list_directory_with_attr

    def listing(folder):
        if folder == "/home/sumiu":
            raise FileNotFoundError(2, "No such file")
        return real(folder)

    sftp.list_directory_with_attr = listing
    ctl = FakeControl([])
    folders = ("/home/sumiu", "/home/fabrica/GEFUS")
    result = sftp_mod.extract(sftp, lambda f, k: None, ctl, LOAD, md5_arquivo, folders=folders)
    [error] = result["errors"]
    assert error.startswith("/home/sumiu: pasta inexistente")
    assert result["keys"] == [f"raw/sftp/fabrica/GEFUS/load_id={LOAD}/X.csv"]


def test_access_lands_one_csv_per_table(sftp_mod, monkeypatch, tmp_path):
    table = tmp_path / "t.csv"
    table.write_text("A\n1\n")
    rec = {
        "status": "skipped_no_pii",
        "masked_columns": [],
        "registros_total": 1,
        "registros_alterados": 0,
    }
    monkeypatch.setattr(
        sftp_mod, "export_access", lambda src, workdir: [("CCI_Analitico", str(table), rec)]
    )
    result, landed, _, _, _ = run(sftp_mod, {"/home/fabrica/GEFUS/MC.mdb": b"mdb"})
    assert result["keys"] == [f"raw/sftp/fabrica/GEFUS/load_id={LOAD}/MC.mdb/CCI_Analitico.csv"]


class FakePg:
    def __init__(self):
        self.calls = []

    def run(self, sql, parameters=None):
        self.calls.append(("run", sql, parameters))

    def insert_rows(self, table, rows, target_fields, **kwargs):
        self.calls.append(("insert", table, dict(zip(target_fields, rows[0])), kwargs))


def test_control_writes_through_the_postgres_hook(sftp_mod):
    pg = FakePg()
    ctl = sftp_mod.Control(pg, "run")
    rec = {"status": "masked", "masked_columns": [], "registros_total": 1, "registros_alterados": 1}
    ctl.mask("raw/k.csv", rec, "h1", "h2")
    ctl.ingest("/home/fabrica/GEFUS/k.csv", "raw/k.csv", 10, 100, "h1", "success")
    (_, table, row, kw), (_, delete, params), (_, table2, row2, kw2) = pg.calls
    assert table == "lake._masking_log" and row["masked_hash"] == "h2"
    assert kw["replace_index"] == ["minio_key", "source_hash"] and kw["replace_target"] == []
    assert "file_hash IS NULL" in delete and params == ("/home/fabrica/GEFUS/k.csv",)
    assert table2 == "lake._ingest_minio_log" and row2["file_mtime"].timestamp() == 100
    assert kw2["replace_index"] == ["sftp_path", "file_hash"]


def test_dry_run_reads_the_control_but_writes_nothing(sftp_mod):
    pg = FakePg()
    ctl = sftp_mod.Control(pg, "run", write=False)
    ctl.ingest("p", None, 1, 1, None, "error")
    ctl.mask("k", {}, "h", "h")
    assert not pg.calls


# transform and report


DS = contrato.Dataset(
    source="s",
    name="d",
    path="staging/sftp/fabrica/GEFUS/D_*.parquet",
    file_pattern=re.compile(r"D_\d+\.csv"),
    columns=("id", "nome"),
)


class FakeLake:
    def __init__(self, tmp_path, content):
        self.tmp, self.content, self.sent = tmp_path, content, {}

    def sample_bytes(self, key, n=65536, inicio=0):
        return self.content

    def baixar_para_tempfile(self, key, suffix="", tmpdir=None):
        path = self.tmp / f"in{suffix}"
        path.write_bytes(self.content)
        return str(path)

    def upload_arquivo(self, path, key):
        self.sent[key] = pq.read_table(path).column_names


@pytest.fixture
def tasks(monkeypatch, tmp_path):
    fresh_import(monkeypatch, "conversao.processamento", "conversao", LAKE_TMPDIR=str(tmp_path))
    return fresh_import(monkeypatch, "datalake.tasks", "datalake")


KEY = f"raw/sftp/fabrica/GEFUS/load_id={LOAD}/D_1.csv"


@pytest.mark.parametrize(
    "content, status, sent",
    [(b"ID;NOME\n1;Ana\n", "converted", True), (b"ID;OUTRA\n1;Ana\n", "quarantined", False)],
)
def test_transform_lets_into_staging_only_what_fits_the_contract(
    tasks, tmp_path, content, status, sent
):
    lake = FakeLake(tmp_path, content)
    [rec] = tasks.transform(lake, KEY, "e", True, [DS], lambda ds, all_: None)
    assert rec["status"] == status
    assert bool(lake.sent) is sent


def test_transform_leaves_uncataloged_in_raw(tasks, tmp_path):
    lake = FakeLake(tmp_path, b"A\n1\n")
    [rec] = tasks.transform(
        lake, KEY.replace("D_1", "OUTRO"), "e", True, [DS], lambda ds, all_: None
    )
    assert rec["status"] == "uncataloged" and not lake.sent


def test_report_is_quiet_on_a_clean_run_and_groups_uncataloged(tasks):
    clean = [{"raw_key": KEY, "status": "converted", "structural": "approved"}]
    body, failed, notable = tasks.summary({"keys": [KEY]}, clean)
    assert not failed and not notable

    other = [
        {
            "raw_key": f"raw/sftp/a/load_id={LOAD}/X{i}.csv",
            "status": "uncataloged",
            "structural": None,
        }
        for i in range(3)
    ]
    body, failed, notable = tasks.summary({"keys": [KEY]}, other)
    assert notable and not failed and "raw/sftp/a: 3 arquivo(s)" in body

    _, failed, _ = tasks.summary({"keys": [KEY], "errors": ["x"]}, [])
    assert failed
    _, failed, _ = tasks.summary({"keys": [KEY, KEY]}, clean, lost=1)
    assert failed


def test_record_upserts_the_staging_log(tasks):
    pg = FakePg()
    rec = {f: None for f in tasks.STAGING_FIELDS} | {"raw_key": KEY, "column_map": {"A": "a"}}
    tasks.record(pg, [rec])
    [(_, table, row, kw)] = pg.calls
    assert table == "lake._staging_log" and row["column_map"] == '{"A": "a"}'
    assert kw["replace_index"] == ["raw_key", "staging_key", "source_hash"]
