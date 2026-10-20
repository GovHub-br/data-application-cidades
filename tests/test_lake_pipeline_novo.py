"""A varredura do lake antigo (mascaramento e raw_para_staging) não toca nas
partições do pipeline novo (plugins/ingestion), reconhecidas pelo _SUCCESS."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from lake_utils import separar_pipeline_novo  # noqa: E402


def test_objects_in_folders_with_success_marker_are_set_aside() -> None:
    objetos = [
        ("raw/fgv/incc_m.xlsx", 100),
        ("raw/fgv/incc_m/2026-10-08/060000/INCC.xlsx", 200),
        ("raw/fgv/incc_m/2026-10-08/060000/_SUCCESS", 50),
        ("raw/sftp/fabrica/GEFUS/foo.csv", 300),
    ]

    mantidos, pulados = separar_pipeline_novo(objetos)

    assert mantidos == [
        ("raw/fgv/incc_m.xlsx", 100),
        ("raw/sftp/fabrica/GEFUS/foo.csv", 300),
    ]
    assert [k for k, _ in pulados] == [
        "raw/fgv/incc_m/2026-10-08/060000/INCC.xlsx",
        "raw/fgv/incc_m/2026-10-08/060000/_SUCCESS",
    ]


def test_only_the_marker_folder_itself_is_set_aside() -> None:
    # O _SUCCESS vale para a pasta dele: nem a pasta-mãe nem as irmãs.
    objetos = [
        ("raw/bacen/sgs/2026-10-08/060000/_SUCCESS", 1),
        ("raw/bacen/sgs/2026-10-08/060000/ipca.json", 1),
        ("raw/bacen/sgs/2026-10-08/120000/ipca.json", 1),
        ("raw/bacen/financiamentos_imobiliarios.json", 1),
    ]

    mantidos, _ = separar_pipeline_novo(objetos)

    assert [k for k, _ in mantidos] == [
        "raw/bacen/sgs/2026-10-08/120000/ipca.json",
        "raw/bacen/financiamentos_imobiliarios.json",
    ]


def test_without_markers_nothing_changes() -> None:
    objetos = [("raw/a.csv", 1), ("raw/b/c.txt", 2)]

    assert separar_pipeline_novo(objetos) == (objetos, [])
