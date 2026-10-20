"""Drift estrutural do SFTP (`scripts/sftp_drift.py`) e as chaves pousadas pelo
`sftp_para_minio.run`, que alimentam a detecção."""

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Tuple

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
BASE = "CO_CONTRATO;VR_OBRA;NO_MUNICIPIO\n1;10;RECIFE\n2;20;NATAL\n3;30;BELEM\n"


def _importar(nome: str) -> ModuleType:
    for chave, valor in {
        "LAKE_LOCAL_ARTIFACTS": "0",
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": "teste",
        "MINIO_SECRET_KEY": "teste",
        "MINIO_BUCKET": "teste",
        "DB_DW_HOST_MCID": "postgres",
        "DB_DW_USER_MCID": "teste",
        "DB_DW_PASSWORD_MCID": "teste",
        "DB_DW_DBNAME_MCID": "teste",
        "SFTP_HOST": "sftp",
        "SFTP_USER": "teste",
        "SFTP_PASSWORD": "teste",
    }.items():
        os.environ.setdefault(chave, valor)
    for pasta in (SCRIPTS, SCRIPTS.parent / "plugins"):
        if str(pasta) not in sys.path:
            sys.path.insert(0, str(pasta))
    return importlib.import_module(nome)


@pytest.fixture(scope="module")
def drift() -> ModuleType:
    return _importar("sftp_drift")


def _comparar(
    drift: ModuleType,
    tmp_path: Path,
    novo: str,
    velho: str = BASE,
    chave: Tuple[str, ...] = (),
) -> Any:
    (tmp_path / "novo.TXT").write_text(novo, encoding="utf-8")
    (tmp_path / "velho.TXT").write_text(velho, encoding="utf-8")
    relatorio = drift.comparar_arquivos(
        str(tmp_path / "novo.TXT"), str(tmp_path / "velho.TXT"), ".txt", ".txt", chave
    )
    return relatorio["tabelas"][""], relatorio["drift"]


@pytest.mark.parametrize(
    "chave, esperado",
    [
        (
            "raw/sftp/fabrica/GEFUS/INT040_MinisterioCidades_FAR_20231228.TXT",
            ("INT040_MinisterioCidades_FAR_DATA", (2023, 12, 28)),
        ),
        (
            "raw/sftp/fabrica/GEFUS/2024_07_SNH_PMCMV_DADOS_AF_BB.csv",
            ("DATA_SNH_PMCMV_DADOS_AF_BB", (2024, 7, 0)),
        ),
        (
            "raw/sftp/fabrica/CadUnico/ARQ_PESSOA_PBF_09012026_SB8.csv",
            ("ARQ_PESSOA_PBF_DATA_SB8", (2026, 1, 9)),
        ),
        (
            "raw/sftp/fabrica/Analise_SNH/032020_tab_validacao_arquivos_pf.csv",
            ("DATA_tab_validacao_arquivos_pf", (2020, 3, 0)),
        ),
        ("raw/sftp/fabrica/GEFUS/Teste_Transmissao.txt", ("Teste_Transmissao", ())),
    ],
)
def test_serie_troca_a_data_do_nome(
    drift: ModuleType, chave: str, esperado: Tuple[str, Tuple[int, ...]]
) -> None:
    assert drift.serie(chave) == esperado


def test_anterior_e_a_mais_recente_antes_dela_em_qualquer_pasta(
    drift: ModuleType,
) -> None:
    chaves = [
        "raw/sftp/fabrica/GEFUS/ANTERIORES/INT040_X_20231130.TXT",
        "raw/sftp/fabrica/GEFUS/ANTERIORES/INT040_X_20231228.TXT",
        "raw/sftp/fabrica/GEFUS/INT040_X_20240131.TXT",
        "raw/sftp/fabrica/GEFUS/INT039_X_20240130.TXT",
    ]

    assert drift.anterior(chaves[2], chaves) == chaves[1]
    assert drift.anterior(chaves[0], chaves) is None
    assert drift.anterior("raw/sftp/fabrica/Teste_Transmissao.txt", chaves) is None


def test_mesma_entrega_nao_e_drift(drift: ModuleType, tmp_path: Path) -> None:
    tabela, tipos = _comparar(drift, tmp_path, BASE)

    assert tipos == []
    assert tabela["conteudo"]["linhas_iguais"] == 3


def test_pii_fica_fora_do_hash(drift: ModuleType, tmp_path: Path) -> None:
    # o novo ainda não foi mascarado e o anterior já: não pode virar "linha alterada"
    novo = "NU_CPF;NO_PESSOA;VR_OBRA\n12345678901;JOAO;10\n98765432100;MARIA;20\n"
    velho = "NU_CPF;NO_PESSOA;VR_OBRA\na1b2c3d4e5f6a7b8;***;10\nf0e9d8c7b6a5f4e3;***;20\n"

    tabela, tipos = _comparar(drift, tmp_path, novo, velho)

    assert tipos == []
    assert tabela["colunas"]["pii_fora_da_comparacao"] == ["no_pessoa", "nu_cpf"]
    assert tabela["conteudo"]["colunas_comparadas"] == 1
    assert tabela["conteudo"]["linhas_iguais"] == 2


def test_so_linhas_novas_nao_e_drift(drift: ModuleType, tmp_path: Path) -> None:
    tabela, tipos = _comparar(drift, tmp_path, BASE + "4;40;SALVADOR\n")

    assert tipos == []
    assert tabela["linhas"] == {"anterior": 3, "atual": 4, "variacao": 1}
    assert tabela["conteudo"]["linhas_novas"] == 1


def test_coluna_nova_e_sumida(drift: ModuleType, tmp_path: Path) -> None:
    novo = "CO_CONTRATO;VR_OBRA;CO_IBGE\n1;10;2611606\n2;20;2408102\n3;30;1501402\n"

    tabela, tipos = _comparar(drift, tmp_path, novo)

    assert tabela["colunas"]["adicionadas"] == ["co_ibge"]
    assert tabela["colunas"]["removidas"] == ["no_municipio"]
    assert tipos == ["colunas_adicionadas", "colunas_removidas"]
    assert tabela["conteudo"]["linhas_iguais"] == 3


def test_ordem_das_colunas_nao_vira_linha_alterada(
    drift: ModuleType, tmp_path: Path
) -> None:
    novo = "VR_OBRA;CO_CONTRATO;NO_MUNICIPIO\n10;1;RECIFE\n20;2;NATAL\n30;3;BELEM\n"

    tabela, tipos = _comparar(drift, tmp_path, novo)

    assert tipos == ["ordem_das_colunas"]
    assert tabela["conteudo"]["linhas_iguais"] == 3


def test_linha_alterada_sem_chave_vira_removida_mais_nova(
    drift: ModuleType, tmp_path: Path
) -> None:
    tabela, tipos = _comparar(drift, tmp_path, BASE.replace("2;20;NATAL", "2;25;NATAL"))

    conteudo = tabela["conteudo"]
    assert (conteudo["linhas_novas"], conteudo["linhas_removidas"]) == (1, 1)
    assert tipos == ["linhas_removidas"]


def test_linha_alterada_com_chave(drift: ModuleType, tmp_path: Path) -> None:
    novo = BASE.replace("2;20;NATAL", "2;25;NATAL") + "4;40;SALVADOR\n"

    tabela, tipos = _comparar(drift, tmp_path, novo, chave=("co_contrato",))

    conteudo = tabela["conteudo"]
    assert conteudo["registros_alterados"] == 1
    assert conteudo["registros_novos"] == 1
    assert conteudo["registros_removidos"] == 0
    assert conteudo["registros_iguais"] == 2
    assert tipos == ["linhas_alteradas"]


def test_linha_removida_e_chave_duplicada(drift: ModuleType, tmp_path: Path) -> None:
    novo = "CO_CONTRATO;VR_OBRA;NO_MUNICIPIO\n1;10;RECIFE\n1;11;RECIFE\n"

    tabela, tipos = _comparar(drift, tmp_path, novo, chave=("co_contrato",))

    assert tabela["conteudo"]["chaves_duplicadas"] == {"anterior": 0, "atual": 1}
    assert tabela["conteudo"]["registros_removidos"] == 2
    # com duas linhas iguais em contrato e município, essas colunas ficaram constantes
    assert tipos == [
        "colunas_ficaram_constantes",
        "linhas_alteradas",
        "linhas_removidas",
        "volume_caiu",
    ]


def test_run_devolve_a_chave_pousada(monkeypatch: pytest.MonkeyPatch) -> None:
    ingest = _importar("sftp_para_minio")
    monkeypatch.setattr(ingest, "baixar_para_tempfile", lambda *a: "/dev/null")
    monkeypatch.setattr(ingest, "_compute_hash", lambda *a: "md5")
    monkeypatch.setattr(ingest, "subir_para_minio", lambda m, t, nome: f"raw/{nome}")
    monkeypatch.setattr(ingest, "_registrar_ingest", lambda *a, **k: None)
    monkeypatch.setattr(ingest.os, "unlink", lambda *a: None)
    pousadas: list = []

    resultado = ingest.processar_arquivo_simples(
        None, None, "", "/home/fabrica/GEFUS/X.TXT", 1, 2, set(), set(), pousadas=pousadas
    )

    assert resultado == "success"
    assert pousadas == ["raw/sftp/fabrica/GEFUS/X.TXT"]


def test_xlsx_compara_aba_a_aba(drift: ModuleType, tmp_path: Path) -> None:
    import openpyxl

    for nome, linhas in (("velho", [[1, "A"], [2, "B"]]), ("novo", [[1, "A"], [2, "C"]])):
        wb = openpyxl.Workbook()
        wb.worksheets[0].title = "Dados"
        wb.worksheets[0].append(["Código", "Situação"])
        for linha in linhas:
            wb.worksheets[0].append(linha)
        wb.create_sheet("Notas" if nome == "velho" else "Resumo").append(["x"])
        wb.save(tmp_path / f"{nome}.xlsx")

    relatorio = drift.comparar_arquivos(
        str(tmp_path / "novo.xlsx"), str(tmp_path / "velho.xlsx"), ".xlsx", ".xlsx"
    )

    assert relatorio["abas"] == {"adicionadas": ["Resumo"], "removidas": ["Notas"]}
    assert relatorio["tabelas"]["Dados"]["conteudo"]["linhas_novas"] == 1
    assert relatorio["drift"] == [
        "linhas_removidas",
        "abas_adicionadas",
        "abas_removidas",
    ]


def test_mdb_compara_tabela_a_tabela(
    drift: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # o mdbtools exporta cada tabela para CSV em UTF-8 com vírgula
    tabelas = {
        "novo.mdb": {"Obras": "APF,QT_UH\n1,10\n2,25\n", "Nova": "A\n1\n"},
        "velho.mdb": {"Obras": "APF,QT_UH\n1,10\n2,20\n"},
    }
    monkeypatch.setattr(drift, "mdb_disponivel", lambda: True)
    monkeypatch.setattr(drift, "mdb_tabelas", lambda p: list(tabelas[Path(p).name]))
    monkeypatch.setattr(
        drift,
        "mdb_export_para_csv",
        lambda p, t, dst: Path(dst).write_text(tabelas[Path(p).name][t]),
    )
    for nome in tabelas:
        (tmp_path / nome).write_bytes(b"mdb")

    relatorio = drift.comparar_arquivos(
        str(tmp_path / "novo.mdb"), str(tmp_path / "velho.mdb"), ".mdb", ".mdb", ("apf",)
    )

    assert relatorio["abas"]["adicionadas"] == ["Nova"]
    assert relatorio["tabelas"]["Obras"]["conteudo"]["registros_alterados"] == 1
    assert relatorio["drift"] == ["linhas_alteradas", "abas_adicionadas"]


def test_serie_ignora_extensoes_encadeadas_e_sufixo_de_colisao(drift: ModuleType) -> None:
    zipado = "raw/sftp/fabrica/GEFUS/CadUnico/ARQ_PESSOA_PBF_09012026_SB8.TXT.zip"
    colisao = "raw/sftp/fabrica/GEFUS/ANTERIORES/INT040_X_20231228_0000.TXT"

    assert (
        drift.serie(zipado)[0]
        == drift.serie("raw/sftp/ARQ_PESSOA_PBF_12122025_SB8.TXT")[0]
    )
    assert drift.serie(colisao) == ("INT040_X_DATA", (2023, 12, 28))
    # ano sozinho não é sufixo de colisão (nem data)
    assert drift.serie("raw/sftp/RELACAO_JANEIRO_E_ABRIL_2026.xlsx")[0] == (
        "RELACAO_JANEIRO_E_ABRIL_2026"
    )


def test_zip_fechado_compara_o_membro(drift: ModuleType, tmp_path: Path) -> None:
    import zipfile

    with zipfile.ZipFile(tmp_path / "novo.zip", "w") as zf:
        zf.writestr("ARQ.TXT", BASE + "4;40;SALVADOR\n")
    (tmp_path / "velho.TXT").write_text(BASE, encoding="utf-8")

    relatorio = drift.comparar_arquivos(
        str(tmp_path / "novo.zip"), str(tmp_path / "velho.TXT"), ".zip", ".txt"
    )

    assert relatorio["drift"] == []
    assert relatorio["tabelas"][""]["conteudo"]["linhas_novas"] == 1


def test_carimbo_da_entrega_sai_da_comparacao(drift: ModuleType, tmp_path: Path) -> None:
    # dt_movimento tem um valor só no arquivo e muda a cada entrega
    velho = "CO_CONTRATO;VR_OBRA;DT_MOVIMENTO\n1;10;31/07/2026\n2;20;31/07/2026\n"
    novo = "CO_CONTRATO;VR_OBRA;DT_MOVIMENTO\n1;10;31/08/2026\n2;25;31/08/2026\n"

    tabela, tipos = _comparar(drift, tmp_path, novo, velho, chave=("co_contrato",))

    assert tabela["colunas"]["carimbo_da_entrega"] == ["dt_movimento"]
    assert tabela["conteudo"]["registros_iguais"] == 1
    assert tabela["conteudo"]["registros_alterados"] == 1
    assert tipos == ["linhas_alteradas"]


def test_coluna_que_esvaziou(drift: ModuleType, tmp_path: Path) -> None:
    novo = BASE.replace(";RECIFE", ";").replace(";NATAL", ";").replace(";BELEM", ";")

    tabela, tipos = _comparar(drift, tmp_path, novo)

    assert tabela["colunas"]["esvaziadas"] == ["no_municipio"]
    assert "colunas_esvaziadas" in tipos


def test_formato_que_mudou_nao_e_carimbo(drift: ModuleType, tmp_path: Path) -> None:
    velho = "CO_CONTRATO;VR_FGTS;VR_OBRA\n1;0,00;10,50\n2;0,00;20,00\n"
    novo = "CO_CONTRATO;VR_FGTS;VR_OBRA\n1;0;10.5\n2;0;20\n"

    tabela, tipos = _comparar(drift, tmp_path, novo, velho)

    assert tabela["colunas"]["formato_mudou"]["vr_fgts"] == {
        "anterior": "9,9",
        "atual": "9",
    }
    assert tabela["colunas"]["carimbo_da_entrega"] == []
    assert "formato_das_colunas" in tipos
    mais = {
        m["coluna"]: m["linhas_com_valor_novo"]
        for m in tabela["conteudo"]["colunas_que_mais_mudaram"]
    }
    assert mais == {"vr_fgts": 2, "vr_obra": 2}


def test_coluna_que_ficou_constante(drift: ModuleType, tmp_path: Path) -> None:
    novo = "CO_CONTRATO;VR_OBRA;NO_MUNICIPIO\n1;0;RECIFE\n2;0;NATAL\n3;0;BELEM\n"

    tabela, tipos = _comparar(drift, tmp_path, novo)

    assert tabela["colunas"]["ficaram_constantes"] == ["vr_obra"]
    assert tabela["colunas"]["esvaziadas"] == []
    assert "colunas_ficaram_constantes" in tipos
