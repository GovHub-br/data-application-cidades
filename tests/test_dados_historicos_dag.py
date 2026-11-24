"""DAG dos dados históricos: estrutura e etapas de I/O, com o S3Hook em memória."""

import hashlib
import importlib.util
import io
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from historico_mcmv import analytics, cleansed, erros, layout, staging

REPO = Path(__file__).resolve().parents[1]


MODULO = REPO / "dags" / "dados_historicos" / "dados_historicos_dag.py"


@pytest.fixture(scope="module")
def dh(tmp_path_factory):
    # o Airflow cria o AIRFLOW_HOME no import: nada no ~ do usuário
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow")))
    spec = importlib.util.spec_from_file_location("dados_historicos_dag", MODULO)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class S3Memoria:
    """O que a DAG usa do S3Hook (provider amazon), num dicionário key -> bytes."""

    transfer_config = None

    def __init__(self, objetos=None):
        self.objetos = dict(objetos or {})

    # API do S3Hook usada pela DAG
    def iter_file_metadata(self, prefix, bucket_name):
        for k, v in sorted(self.objetos.items()):
            if k.startswith(prefix):
                yield {"Key": k, "Size": len(v), "ETag": f'"{hashlib.md5(v).hexdigest()}"'}

    def get_conn(self):
        objetos = self.objetos

        class Cliente:
            def download_file(self, bucket, key, destino, Config=None):
                Path(destino).write_bytes(objetos[key])

        return Cliente()

    def load_file(self, filename, key, bucket_name, replace=False):
        self.objetos[key] = Path(filename).read_bytes()

    def copy_object(self, origem, destino, source_bucket_name, dest_bucket_name):
        self.objetos[destino] = self.objetos[origem]

    def delete_objects(self, bucket, keys):
        for k in keys:
            del self.objetos[k]

    def check_for_key(self, key, bucket_name):
        return key in self.objetos

    def read_key(self, key, bucket_name):
        return self.objetos[key].decode()

    # conveniências dos testes
    def keys(self, prefixo):
        return sorted(k for k in self.objetos if k.startswith(prefixo))

    def texto(self, key):
        return self.objetos[key].decode()

    def tabela(self, key):
        return pq.read_table(io.BytesIO(self.objetos[key]))


def _csv(linhas):
    return ("\n".join(";".join(c) for c in linhas) + "\n").encode()


MAPA_VAZIO = b"arquivo,bloco,acao,linha,fim,colunas,motivo\n"


FAMILIAS = (
    b"familia,padrao,fonte,descricao\n"
    b"entrada,_entrada_,bb,Entrada\n"
    b"controle,_controle$,bb,Controle\n"
)


def _lake_raw():
    return S3Memoria(
        {
            "raw/dados_historicos/bb_2013_06_junho_x_entrada_20130618.csv": _csv(
                # cabeçalho do CSV, depois tudo 2x, inclusive uma cópia da linha de cabeçalho
                [["UF", "UH"], ["SP", "10"], ["UF", "UH"], ["UF", "UH"], ["SP", "10"]]
            ),
            "raw/dados_historicos/bb_2013_06_junho_x_controle.csv": _csv(
                [
                    ["datarespdata"],
                    ["2013-06-17|FULANO|2013-06-17"],
                    ["2013-06-17|FULANO|2013-06-17"],
                ]
            ),
            "staging/historico_mcmv/config/cabecalhos.csv": MAPA_VAZIO
            + b"bb_2013_06_junho_x_controle,,dividir,,,data|resp_arq|data_pos_arq,colado\n",
            "staging/historico_mcmv/config/familias.csv": FAMILIAS,
        }
    )


# ---------------------------------------------------------------------------------------
# DAG


def test_dag_espera_a_raw_e_publica_cada_camada(dh):
    d = dh.dag_instance
    assert d.task_ids == [
        "conferir_raw",
        "raw_para_staging",
        "staging_para_analytics",
        "analytics_para_cleansed",
        "mapear_colunas",
    ]
    assert [a.name for a in d.timetable.asset_condition.objects] == ["dados_historicos_raw"]
    outlets = {t.task_id: [o.name for o in t.outlets] for t in d.tasks}
    assert outlets["raw_para_staging"] == ["dados_historicos_staging"]
    assert outlets["analytics_para_cleansed"] == ["dados_historicos_cleansed"]
    for anterior, seguinte in zip(d.task_ids, d.task_ids[1:]):
        assert d.get_task(anterior).downstream_task_ids == {seguinte}
    # tudo grava sob staging/historico_mcmv/, nunca na conversão antiga nem em tests/
    for prefixo in (layout.STAGING, layout.ANALYTICS, layout.CLEANSED, layout.CONFIG):
        assert prefixo.startswith("staging/historico_mcmv/")


def test_staging_mascara_retoma_e_confere_a_raw(dh, tmp_path):
    lake = _lake_raw()
    assert dh.etapa_staging(lake, tmp_path) == {"ok": 2, "pulado": 0, "erro": 0}
    controle = lake.tabela(layout.STAGING + "bb_2013_06_junho_x_controle.parquet").column(0)
    assert set(controle.to_pylist()) == {"2013-06-17|***|2013-06-17"}
    manifesto = staging.ler_manifesto(lake.texto(layout.MANIFESTO))
    assert manifesto["raw/dados_historicos/bb_2013_06_junho_x_controle.csv"][
        "colunas_mascaradas"
    ] == ("datarespdata:resp_arq")
    # segunda execução: nada mudou na raw, nada é reconvertido; com filtro e forcar, só um
    assert dh.etapa_staging(lake, tmp_path)["pulado"] == 2
    assert dh.etapa_staging(lake, tmp_path, forcar=True, filtro="controle") == {
        "ok": 1,
        "pulado": 0,
        "erro": 0,
    }
    assert dh.conferir_raw_contra_manifesto(lake) == {"arquivos": 2, "novos": 0, "alterados": 0}
    del lake.objetos["raw/dados_historicos/bb_2013_06_junho_x_controle.csv"]
    with pytest.raises(erros.ErroHistorico, match="sumiram"):
        dh.conferir_raw_contra_manifesto(lake)


def test_staging_ignora_arquivo_fora_e_tira_do_manifesto(dh, tmp_path):
    lake = _lake_raw()
    fora = "raw/dados_historicos/bb_2015_08_agosto_loginfesta.csv"
    lake.objetos[fora] = _csv([["login", "senha"], ["x", "1234"]])
    # manifesto de antes da exclusão: o arquivo já tinha sido convertido
    staging.escrever_manifesto([{"raw_key": fora, "status": "ok"}], tmp_path / "m.csv")
    lake.objetos[layout.MANIFESTO] = (tmp_path / "m.csv").read_bytes()
    assert dh.etapa_staging(lake, tmp_path) == {"ok": 2, "pulado": 0, "erro": 0}
    assert fora not in staging.ler_manifesto(lake.texto(layout.MANIFESTO))
    assert not any("loginfesta" in k for k in lake.objetos if k.startswith(layout.STAGING))
    assert dh.conferir_raw_contra_manifesto(lake) == {"arquivos": 2, "novos": 0, "alterados": 0}


def test_analytics_copia_a_selecao_e_tira_a_sobra(dh, tmp_path):
    lake = _lake_raw()
    dh.etapa_staging(lake, tmp_path / "s")
    lake.objetos[layout.ANALYTICS + "antigo.parquet"] = b"x"
    assert dh.etapa_analytics(lake, tmp_path / "a") == {"selecionado": 2}
    assert lake.keys(layout.ANALYTICS) == [
        layout.ANALYTICS + "_analytics_log.parquet",
        layout.ANALYTICS + "bb_2013_06_junho_x_controle.parquet",
        layout.ANALYTICS + "bb_2013_06_junho_x_entrada_20130618.parquet",
    ]
    assert lake.tabela(layout.LOG_ANALYTICS).schema == analytics.SCHEMA_ANALYTICS


def test_analytics_reaproveita_metricas_de_arquivo_que_nao_mudou(dh, tmp_path, monkeypatch):
    lake = _lake_raw()
    dh.etapa_staging(lake, tmp_path / "s")
    dh.etapa_analytics(lake, tmp_path / "a")
    medidos = []
    original = analytics.metricas_parquet
    monkeypatch.setattr(
        analytics,
        "metricas_parquet",
        lambda p, con=None: medidos.append(p.name) or original(p, con),
    )
    # nada mudou na staging: nada é medido de novo e o resultado é o mesmo
    assert dh.etapa_analytics(lake, tmp_path / "b") == {"selecionado": 2}
    assert medidos == []
    # um arquivo alterado: só ele é medido
    chave = layout.STAGING + "bb_2013_06_junho_x_entrada_20130618.parquet"
    lake.objetos[chave] = lake.objetos[chave] + b""  # mesmo conteúdo não muda o etag
    lake.objetos[layout.STAGING + "bb_2013_06_junho_x_controle.parquet"] = lake.objetos[chave]
    dh.etapa_analytics(lake, tmp_path / "c")
    assert medidos == ["bb_2013_06_junho_x_controle.parquet"]


def test_cleansed_por_familia_com_log_e_descartes(dh, tmp_path):
    lake = _lake_raw()
    dh.etapa_staging(lake, tmp_path / "s")
    dh.etapa_analytics(lake, tmp_path / "a")
    lake.objetos[layout.CLEANSED + "saida_de_antes.parquet"] = b"x"
    lake.objetos[layout.CLEANSED + "_001_saida_que_comeca_com_sublinhado.parquet"] = b"x"
    lake.objetos[layout.EVOLUCAO] = b"controle"
    resumo = dh.etapa_cleansed(lake, tmp_path / "c")
    assert (resumo["saidas"], resumo["familias"], resumo["linhas_reais"]) == (2, 2, 2)
    entrada = lake.tabela(layout.CLEANSED + "entrada/bb_2013_06_junho_x_entrada_20130618.parquet")
    assert entrada.column("uf").to_pylist() == ["SP"]
    controle = lake.tabela(layout.CLEANSED + "controle/bb_2013_06_junho_x_controle.parquet")
    assert controle.column("resp_arq").to_pylist() == ["***"]
    assert layout.CLEANSED + "saida_de_antes.parquet" not in lake.objetos
    assert layout.CLEANSED + "_001_saida_que_comeca_com_sublinhado.parquet" not in lake.objetos
    assert lake.objetos[layout.EVOLUCAO] == b"controle"
    log_ = lake.tabela(layout.LOG_CLEANSED)
    assert log_.schema == cleansed.SCHEMA_CLEANSED
    linhas = {r["saida"]: r for r in log_.to_pylist()}
    assert linhas["bb_2013_06_junho_x_entrada_20130618"]["competencia"] == "2013-06"
    assert linhas["bb_2013_06_junho_x_entrada_20130618"]["descartes_por_motivo"] == [
        ("copia_cabecalho", 1)
    ]
    assert lake.tabela(layout.DESCARTES_CLEANSED).num_rows == 1


def test_cleansed_padroniza_e_guarda_a_versao_substituida(dh, tmp_path):
    lake = _lake_raw()
    # a mesma entrada, numa versão "_v2" que só difere no ".0"
    lake.objetos["raw/dados_historicos/bb_2013_06_junho_x_entrada_20130618_v2.csv"] = _csv(
        [["UF", "UH"], ["SP", "10.0"], ["UF", "UH"], ["UF", "UH"], ["SP", "10.0"]]
    )
    lake.objetos[layout.MAPA_FAMILIAS] = FAMILIAS.replace(b"_entrada_,", b"_entrada_\\d+(_v2)?$,")
    dh.etapa_staging(lake, tmp_path / "s")
    dh.etapa_analytics(lake, tmp_path / "a")
    resumo = dh.etapa_cleansed(lake, tmp_path / "c")
    assert (resumo["saidas"], resumo["versoes_substituidas"]) == (2, 1)
    assert resumo["padronizacoes"] == {"numero_ponto_zero": 1}
    log_ = {r["saida"]: r for r in lake.tabela(layout.LOG_CLEANSED).to_pylist()}
    v2 = log_["bb_2013_06_junho_x_entrada_20130618_v2"]
    assert (v2["status"], v2["motivo_substituicao"]) == ("tratado", None)
    original = log_["bb_2013_06_junho_x_entrada_20130618"]
    assert (original["status"], original["substituida_por"]) == (
        "versao_substituida",
        "bb_2013_06_junho_x_entrada_20130618_v2",
    )
    assert original["cleansed_key"] in lake.objetos  # não some: fica fora da pasta da família
    assert lake.tabela(layout.PADRONIZACOES).num_rows == 1
