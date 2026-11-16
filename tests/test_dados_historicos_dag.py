"""DAG dos dados históricos: raw -> staging -> analytics -> cleansed -> colunas.

O módulo da DAG fica fora do `pythonpath` dos testes (dags/ colide com scripts/), então é
carregado pelo caminho. O lake é um dicionário em memória no lugar do S3Hook.
"""

import importlib.util
import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = Path(__file__).resolve().parents[1]
MODULO = REPO / "dags" / "dados_historicos" / "dados_historicos_dag.py"
HOJE = date(2026, 11, 14)


@pytest.fixture(scope="module")
def dh(tmp_path_factory):
    # o Airflow cria o AIRFLOW_HOME no import: nada no ~ do usuário
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow")))
    spec = importlib.util.spec_from_file_location("dados_historicos_dag", MODULO)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class LakeMemoria:
    """O que a DAG usa do bucket, num dicionário key -> bytes."""

    def __init__(self, objetos=None):
        self.objetos = dict(objetos or {})

    def listar(self, prefixo):
        import hashlib

        return [
            (k, len(v), hashlib.md5(v).hexdigest())
            for k, v in sorted(self.objetos.items())
            if k.startswith(prefixo)
        ]

    def baixar(self, key, destino):
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(self.objetos[key])
        return destino

    def subir(self, local, key):
        self.objetos[key] = Path(local).read_bytes()

    def copiar(self, origem, destino):
        self.objetos[destino] = self.objetos[origem]

    def apagar(self, keys):
        for k in keys:
            del self.objetos[k]

    def ler_texto(self, key):
        return self.objetos[key].decode() if key in self.objetos else None

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
    # tudo grava em tests/ até a validação terminar
    for prefixo in (dh.STAGING, dh.ANALYTICS, dh.CLEANSED, dh.CONFIG):
        assert prefixo.startswith("tests/")


def test_normalizacao_de_nomes_e_a_do_lake(dh):
    from lake_utils import normalizar_colunas

    nomes = ["Município", "municÃ\xadpio", "% Meta R$", "", "UF", "UF", "﻿Código"]
    assert dh.normalizar_colunas(nomes) == normalizar_colunas(nomes)


# ---------------------------------------------------------------------------------------
# 1. staging


def test_chave_espelha_a_raw(dh):
    assert dh.chave_staging("raw/dados_historicos/bb_2012_entrada.csv") == (
        "tests/staging/dados_historicos/bb_2012_entrada.parquet"
    )
    with pytest.raises(dh.ErroHistorico):
        dh.chave_staging("raw/sftp/x.csv")


def test_conversao_nao_tira_nenhuma_linha(dh, tmp_path):
    # a duplicação 2x, a linha vazia e o subtítulo continuam: quem decide é o cleansed
    src = tmp_path / "t.csv"
    src.write_text(
        "fase_do_pmcmv;município;valor\n"
        "Fase II;;\n"
        "Fase II;AMERICANA;1.000,50\n"
        "Fase II;AMERICANA;1.000,50\n"
        ";;\n"
        ";;\n"
        '"Fase I";"CAMARAGIBE; PE";0012\n',
        encoding="utf-8",
    )
    info = dh.converter_csv(src, tmp_path / "t.parquet", "raw/dados_historicos/t.csv")
    tabela = pq.read_table(tmp_path / "t.parquet").to_pandas()
    assert info["n_linhas"] == len(tabela) == 6
    assert info["encoding"] == "utf-8"
    assert list(tabela.columns) == ["fase_do_pmcmv", "municipio", "valor", *dh.LINHAGEM]
    assert tabela["valor"].tolist() == ["", "1.000,50", "1.000,50", "", "", "0012"]
    assert tabela["municipio"].iloc[-1] == "CAMARAGIBE; PE"
    assert (tabela["_source_file"] == "raw/dados_historicos/t.csv").all()


def test_so_cabecalho_e_sem_coluna_viram_parquet_vazio(dh, tmp_path):
    (tmp_path / "h.csv").write_text("a;b\n", encoding="utf-8")
    info = dh.converter_csv(tmp_path / "h.csv", tmp_path / "h.parquet", "r/h.csv")
    assert info["n_linhas"] == 0
    assert pq.read_table(tmp_path / "h.parquet").column_names[:2] == ["a", "b"]
    (tmp_path / "s.csv").write_text("\n", encoding="utf-8")
    info = dh.converter_csv(tmp_path / "s.csv", tmp_path / "s.parquet", "r/s.csv")
    assert info["n_colunas"] == 0
    assert pq.read_table(tmp_path / "s.parquet").column_names == dh.LINHAGEM


def test_retomada_pula_so_o_que_ja_bate(dh):
    ok = {"status": "ok", "raw_etag": "abc"}
    assert dh.ja_convertido(ok, "abc", True)
    assert not dh.ja_convertido(ok, "abc", False)
    assert not dh.ja_convertido(ok, "outro", True)
    assert not dh.ja_convertido({"status": "erro", "raw_etag": "abc"}, "abc", True)
    assert not dh.ja_convertido(None, "abc", True)


def test_categorias_de_dado_pessoal(dh):
    pf = ["cod_cpf", "txt_nome_benef", "txt_endereco_und_alien", "vlr_renda", "dte_nascimento"]
    assert dh.colunas_pii(pf) == {
        "cod_cpf": "cpf",
        "txt_nome_benef": "nome",
        "txt_endereco_und_alien": "endereco",
    }
    # endereço de empreendimento (sem indicador de pessoa física) não é dado pessoal
    obra = ["endereco_do_empreendimento", "nome_do_empreendimento", "cnpj_proponente"]
    assert dh.colunas_pii(obra) == {}
    assert dh.colunas_pii(["resp_arq", "tel_resp_arq", "e_mail_resp_arq", "data"]).keys() == {
        "resp_arq",
        "tel_resp_arq",
        "e_mail_resp_arq",
    }


def _parquet_staging(path, dados):
    n = len(next(iter(dados.values())))
    meta = {b"source_file": b"raw/x.csv"}
    campos = [pa.field(c, pa.string()) for c in [*dados, *["_source_file"]]]
    tabela = pa.table(
        {**dados, "_source_file": ["raw/x.csv"] * n}, schema=pa.schema(campos, metadata=meta)
    )
    pq.write_table(tabela, path)


def test_mascara_redige_nome_e_mantem_cpf_em_hash(dh, tmp_path):
    p = tmp_path / "b.parquet"
    _parquet_staging(
        p,
        {
            "cod_cpf": ["a1b2c3d4e5f6", "a1b2c3d4e5f6", ""],
            "txt_nome_benef": ["FULANA", "FULANA", ""],
            "vlr_renda": ["100", "100", "50"],
        },
    )
    assert dh.mascarar_parquet(p) == ["txt_nome_benef"]
    t = pq.read_table(p)
    assert t.column("txt_nome_benef").to_pylist() == ["***", "***", ""]  # vazio continua vazio
    assert t.column("cod_cpf").to_pylist() == ["a1b2c3d4e5f6", "a1b2c3d4e5f6", ""]
    assert t.schema.metadata[b"source_file"] == b"raw/x.csv"


def test_cpf_em_claro_para_tudo(dh, tmp_path):
    p = tmp_path / "c.parquet"
    _parquet_staging(p, {"cod_cpf": ["12345678901"], "uf": ["SP"]})
    with pytest.raises(dh.ErroDadoPessoal):
        dh.mascarar_parquet(p)


def test_mascara_campo_dentro_da_coluna_colada(dh, tmp_path):
    # o nome da coluna colada é o cabeçalho grudado e casa com "_resp_": não pode virar
    # "***" inteira (o dividir do cleansed perderia os campos)
    p = tmp_path / "d.parquet"
    _parquet_staging(
        p, {"datacnpj_bbresp_arqtel_resp_arq": ["2013-06-17|0001|FULANO|2013-06-17", "x|y"]}
    )
    campos = ["data", "cnpj_bb", "resp_arq", "data_pos_arq"]
    assert dh.mascarar_parquet(p, campos) == ["datacnpj_bbresp_arqtel_resp_arq:resp_arq"]
    # a linha torta fica como está: o dividir do cleansed recusa o arquivo
    assert pq.read_table(p).column(0).to_pylist() == ["2013-06-17|0001|***|2013-06-17", "x|y"]


def _lake_raw():
    return LakeMemoria(
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
            "tests/config/dados_historicos/cabecalhos.csv": MAPA_VAZIO
            + b"bb_2013_06_junho_x_controle,,dividir,,,data|resp_arq|data_pos_arq,colado\n",
            "tests/config/dados_historicos/familias.csv": FAMILIAS,
        }
    )


def test_staging_mascara_retoma_e_confere_a_raw(dh, tmp_path):
    lake = _lake_raw()
    assert dh.etapa_staging(lake, tmp_path) == {"ok": 2, "pulado": 0, "erro": 0}
    controle = lake.tabela(dh.STAGING + "bb_2013_06_junho_x_controle.parquet").column(0)
    assert set(controle.to_pylist()) == {"2013-06-17|***|2013-06-17"}
    manifesto = dh.ler_manifesto(lake.ler_texto(dh.MANIFESTO))
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
    with pytest.raises(dh.ErroHistorico, match="sumiram"):
        dh.conferir_raw_contra_manifesto(lake)


# ---------------------------------------------------------------------------------------
# 2. analytics


def _parquet(path, dados):
    dados = {**dados, "_source_file": ["x"] * len(next(iter(dados.values()), []))}
    pq.write_table(pa.table(dados), path)
    return path


def test_periodo_do_nome(dh):
    assert dh.periodo("bb_2012_10_outubro_entrada_bb_20121023") == "201210"
    assert dh.periodo("historico_recente_202406_snh_pmcmv_dados_prioritarios_af_bb") == "202406"
    assert dh.periodo("caixa_002_2018_bext_mai18") == "201805"
    assert dh.periodo("caixa_003_2017_bext_jan_2017") == "201701"
    assert dh.periodo("caixa_001_2016_bext_31102016") == "201610"
    assert dh.periodo("caixa_002_2018_novo_relat_rio_executivo") == "201800"
    assert dh.periodo("sem_data") == ""


def test_hash_ignora_ordem_das_linhas_e_nome_das_colunas(dh, tmp_path):
    a = dh.metricas_parquet(_parquet(tmp_path / "a.parquet", {"x": ["1", "2"], "y": ["a", ""]}))
    b = dh.metricas_parquet(_parquet(tmp_path / "b.parquet", {"p": ["2", "1"], "q": ["", "a"]}))
    c = dh.metricas_parquet(_parquet(tmp_path / "c.parquet", {"x": ["1", "2"], "y": ["a", "b"]}))
    assert a["hash_conteudo"] == b["hash_conteudo"] != c["hash_conteudo"]
    d = dh.metricas_parquet(
        _parquet(tmp_path / "d.parquet", {"x": ["1", "2"] * 2, "y": ["a", ""] * 2})
    )
    assert d["hash_conteudo"] != a["hash_conteudo"]  # o dobro de linhas não é cópia
    e = dh.metricas_parquet(
        _parquet(tmp_path / "e.parquet", {"x": ["1", "  ", ""], "y": ["", "", "z"]})
    )
    assert (e["n_linhas"], e["n_colunas"], e["celulas_preenchidas"]) == (3, 2, 2)


def _reg_analytics(arquivo, hash_conteudo="h1", colunas=("a",), linhas=2, cheias=2):
    return {
        "arquivo": arquivo,
        "periodo": arquivo.split("_")[0],
        "n_linhas": linhas,
        "n_colunas": len(colunas),
        "colunas": list(colunas),
        "celulas_preenchidas": cheias,
        "hash_conteudo": hash_conteudo,
    }


def test_classificar(dh):
    modelo = sorted(dh.COLUNAS_MODELO_2009)
    regs = dh.classificar(
        [
            _reg_analytics("201305_copia_de_b", "h1"),
            _reg_analytics("201204_b", "h1"),
            _reg_analytics("201207_branco", "h3", cheias=0),
            _reg_analytics("201209_modelo", "hm", colunas=modelo),
        ]
    )
    assert {r["arquivo"]: (r["status"], r["copia_de"]) for r in regs} == {
        "201204_b": ("selecionado", None),
        "201305_copia_de_b": ("copia", "201204_b"),  # fica o de período mais antigo
        "201207_branco": ("vazio", None),
        "201209_modelo": ("modelo_2009_sem_dado", None),
    }


def test_analytics_copia_a_selecao_e_tira_a_sobra(dh, tmp_path):
    lake = _lake_raw()
    dh.etapa_staging(lake, tmp_path / "s")
    lake.objetos[dh.ANALYTICS + "antigo.parquet"] = b"x"
    assert dh.etapa_analytics(lake, tmp_path / "a") == {"selecionado": 2}
    assert sorted(k for k, _, _ in lake.listar(dh.ANALYTICS)) == [
        dh.ANALYTICS + "_analytics_log.parquet",
        dh.ANALYTICS + "bb_2013_06_junho_x_controle.parquet",
        dh.ANALYTICS + "bb_2013_06_junho_x_entrada_20130618.parquet",
    ]
    assert lake.tabela(dh.LOG_ANALYTICS).schema == dh.SCHEMA_ANALYTICS


# ---------------------------------------------------------------------------------------
# 3. tratamento estrutural


def _df(linhas, colunas=None):
    colunas = colunas or [f"unnamed_{i}" for i in range(len(linhas[0]))]
    return pd.DataFrame(linhas, columns=colunas, dtype=str)


def _2x(linhas):
    return linhas + linhas[::-1]  # a exportação do dump: tudo 2x, a 2ª cópia em outra ordem


def _sem_origem(df):
    return [c for c in df.columns if c != "_linha_origem"]


def test_duplicacao_mantem_repeticao_legitima_e_ordem(dh):
    unico = dh.desfazer_duplicacao(_df(_2x([["a"], ["b"], ["c"], ["b"]]), ["x"]))
    assert unico.x.tolist() == ["a", "b", "c", "b"]
    assert unico._linha_origem.tolist() == [0, 1, 2, 3]
    with pytest.raises(dh.ErroHistorico, match="ímpar"):
        dh.desfazer_duplicacao(_df([["a"], ["a"], ["b"]], ["x"]))


def test_promover_com_titulo_e_copia_do_cabecalho(dh):
    linhas = [
        ["Posição: 21/11/2011", "", "", ""],
        ["UF", "Município", "UH", "Valor"],
        ["SP", "Americana", "10", "1.000,50"],
        ["RJ", "Niterói", "5", "20"],
    ]
    res = dh.tratar_arquivo(_df(_2x(linhas)), dh.Regra("promover", 1))
    assert _sem_origem(res.dados) == ["uf", "municipio", "uh", "valor"]
    assert res.dados.uf.tolist() == ["SP", "RJ"]
    assert res.por_motivo == {"titulo": 1, "cabecalho": 1}


def test_compor_grupo_mesclado_e_centralizado(dh):
    linhas = [
        ["", "Desligamento", "", "", "Aprovado CEF", "", "Contratação total", ""],
        ["UF", "Unidades", "Valor", "Unidades", "Valor", "Meta", "Unidades", ""],
        ["AC", "1", "2", "3", "4", "5", "6", ""],
    ]
    res = dh.tratar_arquivo(_df(_2x(linhas)), dh.Regra("compor", 0))
    assert _sem_origem(res.dados) == [
        "uf",
        "desligamento_unidades",
        "desligamento_valor",
        "aprovado_cef_unidades",
        "aprovado_cef_valor",
        "aprovado_cef_meta",
        "contratacao_total_unidades",
    ]
    assert res.colunas_removidas == ["coluna_8"]


def test_descartes_vazia_subtitulo_total_rodape(dh):
    cols = ["fase", "codigo", "uh", "vgv"]
    linhas = [
        ["Fase II", "", "", ""],  # subtítulo
        ["Fase II", "1", "10", "100"],
        ["", "-", "", ""],  # vazia
        ["TOTAL FASE I", "", "10", "100"],  # total
        ["", "", "", "123"],  # número sozinho no meio: fica
        ["BELLA CITTA TOTAL VILLE", "3", "7", "70"],  # TOTAL no meio do nome: dado
        ["BRASIL", "", "13", "174"],  # total nacional
        ["", "", "999", ""],  # rodapé
    ]
    res = dh.tratar_arquivo(_df(_2x(linhas), cols))
    assert res.dados.codigo.tolist() == ["1", "", "3"]
    assert res.por_motivo == {"subtitulo": 1, "vazia": 1, "total": 2, "rodape": 1}


def test_coluna_vazia_sem_nome_sai_e_com_nome_fica(dh):
    df = _df(_2x([["a", "", "", "1"], ["b", " ", "", "2"]]), ["x", "unnamed_1", "observacoes", "y"])
    res = dh.tratar_arquivo(df)
    assert (res.colunas_removidas, res.colunas_vazias) == (["unnamed_1"], ["observacoes"])


def test_conferencia_pega_perda(dh):
    df = _df(_2x([["a", "1"], ["b", "2"]]), ["x", "y"])
    res = dh.tratar_arquivo(df)
    res.dados = res.dados.iloc[:1]
    with pytest.raises(dh.ErroHistorico, match="linhas não fecham"):
        dh.conferir(df, [res])


def test_blocos_empilhados(dh):
    linhas = [
        ["UF", "Unidades", ""],
        ["AC", "10", ""],
        ["Relatório 2: por UF e Público Alvo", "", ""],
        ["UF", "Público Alvo", "Valor"],
        ["AC", "0 A 3 SM", "5"],
    ]
    regras = [
        dh.Regra("promover", 0, bloco="relatorio_1", fim=2),
        dh.Regra("promover", 3, bloco="relatorio_2"),
    ]
    (b1, r1), (b2, r2) = dh.tratar_blocos(_df(_2x(linhas)), regras)
    assert (b1, _sem_origem(r1.dados)) == ("relatorio_1", ["uf", "unidades"])
    assert _sem_origem(r2.dados) == ["uf", "publico_alvo", "valor"]
    with pytest.raises(dh.ErroHistorico, match="fim"):
        dh.tratar_blocos(_df(_2x(linhas)), [dh.Regra("promover", 0), dh.Regra("promover", 3)])


def test_nomear_e_dividir(dh):
    df = _df(_2x([["466787", "FAR"], ["466788", "FDS"]]), ["600182", "far"])
    res = dh.tratar_arquivo(df, dh.Regra("nomear", colunas=["cod_empreendimento", "modalidade"]))
    assert res.dados.cod_empreendimento.tolist() == ["466787", "466788"]
    colado = _df(_2x([["389013|403204|0|2012-10-19"], ["1|2||2013-01-01"]]), ["cod"])
    nomes = ["cod_contrato_pj", "cod_operacao", "cod_sit", "dte_assin"]
    res = dh.tratar_arquivo(colado, dh.Regra("dividir", colunas=nomes))
    assert res.dados.iloc[1].tolist()[:4] == ["1", "2", "", "2013-01-01"]
    with pytest.raises(dh.ErroHistorico, match="campos"):
        dh.tratar_arquivo(
            _df(_2x([["a|b|c|d"], ["a|b"]]), ["x"]), dh.Regra("dividir", colunas=nomes)
        )


# ---------------------------------------------------------------------------------------
# 3b. datas


@pytest.mark.parametrize(
    "nome, esperado",
    [
        ("bb_2012_10_outubro_entrada_bb_20121023", (date(2012, 10, 23), "dia")),
        ("caixa_002_2017_bext_fev2017", (date(2017, 2, 1), "mes")),
        ("bb_2014_11_novembro_3011_min_cidades_pf_pf", (date(2014, 11, 30), "dia")),
        (
            "001_2012_10_outubro_20121009_bases_relatório_executivo_1610201",
            (date(2012, 10, 16), "dia"),
        ),
        ("bb_2013_02_fevereiro_propostas_bb", (date(2013, 2, 1), "pasta")),
        ("024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb", (None, None)),
    ],
)
def test_data_do_nome(dh, nome, esperado):
    assert dh.data_do_nome(nome, HOJE) == esperado


def test_para_datas(dh):
    datas = dh.para_datas(["2019-2-28", "31/10/2024", "2010-11-10 00:00:00", "2016-06-31", ""])
    assert [None if pd.isna(d) else d.date() for d in datas] == [
        date(2019, 2, 28),
        date(2024, 10, 31),
        date(2010, 11, 10),
        None,
        None,
    ]


def _reg_data(arquivo, contrato=None, declarada=None, coluna=None):
    return {
        "arquivo": arquivo,
        "contrato_mais_novo": contrato,
        "data_declarada": declarada,
        "data_declarada_coluna": coluna,
        "data_declarada_distintas": 1 if declarada else 0,
    }


def test_movimento_da_snh_vale_mais_que_o_nome(dh):
    r = _reg_data(
        "historico_recente_202402_snh_af_bb", date(2018, 11, 28), "28/02/2025", "data_de_movimento"
    )
    dh.datar([r], HOJE)
    assert (r["competencia"], r["data_corte"]) == ("2025-02", date(2025, 2, 28))
    assert r["alertas_data"] == ["competencia_diferente_do_nome", "contrato_mais_novo_antigo"]


def test_lote_pelo_pf_pf_e_corte_nunca_antes_do_contrato(dh):
    lote = [
        _reg_data("bb_2018_2018_04_06_pf_pf", date(2018, 4, 5)),
        _reg_data("bb_2018_2018_04_06_pj", date(2018, 3, 13)),
    ]
    dh.datar(lote, HOJE)
    assert {r["competencia"] for r in lote} == {"2018-03"}
    depois = [_reg_data("bb_2018_2018_10_30_pf_pf", date(2018, 11, 6))]
    dh.datar(depois, HOJE)
    assert (depois[0]["competencia"], depois[0]["data_corte"]) == ("2018-10", date(2018, 11, 6))
    meio = [_reg_data("bb_2015_03_marco_min_cidades_pf_pf", date(2015, 3, 13))]
    dh.datar(meio, HOJE)
    assert meio[0]["alertas_data"] == ["corte_no_meio_do_mes"]


def test_fora_de_lote_vale_o_nome(dh):
    regs = [_reg_data("bb_2014_03_marco_entrada_bb_20140401"), _reg_data("sem_data")]
    dh.datar(regs, HOJE)
    assert (regs[0]["competencia"], regs[0]["alertas_data"]) == (
        "2014-04",
        ["pasta_diferente_do_nome"],
    )
    assert regs[1]["alertas_data"] == ["sem_competencia"]


# ---------------------------------------------------------------------------------------
# 3c. cleansed


def _sel(arquivo, colunas):
    return {"arquivo": arquivo, "analytics_key": f"a/{arquivo}.parquet", "colunas": colunas}


def test_plano_familia_excluidos_e_cabecalho_ruim(dh):
    familias = dh.ler_familias(pd.read_csv(io.BytesIO(FAMILIAS), dtype=str))
    selecionados = [
        _sel("201204_entrada_bom", ["uf", "uh"]),
        _sel("201205_painel", ["unnamed_0", "unnamed_1"]),
    ]
    mapa = {"201205_painel": [dh.Regra("excluir", 0, "relatorio_formatado")]}
    por_nome = {r["arquivo"]: r for r in dh.planejar(selecionados, mapa, familias)}
    assert (por_nome["201204_entrada_bom"]["familia"], por_nome["201205_painel"]["status"]) == (
        "entrada",
        "excluido_formatado",
    )
    with pytest.raises(dh.ErroHistorico, match="fora do mapa"):
        dh.planejar(selecionados, {}, familias)
    with pytest.raises(dh.ErroHistorico, match="sem família"):
        dh.planejar([_sel("201204_outro", ["uf"])], {}, familias)


def test_mapa_de_cabecalhos(dh):
    mapa = pd.read_csv(
        io.StringIO(
            "arquivo,bloco,acao,linha,fim,colunas,motivo\n"
            "a,relatorio_1,promover,0,38,,\n"
            "a,relatorio_2,promover,39,,,\n"
            "b,,dividir,,,x|y,colado\n"
        ),
        dtype=str,
        keep_default_na=False,
    )
    regras = dh.ler_mapa(mapa)
    assert [(r.bloco, r.linha, r.fim) for r in regras["a"]] == [
        ("relatorio_1", 0, 38),
        ("relatorio_2", 39, None),
    ]
    assert regras["b"][0].colunas == ["x", "y"]
    with pytest.raises(dh.ErroHistorico, match="sem colunas"):
        dh.ler_mapa(mapa.assign(colunas=""))


def test_cleansed_por_familia_com_log_e_descartes(dh, tmp_path):
    lake = _lake_raw()
    dh.etapa_staging(lake, tmp_path / "s")
    dh.etapa_analytics(lake, tmp_path / "a")
    lake.objetos[dh.CLEANSED + "saida_de_antes.parquet"] = b"x"
    lake.objetos[dh.CLEANSED + "_001_saida_que_comeca_com_sublinhado.parquet"] = b"x"
    lake.objetos[dh.EVOLUCAO] = b"controle"
    resumo = dh.etapa_cleansed(lake, tmp_path / "c")
    assert (resumo["saidas"], resumo["familias"], resumo["linhas_reais"]) == (2, 2, 2)
    entrada = lake.tabela(dh.CLEANSED + "entrada/bb_2013_06_junho_x_entrada_20130618.parquet")
    assert entrada.column("uf").to_pylist() == ["SP"]
    controle = lake.tabela(dh.CLEANSED + "controle/bb_2013_06_junho_x_controle.parquet")
    assert controle.column("resp_arq").to_pylist() == ["***"]
    assert dh.CLEANSED + "saida_de_antes.parquet" not in lake.objetos
    assert dh.CLEANSED + "_001_saida_que_comeca_com_sublinhado.parquet" not in lake.objetos
    assert lake.objetos[dh.EVOLUCAO] == b"controle"
    log_ = lake.tabela(dh.LOG_CLEANSED)
    assert log_.schema == dh.SCHEMA_CLEANSED
    linhas = {r["saida"]: r for r in log_.to_pylist()}
    assert linhas["bb_2013_06_junho_x_entrada_20130618"]["competencia"] == "2013-06"
    assert linhas["bb_2013_06_junho_x_entrada_20130618"]["descartes_por_motivo"] == [
        ("copia_cabecalho", 1)
    ]
    assert lake.tabela(dh.DESCARTES_CLEANSED).num_rows == 1


# ---------------------------------------------------------------------------------------
# 4. colunas


def test_radical_e_conceito(dh):
    assert (
        dh.radical("icodigo_empreendimento") == dh.radical("cod_empreendimento") == "empreendimento"
    )
    assert dh.radical("vnome_municipio") == "municipio"
    assert dh.radical("valor_contratado") == "contratado"
    assert [dh.conceito(c) for c in ("cod_apf", "codapf", "nu_apf", "csigla_uf", "cnpj_bb")] == [
        "apf",
        "apf",
        "apf",
        "uf",
        "cnpj",
    ]


def _log_colunas(arquivos):
    return pd.DataFrame(
        [
            {
                "status": "tratado",
                "familia": "f",
                "saida": s,
                "competencia": c,
                "colunas": cols,
                "cleansed_key": s,
            }
            for s, c, cols in arquivos
        ]
    )


def test_renomeacao_provada_pelos_valores(dh):
    log_ = _log_colunas(
        [
            ("a1", "2015-01", ["cod_apf", "qtd_uh", "obs"]),
            ("a2", "2015-02", ["cod_apf", "qtd_uh", "obs"]),
            ("b1", "2016-01", ["nu_apf", "unidades", "situacao"]),
        ]
    )
    valores = {
        ("a2", "cod_apf"): ["100", "200", "300"],
        ("a2", "qtd_uh"): ["10", "20", "30"],
        ("a2", "obs"): ["x", "y", "z"],
        ("b1", "nu_apf"): ["100", "200", "301"],
        ("b1", "unidades"): ["10", "20", "30.0"],
        ("b1", "situacao"): ["ok", "ok", "ok"],
    }
    trocas = dh.trocas_de_layout(log_)
    assert [(t["antes"].saida, t["depois"].saida) for t in trocas] == [("a2", "b1")]
    cand = dh.candidatos_renomeacao(trocas, lambda reg, col: pd.Series(valores[(reg.saida, col)]))
    provaveis = set(map(tuple, cand[cand.provavel][["coluna_antes", "coluna_depois"]].values))
    assert provaveis == {("cod_apf", "nu_apf"), ("qtd_uh", "unidades")}
    conceitos = dh.conceitos_colunas(dh.evolucao_colunas(log_), cand).set_index("coluna")
    # o glossário dá o nome; sem glossário, vale o nome mais recente
    assert (
        conceitos.loc["cod_apf", "nome_canonico"]
        == conceitos.loc["nu_apf", "nome_canonico"]
        == "apf"
    )
    assert conceitos.loc["qtd_uh", "nome_canonico"] == "quantidade_uh"
    assert conceitos.loc["obs", "nome_canonico"] == "obs"
    grafo = dh.grafo_mermaid(conceitos.reset_index())
    assert "## `apf`" in grafo and "cod_apf<br/>f<br/>2015-01 a 2015-02" in grafo


# ---------------------------------------------------------------------------------------
# 3a. padronização, duplicadas e versões


def _padronizada(dh, coluna, valores):
    serie, contagem, _ = dh.padronizar_coluna(coluna, pd.Series(valores, dtype=object))
    return serie.tolist(), contagem


def test_padroniza_numeros_sem_mudar_o_valor(dh):
    assert _padronizada(dh, "qtd", ["56020.0", "52036.0", ""]) == (
        ["56020", "52036", ""],
        {"numero_ponto_zero": 2},
    )
    # vírgula decimal e ponto de milhar (lote do BB) viram ponto decimal
    assert _padronizada(dh, "vlr", ["206.500,00", "1.234,5", "3"])[0] == ["206500", "1234.5", "3"]
    # ruído de float32 e float64; decimal de verdade fica
    assert _padronizada(dh, "pct", ["97.83000183105469", "14943.216799999999", "0.5"])[0] == [
        "97.83",
        "14943.2168",
        "0.5",
    ]


def test_numero_ambiguo_ou_codigo_fica_como_esta(dh):
    # mistura vírgula e ponto decimal no mesmo arquivo: não dá para saber
    assert _padronizada(dh, "x", ["1,5", "2.5", "3"])[0] == ["1,5", "2.5", "3"]
    # "1.000" sem nenhum outro decimal: mil ou um?
    assert _padronizada(dh, "x", ["1.000", "250", "12"])[0] == ["1.000", "250", "12"]
    # código com zero à esquerda: só sai o ".0"
    assert _padronizada(dh, "cep", ["0012", "0034.0", "1200"])[0] == ["0012", "0034", "1200"]


def test_padroniza_datas_e_esvazia_a_que_nao_existe(dh):
    valores, contagem = _padronizada(
        dh,
        "dt",
        [
            "2019-2-28",
            "31/10/2024",
            "2010-11-10 00:00:00",
            "2016-06-31",
            "00:00:00",
            "2024-06-20 13:45:00",
        ],
    )
    assert valores == ["2019-02-28", "2024-10-31", "2010-11-10", "", "", "2024-06-20 13:45:00"]
    assert contagem == {"data": 3, "data_invalida": 1, "nulo": 1}


def test_padroniza_texto_mojibake_nulo_e_cnpj(dh):
    quebrado = "CONDOMÍNIO".encode("utf-8").decode("latin-1")
    valores, contagem = _padronizada(
        dh, "nome", [" BEM VIVER II   ", "SÃO PAULO", quebrado, "NULL", "-"]
    )
    assert valores == ["BEM VIVER II", "SÃO PAULO", "CONDOMÍNIO", "", ""]
    assert contagem == {"espaco": 1, "mojibake": 1, "nulo": 2}
    assert _padronizada(dh, "inumero_cnpj", ["2219108000103.0", "21049915000161"])[0] == [
        "02219108000103",
        "21049915000161",
    ]


def test_padronizar_registra_e_confere_as_celulas(dh):
    df = pd.DataFrame(
        {"a": ["1.0", "2", "3"], "b": ["y", " x ", "NULL"], "_linha_origem": [0, 1, 2]}
    )
    saida, registros = dh.padronizar(df)
    assert (saida.a.tolist(), saida.b.tolist()) == (["1", "2", "3"], ["y", "x", ""])
    # coluna com texto demais não é tratada como número
    assert dh.padronizar(df.assign(a=["1.0", "x", "z"]))[0].a.tolist() == ["1.0", "x", "z"]
    assert {(r["regra"], r["celulas"]) for r in registros} == {
        ("numero_ponto_zero", 1),
        ("espaco", 1),
        ("nulo", 1),
    }


def test_linhas_zeradas_so_com_regra_no_mapa(dh):
    cols = ["nome", "produto", "uh", "valor"]
    # a última tem um valor sem identificação: é informação, fica
    linhas = [
        ["", "CCFGTS", "0", "0"],
        ["RES X", "CCFGTS", "64", "100"],
        ["", "", "0", "3667432.96"],
    ]
    df = _df(linhas, cols).assign(_linha_origem=[0, 1, 2])
    dados, desc = dh.separar_descartes(df, [cols], zeradas=True)
    assert (dados.nome.tolist(), desc.motivo.tolist()) == (["RES X", ""], ["zerada"])
    assert len(dh.separar_descartes(df, [cols])[0]) == 3  # sem a regra, fica


def test_duplicada_identica_sai(dh):
    df = pd.DataFrame({"apf": ["1", "1", "2"], "uh": ["5", "5", "5"], "_linha_origem": [0, 1, 2]})
    dados, desc = dh.remover_duplicadas(df)
    assert (dados.apf.tolist(), desc.motivo.tolist(), desc._linha_origem.tolist()) == (
        ["1", "2"],
        ["duplicada"],
        [1],
    )


def _reg_versao(saida, competencia, hash_, corte=None, colunas=("a",)):
    return {
        "saida": saida,
        "familia": "f",
        "status": "tratado",
        "competencia": competencia,
        "data_corte": corte,
        "hash_conteudo": hash_,
        "colunas": list(colunas),
    }


def test_escolhe_a_versao_corrigida_e_tira_o_conteudo_repetido(dh):
    regs = [
        _reg_versao("x_20121031", "2012-10", "h1", "2012-10-31"),
        _reg_versao("x_20121031_ajustada", "2012-10", "h1", "2012-10-31"),  # só formato
        _reg_versao("y_30042015", "2015-04", "h2"),
        _reg_versao("y_30042015_v2", "2015-04", "h3"),  # valores corrigidos
        _reg_versao("z_min_cidades_pj", "2015-02", "h4"),
        _reg_versao("z_min_cidades_pj_2015_02", "2015-02", "h5"),  # acentuação quebrada
        _reg_versao("w_mai", "2017-05", "h6"),
        _reg_versao("w_jul", "2017-07", "h6"),  # reenvio de maio
        _reg_versao("k_a", "2016-01", "h7"),
        _reg_versao("k_b", "2016-01", "h8"),  # sem critério: ficam os dois
    ]
    assert dh.escolher_versoes(regs) == 4
    por = {
        r["saida"]: (r["status"], r.get("substituida_por"), r.get("motivo_substituicao"))
        for r in regs
    }
    assert por["x_20121031"] == ("versao_substituida", "x_20121031_ajustada", "conteudo_identico")
    assert por["y_30042015"] == ("versao_substituida", "y_30042015_v2", "versao_corrigida")
    assert por["z_min_cidades_pj_2015_02"][0] == "versao_substituida"
    assert por["w_jul"] == ("versao_substituida", "w_mai", "conteudo_identico")
    assert por["k_a"][0] == por["k_b"][0] == "tratado"
    assert (
        regs[0]["cleansed_key"]
        == "tests/cleansed/dados_historicos/_versoes_substituidas/f/x_20121031.parquet"
    )


def test_cleansed_padroniza_e_guarda_a_versao_substituida(dh, tmp_path):
    lake = _lake_raw()
    # a mesma entrada, numa versão "_v2" que só difere no ".0"
    lake.objetos["raw/dados_historicos/bb_2013_06_junho_x_entrada_20130618_v2.csv"] = _csv(
        [["UF", "UH"], ["SP", "10.0"], ["UF", "UH"], ["UF", "UH"], ["SP", "10.0"]]
    )
    lake.objetos[dh.MAPA_FAMILIAS] = FAMILIAS.replace(b"_entrada_,", b"_entrada_\\d+(_v2)?$,")
    dh.etapa_staging(lake, tmp_path / "s")
    dh.etapa_analytics(lake, tmp_path / "a")
    resumo = dh.etapa_cleansed(lake, tmp_path / "c")
    assert (resumo["saidas"], resumo["versoes_substituidas"]) == (2, 1)
    assert resumo["padronizacoes"] == {"numero_ponto_zero": 1}
    log_ = {r["saida"]: r for r in lake.tabela(dh.LOG_CLEANSED).to_pylist()}
    v2 = log_["bb_2013_06_junho_x_entrada_20130618_v2"]
    assert (v2["status"], v2["motivo_substituicao"]) == ("tratado", None)
    original = log_["bb_2013_06_junho_x_entrada_20130618"]
    assert (original["status"], original["substituida_por"]) == (
        "versao_substituida",
        "bb_2013_06_junho_x_entrada_20130618_v2",
    )
    assert original["cleansed_key"] in lake.objetos  # não some: fica fora da pasta da família
    assert lake.tabela(dh.PADRONIZACOES).num_rows == 1
