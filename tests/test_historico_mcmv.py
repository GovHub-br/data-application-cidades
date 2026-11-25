"""Tratamento dos dados históricos (helpers/historico_mcmv/): funções puras, sem S3."""

import io
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from historico_mcmv import (
    analytics,
    cleansed,
    colunas,
    datas,
    erros,
    estrutura,
    layout,
    nomes,
    padronizacao,
    staging,
    validacao,
)

HOJE = date(2026, 11, 14)


FAMILIAS = (
    b"familia,padrao,fonte,descricao\n"
    b"entrada,_entrada_,bb,Entrada\n"
    b"controle,_controle$,bb,Controle\n"
)


def _parquet_staging(path, dados):
    n = len(next(iter(dados.values())))
    meta = {b"source_file": b"raw/x.csv"}
    campos = [pa.field(c, pa.string()) for c in [*dados, *["_source_file"]]]
    tabela = pa.table(
        {**dados, "_source_file": ["raw/x.csv"] * n}, schema=pa.schema(campos, metadata=meta)
    )
    pq.write_table(tabela, path)


# ---------------------------------------------------------------------------------------
# 2. analytics


def _parquet(path, dados):
    dados = {**dados, "_source_file": ["x"] * len(next(iter(dados.values()), []))}
    pq.write_table(pa.table(dados), path)
    return path


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


# ---------------------------------------------------------------------------------------
# 3. tratamento estrutural


def _df(linhas, colunas=None):
    colunas = colunas or [f"unnamed_{i}" for i in range(len(linhas[0]))]
    return pd.DataFrame(linhas, columns=colunas, dtype=str)


def _2x(linhas):
    return linhas + linhas[::-1]  # a exportação do dump: tudo 2x, a 2ª cópia em outra ordem


def _sem_origem(df):
    return [c for c in df.columns if c != "_linha_origem"]


def _reg_data(arquivo, contrato=None, declarada=None, coluna=None):
    return {
        "arquivo": arquivo,
        "contrato_mais_novo": contrato,
        "data_declarada": declarada,
        "data_declarada_coluna": coluna,
        "data_declarada_distintas": 1 if declarada else 0,
    }


# ---------------------------------------------------------------------------------------
# 3c. cleansed


def _sel(arquivo, colunas):
    return {"arquivo": arquivo, "analytics_key": f"a/{arquivo}.parquet", "colunas": colunas}


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


# ---------------------------------------------------------------------------------------
# 3a. padronização, duplicadas e versões


def _padronizada(coluna, valores):
    serie, contagem, _ = padronizacao.padronizar_coluna(coluna, pd.Series(valores, dtype=object))
    return serie.tolist(), contagem


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


# ---------------------------------------------------------------------------------------
# 1. staging


def test_normalizacao_de_nomes_e_a_do_lake():
    # mesma regra do resto do lake, sem o código depender de scripts/
    from lake_utils import corrigir_mojibake_texto, normalize_columns

    amostra = ["Município", "municÃ\xadpio", "% Meta R$", "", "UF", "UF", "\ufeffCódigo"]
    assert nomes.normalizar_colunas(amostra) == normalize_columns(amostra)
    for t in amostra + ["SÃO PAULO", "José", "Ã"]:
        assert nomes.corrigir_mojibake(t) == corrigir_mojibake_texto(t)


def test_chave_espelha_a_raw():
    assert staging.chave_staging("snh/raw/dados_historicos/bb_2012_entrada.csv") == (
        "snh/staging/historico_mcmv/convertido/bb_2012_entrada.parquet"
    )
    with pytest.raises(erros.ErroHistorico):
        staging.chave_staging("raw/sftp/x.csv")


def test_conversao_nao_tira_nenhuma_linha(tmp_path):
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
    info = staging.converter_csv(src, tmp_path / "t.parquet", "snh/raw/dados_historicos/t.csv")
    tabela = pq.read_table(tmp_path / "t.parquet").to_pandas()
    assert info["n_linhas"] == len(tabela) == 6
    assert info["encoding"] == "utf-8"
    assert list(tabela.columns) == ["fase_do_pmcmv", "municipio", "valor", *staging.LINHAGEM]
    assert tabela["valor"].tolist() == ["", "1.000,50", "1.000,50", "", "", "0012"]
    assert tabela["municipio"].iloc[-1] == "CAMARAGIBE; PE"
    assert (tabela["_source_file"] == "snh/raw/dados_historicos/t.csv").all()


def test_so_cabecalho_e_sem_coluna_viram_parquet_vazio(tmp_path):
    (tmp_path / "h.csv").write_text("a;b\n", encoding="utf-8")
    info = staging.converter_csv(tmp_path / "h.csv", tmp_path / "h.parquet", "r/h.csv")
    assert info["n_linhas"] == 0
    assert pq.read_table(tmp_path / "h.parquet").column_names[:2] == ["a", "b"]
    (tmp_path / "s.csv").write_text("\n", encoding="utf-8")
    info = staging.converter_csv(tmp_path / "s.csv", tmp_path / "s.parquet", "r/s.csv")
    assert info["n_colunas"] == 0
    assert pq.read_table(tmp_path / "s.parquet").column_names == staging.LINHAGEM


def test_retomada_pula_so_o_que_ja_bate():
    ok = {"status": "ok", "raw_etag": "abc"}
    assert staging.ja_convertido(ok, "abc", True)
    assert not staging.ja_convertido(ok, "abc", False)
    assert not staging.ja_convertido(ok, "outro", True)
    assert not staging.ja_convertido({"status": "erro", "raw_etag": "abc"}, "abc", True)
    assert not staging.ja_convertido(None, "abc", True)


def test_categorias_de_dado_pessoal():
    pf = ["cod_cpf", "txt_nome_benef", "txt_endereco_und_alien", "vlr_renda", "dte_nascimento"]
    assert staging.colunas_pii(pf) == {
        "cod_cpf": "cpf",
        "txt_nome_benef": "nome",
        "txt_endereco_und_alien": "endereco",
    }
    # endereço de empreendimento (sem indicador de pessoa física) não é dado pessoal
    obra = ["endereco_do_empreendimento", "nome_do_empreendimento", "cnpj_proponente"]
    assert staging.colunas_pii(obra) == {}
    assert staging.colunas_pii(["resp_arq", "tel_resp_arq", "e_mail_resp_arq", "data"]).keys() == {
        "resp_arq",
        "tel_resp_arq",
        "e_mail_resp_arq",
    }


def test_mascara_redige_nome_e_mantem_cpf_em_hash(tmp_path):
    p = tmp_path / "b.parquet"
    _parquet_staging(
        p,
        {
            "cod_cpf": ["a1b2c3d4e5f6", "a1b2c3d4e5f6", ""],
            "txt_nome_benef": ["FULANA", "FULANA", ""],
            "vlr_renda": ["100", "100", "50"],
        },
    )
    assert staging.mascarar_parquet(p) == ["txt_nome_benef"]
    t = pq.read_table(p)
    assert t.column("txt_nome_benef").to_pylist() == ["***", "***", ""]  # vazio continua vazio
    assert t.column("cod_cpf").to_pylist() == ["a1b2c3d4e5f6", "a1b2c3d4e5f6", ""]
    assert t.schema.metadata[b"source_file"] == b"raw/x.csv"


def test_cpf_em_claro_para_tudo(tmp_path):
    p = tmp_path / "c.parquet"
    _parquet_staging(p, {"cod_cpf": ["12345678901"], "uf": ["SP"]})
    with pytest.raises(erros.ErroDadoPessoal):
        staging.mascarar_parquet(p)


def test_mascara_campo_dentro_da_coluna_colada(tmp_path):
    # o nome da coluna colada é o cabeçalho grudado e casa com "_resp_": não pode virar
    # "***" inteira (o dividir do cleansed perderia os campos)
    p = tmp_path / "d.parquet"
    _parquet_staging(
        p, {"datacnpj_bbresp_arqtel_resp_arq": ["2013-06-17|0001|FULANO|2013-06-17", "x|y"]}
    )
    campos = ["data", "cnpj_bb", "resp_arq", "data_pos_arq"]
    assert staging.mascarar_parquet(p, campos) == ["datacnpj_bbresp_arqtel_resp_arq:resp_arq"]
    # a linha torta fica como está: o dividir do cleansed recusa o arquivo
    assert pq.read_table(p).column(0).to_pylist() == ["2013-06-17|0001|***|2013-06-17", "x|y"]


def test_periodo_do_nome():
    assert analytics.periodo("bb_2012_10_outubro_entrada_bb_20121023") == "201210"
    assert (
        analytics.periodo("historico_recente_202406_snh_pmcmv_dados_prioritarios_af_bb") == "202406"
    )
    assert analytics.periodo("caixa_002_2018_bext_mai18") == "201805"
    assert analytics.periodo("caixa_003_2017_bext_jan_2017") == "201701"
    assert analytics.periodo("caixa_001_2016_bext_31102016") == "201610"
    assert analytics.periodo("caixa_002_2018_novo_relat_rio_executivo") == "201800"
    assert analytics.periodo("sem_data") == ""


def test_hash_ignora_ordem_das_linhas_e_nome_das_colunas(tmp_path):
    a = analytics.metricas_parquet(
        _parquet(tmp_path / "a.parquet", {"x": ["1", "2"], "y": ["a", ""]})
    )
    b = analytics.metricas_parquet(
        _parquet(tmp_path / "b.parquet", {"p": ["2", "1"], "q": ["", "a"]})
    )
    c = analytics.metricas_parquet(
        _parquet(tmp_path / "c.parquet", {"x": ["1", "2"], "y": ["a", "b"]})
    )
    assert a["hash_conteudo"] == b["hash_conteudo"] != c["hash_conteudo"]
    d = analytics.metricas_parquet(
        _parquet(tmp_path / "d.parquet", {"x": ["1", "2"] * 2, "y": ["a", ""] * 2})
    )
    assert d["hash_conteudo"] != a["hash_conteudo"]  # o dobro de linhas não é cópia
    e = analytics.metricas_parquet(
        _parquet(tmp_path / "e.parquet", {"x": ["1", "  ", ""], "y": ["", "", "z"]})
    )
    assert (e["n_linhas"], e["n_colunas"], e["celulas_preenchidas"]) == (3, 2, 2)


def test_classificar():
    modelo = sorted(analytics.COLUNAS_MODELO_2009)
    regs = analytics.classificar(
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


def test_duplicacao_mantem_repeticao_legitima_e_ordem():
    unico = estrutura.desfazer_duplicacao(_df(_2x([["a"], ["b"], ["c"], ["b"]]), ["x"]))
    assert unico.x.tolist() == ["a", "b", "c", "b"]
    assert unico._linha_origem.tolist() == [0, 1, 2, 3]
    with pytest.raises(erros.ErroHistorico, match="ímpar"):
        estrutura.desfazer_duplicacao(_df([["a"], ["a"], ["b"]], ["x"]))


def test_promover_com_titulo_e_copia_do_cabecalho():
    linhas = [
        ["Posição: 21/11/2011", "", "", ""],
        ["UF", "Município", "UH", "Valor"],
        ["SP", "Americana", "10", "1.000,50"],
        ["RJ", "Niterói", "5", "20"],
    ]
    res = estrutura.tratar_arquivo(_df(_2x(linhas)), estrutura.Regra("promover", 1))
    assert _sem_origem(res.dados) == ["uf", "municipio", "uh", "valor"]
    assert res.dados.uf.tolist() == ["SP", "RJ"]
    assert res.por_motivo == {"titulo": 1, "cabecalho": 1}


def test_compor_grupo_mesclado_e_centralizado():
    linhas = [
        ["", "Desligamento", "", "", "Aprovado CEF", "", "Contratação total", ""],
        ["UF", "Unidades", "Valor", "Unidades", "Valor", "Meta", "Unidades", ""],
        ["AC", "1", "2", "3", "4", "5", "6", ""],
    ]
    res = estrutura.tratar_arquivo(_df(_2x(linhas)), estrutura.Regra("compor", 0))
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


def test_descartes_vazia_subtitulo_total_rodape():
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
    res = estrutura.tratar_arquivo(_df(_2x(linhas), cols))
    assert res.dados.codigo.tolist() == ["1", "", "3"]
    assert res.por_motivo == {"subtitulo": 1, "vazia": 1, "total": 2, "rodape": 1}


def test_coluna_vazia_sem_nome_sai_e_com_nome_fica():
    df = _df(_2x([["a", "", "", "1"], ["b", " ", "", "2"]]), ["x", "unnamed_1", "observacoes", "y"])
    res = estrutura.tratar_arquivo(df)
    assert (res.colunas_removidas, res.colunas_vazias) == (["unnamed_1"], ["observacoes"])


def test_conferencia_pega_perda():
    df = _df(_2x([["a", "1"], ["b", "2"]]), ["x", "y"])
    res = estrutura.tratar_arquivo(df)
    res.dados = res.dados.iloc[:1]
    with pytest.raises(erros.ErroHistorico, match="linhas não fecham"):
        estrutura.conferir(df, [res])


def test_blocos_empilhados():
    linhas = [
        ["UF", "Unidades", ""],
        ["AC", "10", ""],
        ["Relatório 2: por UF e Público Alvo", "", ""],
        ["UF", "Público Alvo", "Valor"],
        ["AC", "0 A 3 SM", "5"],
    ]
    regras = [
        estrutura.Regra("promover", 0, bloco="relatorio_1", fim=2),
        estrutura.Regra("promover", 3, bloco="relatorio_2"),
    ]
    (b1, r1), (b2, r2) = estrutura.tratar_blocos(_df(_2x(linhas)), regras)
    assert (b1, _sem_origem(r1.dados)) == ("relatorio_1", ["uf", "unidades"])
    assert _sem_origem(r2.dados) == ["uf", "publico_alvo", "valor"]
    with pytest.raises(erros.ErroHistorico, match="fim"):
        estrutura.tratar_blocos(
            _df(_2x(linhas)), [estrutura.Regra("promover", 0), estrutura.Regra("promover", 3)]
        )


def test_nomear_e_dividir():
    df = _df(_2x([["466787", "FAR"], ["466788", "FDS"]]), ["600182", "far"])
    res = estrutura.tratar_arquivo(
        df, estrutura.Regra("nomear", colunas=["cod_empreendimento", "modalidade"])
    )
    assert res.dados.cod_empreendimento.tolist() == ["466787", "466788"]
    colado = _df(_2x([["389013|403204|0|2012-10-19"], ["1|2||2013-01-01"]]), ["cod"])
    nomes = ["cod_contrato_pj", "cod_operacao", "cod_sit", "dte_assin"]
    res = estrutura.tratar_arquivo(colado, estrutura.Regra("dividir", colunas=nomes))
    assert res.dados.iloc[1].tolist()[:4] == ["1", "2", "", "2013-01-01"]
    with pytest.raises(erros.ErroHistorico, match="campos"):
        estrutura.tratar_arquivo(
            _df(_2x([["a|b|c|d"], ["a|b"]]), ["x"]), estrutura.Regra("dividir", colunas=nomes)
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
def test_data_do_nome(nome, esperado):
    assert datas.data_do_nome(nome, HOJE) == esperado


def test_para_datas():
    lidas = datas.para_datas(["2019-2-28", "31/10/2024", "2010-11-10 00:00:00", "2016-06-31", ""])
    assert [None if pd.isna(d) else d.date() for d in lidas] == [
        date(2019, 2, 28),
        date(2024, 10, 31),
        date(2010, 11, 10),
        None,
        None,
    ]


def test_movimento_da_snh_vale_mais_que_o_nome():
    r = _reg_data(
        "historico_recente_202402_snh_af_bb", date(2018, 11, 28), "28/02/2025", "data_de_movimento"
    )
    datas.datar([r], HOJE)
    assert (r["competencia"], r["data_corte"]) == ("2025-02", date(2025, 2, 28))
    assert r["alertas_data"] == ["competencia_diferente_do_nome", "contrato_mais_novo_antigo"]


def test_lote_pelo_pf_pf_e_corte_nunca_antes_do_contrato():
    lote = [
        _reg_data("bb_2018_2018_04_06_pf_pf", date(2018, 4, 5)),
        _reg_data("bb_2018_2018_04_06_pj", date(2018, 3, 13)),
    ]
    datas.datar(lote, HOJE)
    assert {r["competencia"] for r in lote} == {"2018-03"}
    depois = [_reg_data("bb_2018_2018_10_30_pf_pf", date(2018, 11, 6))]
    datas.datar(depois, HOJE)
    assert (depois[0]["competencia"], depois[0]["data_corte"]) == ("2018-10", date(2018, 11, 6))
    meio = [_reg_data("bb_2015_03_marco_min_cidades_pf_pf", date(2015, 3, 13))]
    datas.datar(meio, HOJE)
    assert meio[0]["alertas_data"] == ["corte_no_meio_do_mes"]


def test_fora_de_lote_vale_o_nome():
    regs = [_reg_data("bb_2014_03_marco_entrada_bb_20140401"), _reg_data("sem_data")]
    datas.datar(regs, HOJE)
    assert (regs[0]["competencia"], regs[0]["alertas_data"]) == (
        "2014-04",
        ["pasta_diferente_do_nome"],
    )
    assert regs[1]["alertas_data"] == ["sem_competencia"]


def test_plano_familia_excluidos_e_cabecalho_ruim():
    familias = cleansed.ler_familias(pd.read_csv(io.BytesIO(FAMILIAS), dtype=str))
    selecionados = [
        _sel("201204_entrada_bom", ["uf", "uh"]),
        _sel("201205_painel", ["unnamed_0", "unnamed_1"]),
    ]
    mapa = {"201205_painel": [estrutura.Regra("excluir", 0, "relatorio_formatado")]}
    por_nome = {r["arquivo"]: r for r in cleansed.planejar(selecionados, mapa, familias)}
    assert (por_nome["201204_entrada_bom"]["familia"], por_nome["201205_painel"]["status"]) == (
        "entrada",
        "excluido_formatado",
    )
    with pytest.raises(erros.ErroHistorico, match="fora do mapa"):
        cleansed.planejar(selecionados, {}, familias)
    with pytest.raises(erros.ErroHistorico, match="sem família"):
        cleansed.planejar([_sel("201204_outro", ["uf"])], {}, familias)


def test_mapa_de_cabecalhos():
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
    regras = cleansed.ler_mapa(mapa)
    assert [(r.bloco, r.linha, r.fim) for r in regras["a"]] == [
        ("relatorio_1", 0, 38),
        ("relatorio_2", 39, None),
    ]
    assert regras["b"][0].colunas == ["x", "y"]
    with pytest.raises(erros.ErroHistorico, match="sem colunas"):
        cleansed.ler_mapa(mapa.assign(colunas=""))


# ---------------------------------------------------------------------------------------
# 4. colunas


def test_radical_e_conceito():
    assert (
        colunas.radical("icodigo_empreendimento")
        == colunas.radical("cod_empreendimento")
        == "empreendimento"
    )
    assert colunas.radical("vnome_municipio") == "municipio"
    assert colunas.radical("valor_contratado") == "contratado"
    assert [
        colunas.conceito(c) for c in ("cod_apf", "codapf", "nu_apf", "csigla_uf", "cnpj_bb")
    ] == [
        "apf",
        "apf",
        "apf",
        "uf",
        "cnpj",
    ]


def test_renomeacao_provada_pelos_valores():
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
    trocas = colunas.trocas_de_layout(log_)
    assert [(t["antes"].saida, t["depois"].saida) for t in trocas] == [("a2", "b1")]
    cand = colunas.candidatos_renomeacao(
        trocas, lambda reg, col: pd.Series(valores[(reg.saida, col)])
    )
    provaveis = set(map(tuple, cand[cand.provavel][["coluna_antes", "coluna_depois"]].values))
    assert provaveis == {("cod_apf", "nu_apf"), ("qtd_uh", "unidades")}
    conceitos = colunas.conceitos_colunas(colunas.evolucao_colunas(log_), cand).set_index("coluna")
    # o glossário dá o nome; sem glossário, vale o nome mais recente
    assert (
        conceitos.loc["cod_apf", "nome_canonico"]
        == conceitos.loc["nu_apf", "nome_canonico"]
        == "apf"
    )
    assert conceitos.loc["qtd_uh", "nome_canonico"] == "quantidade_uh"
    assert conceitos.loc["obs", "nome_canonico"] == "obs"
    grafo = colunas.grafo_mermaid(conceitos.reset_index())
    assert "## `apf`" in grafo and "cod_apf<br/>f<br/>2015-01 a 2015-02" in grafo


def test_padroniza_numeros_sem_mudar_o_valor():
    assert _padronizada("qtd", ["56020.0", "52036.0", ""]) == (
        ["56020", "52036", ""],
        {"numero_ponto_zero": 2},
    )
    # vírgula decimal e ponto de milhar (lote do BB) viram ponto decimal
    assert _padronizada("vlr", ["206.500,00", "1.234,5", "3"])[0] == ["206500", "1234.5", "3"]
    # ruído de float32 e float64; decimal de verdade fica
    assert _padronizada("pct", ["97.83000183105469", "14943.216799999999", "0.5"])[0] == [
        "97.83",
        "14943.2168",
        "0.5",
    ]


def test_virgula_decimal_sem_parte_inteira():
    # INT040: ",00" e ",01" ao lado de "100,00"; sem a regra a coluna não chegava a 95%
    assert _padronizada("pct", ["100,00", ",00", ",01", "4,07"])[0] == [
        "100",
        "0",
        "0.01",
        "4.07",
    ]
    # ",01" numa coluna de ponto decimal: não pode ser milhar, então é decimal
    assert _padronizada("vlr", ["22500000", "5360361.17", ",01"])[0] == [
        "22500000",
        "5360361.17",
        "0.01",
    ]


def test_numero_ambiguo_ou_codigo_fica_como_esta():
    # mistura vírgula e ponto decimal no mesmo arquivo: não dá para saber
    assert _padronizada("x", ["1,5", "2.5", "3"])[0] == ["1,5", "2.5", "3"]
    # "1.000" sem nenhum outro decimal: mil ou um?
    assert _padronizada("x", ["1.000", "250", "12"])[0] == ["1.000", "250", "12"]
    # código com zero à esquerda: só sai o ".0"
    assert _padronizada("cep", ["0012", "0034.0", "1200"])[0] == ["0012", "0034", "1200"]


def test_moeda_e_milhar_br_inequivoco_em_coluna_mista():
    # SNH BB 2024-06: "R$" na frente e milhar BR numa coluna que também tem ponto decimal
    valores, contagem = _padronizada(
        "vlr", ["R$ 1.008.000,00", "R$ -", "38670473.58", "1.737.972,88", "1,5"]
    )
    # "1,5" segue ambíguo (vírgula decimal sem milhar ao lado de ponto decimal)
    assert valores == ["1008000", "", "38670473.58", "1737972.88", "1,5"]
    assert contagem == {"numero_decimal_br": 2, "nulo": 1}


def test_padroniza_datas_e_esvazia_a_que_nao_existe():
    valores, contagem = _padronizada(
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


def test_padroniza_texto_mojibake_nulo_e_cnpj():
    quebrado = "CONDOMÍNIO".encode("utf-8").decode("latin-1")
    valores, contagem = _padronizada(
        "nome", [" BEM VIVER II   ", "SÃO PAULO", quebrado, "NULL", "-"]
    )
    assert valores == ["BEM VIVER II", "SÃO PAULO", "CONDOMÍNIO", "", ""]
    assert contagem == {"espaco": 1, "mojibake": 1, "nulo": 2}
    assert _padronizada("inumero_cnpj", ["2219108000103.0", "21049915000161"])[0] == [
        "02219108000103",
        "21049915000161",
    ]


def test_padronizar_registra_e_confere_as_celulas():
    df = pd.DataFrame(
        {"a": ["1.0", "2", "3"], "b": ["y", " x ", "NULL"], "_linha_origem": [0, 1, 2]}
    )
    saida, registros = padronizacao.padronizar(df)
    assert (saida.a.tolist(), saida.b.tolist()) == (["1", "2", "3"], ["y", "x", ""])
    # coluna com texto demais não é tratada como número
    assert padronizacao.padronizar(df.assign(a=["1.0", "x", "z"]))[0].a.tolist() == [
        "1.0",
        "x",
        "z",
    ]
    assert {(r["regra"], r["celulas"]) for r in registros} == {
        ("numero_ponto_zero", 1),
        ("espaco", 1),
        ("nulo", 1),
    }


def test_linhas_zeradas_so_com_regra_no_mapa():
    cols = ["nome", "produto", "uh", "valor"]
    # a última tem um valor sem identificação: é informação, fica
    linhas = [
        ["", "CCFGTS", "0", "0"],
        ["RES X", "CCFGTS", "64", "100"],
        ["", "", "0", "3667432.96"],
    ]
    df = _df(linhas, cols).assign(_linha_origem=[0, 1, 2])
    dados, desc = estrutura.separar_descartes(df, [cols], zeradas=True)
    assert (dados.nome.tolist(), desc.motivo.tolist()) == (["RES X", ""], ["zerada"])
    assert len(estrutura.separar_descartes(df, [cols])[0]) == 3  # sem a regra, fica


def test_total_sem_rotulo_so_pela_linha_do_mapa():
    cols = ["apf", "municipio", "uh", "valor"]
    linhas = [["1", "A", "10", "5"], ["2", "B", "20", "7"], ["", "", "29", "12.5"]]
    df = _df(linhas, cols).assign(_linha_origem=[7, 8, 9])
    assert len(estrutura.separar_descartes(df, [cols])[0]) == 3  # sem rótulo e sem o mapa, fica
    dados, desc = estrutura.separar_descartes(df, [cols], totais=[9])
    assert (dados.apf.tolist(), desc.motivo.tolist()) == (["1", "2"], ["total"])


def test_total_do_mapa_que_nao_existe_para():
    linhas = [["apf", "uh"], ["1", "10"], ["2", "20"]]
    regra = estrutura.Regra(estrutura.PROMOVER, 0, totais=[99])
    with pytest.raises(erros.ErroHistorico, match="total do mapa"):
        estrutura.tratar_blocos(_df(_2x(linhas)), [regra])
    mapa = pd.read_csv(
        io.StringIO(
            "arquivo,bloco,acao,linha,fim,colunas,motivo,linhas_zeradas,linhas_total\n"
            "a,,manter,,,,total sem rótulo,,9075|9076\n"
        ),
        dtype=str,
        keep_default_na=False,
    )
    assert cleansed.ler_mapa(mapa)["a"][0].totais == [9075, 9076]


def test_duplicada_identica_sai():
    df = pd.DataFrame({"apf": ["1", "1", "2"], "uh": ["5", "5", "5"], "_linha_origem": [0, 1, 2]})
    dados, desc = padronizacao.remover_duplicadas(df)
    assert (dados.apf.tolist(), desc.motivo.tolist(), desc._linha_origem.tolist()) == (
        ["1", "2"],
        ["duplicada"],
        [1],
    )


def test_escolhe_a_versao_corrigida_e_tira_o_conteudo_repetido():
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
    assert cleansed.escolher_versoes(regs) == 4
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
        == "snh/staging/historico_mcmv/cleansed/_versoes_substituidas/f/x_20121031.parquet"
    )


# ---------------------------------------------------------------------------------------
# validação (pandera): cada contrato aceita o caso bom e barra o ruim


def _mapa(texto):
    return pd.read_csv(io.StringIO(texto), dtype=str, keep_default_na=False)


CABECALHOS_OK = (
    "arquivo,bloco,acao,linha,fim,colunas,motivo,linhas_zeradas,linhas_total\n"
    "a,relatorio_1,promover,0,38,,,,\n"
    "a,relatorio_2,promover,39,,,,,\n"
    "b,,dividir,,,x|y,colado,,\n"
    "c,,manter,,,,total sem rótulo,,9075|9076\n"
)


def test_mapa_de_cabecalhos_valido_passa():
    assert len(validacao.validar_mapa_cabecalhos(_mapa(CABECALHOS_OK))) == 4


@pytest.mark.parametrize(
    "troca, falha",
    [
        ("b,,dividir,,,x|y,", "b,,dividir,,,,"),  # dividir sem colunas
        ("c,,manter,", "c,,apagar,"),  # ação que não existe
        ("9075|9076", "9075;9076"),  # lista de linhas mal escrita
        ("a,relatorio_2,promover,39", "a,relatorio_1,promover,39"),  # bloco repetido
        ("a,relatorio_1,promover,0,38", "a,relatorio_1,promover,0,3a"),  # fim não inteiro
    ],
)
def test_mapa_de_cabecalhos_quebrado_para(troca, falha):
    with pytest.raises(erros.ErroHistorico, match="cabecalhos.csv"):
        validacao.validar_mapa_cabecalhos(_mapa(CABECALHOS_OK.replace(troca, falha)))


def test_mapa_de_familias():
    ok = "familia,padrao,chave,fonte,descricao\nexec,relatorio_exec,cod_apf|codapf,caixa,X\n"
    assert len(validacao.validar_mapa_familias(_mapa(ok))) == 1
    with pytest.raises(erros.ErroHistorico, match="regex"):
        validacao.validar_mapa_familias(_mapa(ok.replace("relatorio_exec", "relatorio_(exec")))
    with pytest.raises(erros.ErroHistorico, match="familias.csv"):
        validacao.validar_mapa_familias(_mapa(ok + ok.splitlines()[1] + "\n"))  # repetida


def test_saida_do_cleansed():
    boa = pd.DataFrame({"_linha_origem": [3, 4], "uf": ["SP", None], "cod_apf": ["1", "2"]})
    assert len(validacao.validar_saida(boa, "x")) == 2
    with pytest.raises(erros.ErroHistorico, match="saída x"):
        validacao.validar_saida(boa.assign(_linha_origem=[3, 3]), "x")  # linha repetida
    with pytest.raises(erros.ErroHistorico, match="saída x"):
        validacao.validar_saida(boa.rename(columns={"uf": "UF"}), "x")  # nome sem normalizar
    with pytest.raises(erros.ErroHistorico, match="saída x"):
        validacao.validar_saida(boa.assign(uf=[1.5, 2.0]), "x")  # valor tipado: é da silver


def test_log_do_analytics_coerente():
    log = pd.DataFrame(
        {
            "arquivo": ["a", "b"],
            "staging_key": [layout.STAGING + "a.parquet", layout.STAGING + "b.parquet"],
            "status": ["selecionado", "copia"],
            "analytics_key": [layout.ANALYTICS + "a.parquet", None],
            "copia_de": [None, "a"],
            "n_linhas": [1, 1],
            "n_colunas": [1, 1],
        }
    )
    assert len(validacao.validar_log_analytics(log)) == 2
    with pytest.raises(erros.ErroHistorico, match="selecionado"):
        validacao.validar_log_analytics(log.assign(analytics_key=[None, None]))


def test_log_do_cleansed_pasta_da_familia():
    log = pd.DataFrame(
        {
            "saida": ["s1", "s2", "s3"],
            "status": ["tratado", "versao_substituida", "excluido_formatado"],
            "familia": ["entrada", "entrada", None],
            "cleansed_key": [
                layout.CLEANSED + "entrada/s1.parquet",
                layout.SUBSTITUIDAS + "entrada/s2.parquet",
                None,
            ],
            "competencia": ["2013-06", "2013-06", None],
            "linhas_entrada": [4.0, 4.0, None],
            "linhas_reais": [2.0, 2.0, None],
        }
    )
    assert len(validacao.validar_log_cleansed(log)) == 3
    errado = log.copy()
    errado.loc[0, "cleansed_key"] = layout.CLEANSED + "controle/s1.parquet"
    with pytest.raises(erros.ErroHistorico, match="pasta da família"):
        validacao.validar_log_cleansed(errado)
    with pytest.raises(erros.ErroHistorico, match="log do cleansed"):
        validacao.validar_log_cleansed(log.assign(competencia=["2013-13", "2013-06", None]))


def test_prefixo_das_camadas_pode_ser_desviado(monkeypatch):
    import importlib

    assert layout.BASE == "snh/staging/historico_mcmv/"  # o padrão é o oficial
    try:
        monkeypatch.setenv("HISTORICO_MCMV_PREFIXO", "tests/historico_mcmv/")
        importlib.reload(layout)
        assert layout.CLEANSED == "tests/historico_mcmv/cleansed/"
        assert layout.RAW == "snh/raw/dados_historicos/"  # a raw não muda
        monkeypatch.setenv("HISTORICO_MCMV_PREFIXO", "tests/sem_barra")
        with pytest.raises(ValueError, match="terminar em"):
            importlib.reload(layout)
    finally:
        monkeypatch.delenv("HISTORICO_MCMV_PREFIXO")
        importlib.reload(layout)
