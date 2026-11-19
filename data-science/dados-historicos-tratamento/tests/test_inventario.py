"""Testes do módulo de inventário.

Cobre identificar_frentes, extrair_periodo_dados, detectar_dimensoes,
calcular_score_utilidade e gerar_inventario.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from classificacao.inventario import (
    calcular_score_utilidade,
    detectar_dimensoes,
    extrair_periodo_dados,
    gerar_inventario,
    identificar_frentes,
)


# ---------------------------------------------------------------------------
# 6.2: identificar_frentes
# ---------------------------------------------------------------------------


def test_identificar_frentes_far_from_table_name() -> None:
    """'caixa_001_2012_far_empreendimentos' deve conter 'FAR'."""
    fronts = identificar_frentes("caixa_001_2012_far_empreendimentos")
    assert "FAR" in fronts


def test_identificar_frentes_pnhr_from_table_name() -> None:
    """Nome com 'pnhr' entre separadores não-underscore contém 'PNHR'."""
    fronts = identificar_frentes("bb-2015-pnhr-contratos")
    assert "PNHR" in fronts


def test_identificar_frentes_pf_pj_from_table_name() -> None:
    """'bb_2015_01_janeiro_2015_01_15_min_cidades_pj_pf' deve conter 'PF' e 'PJ'."""
    fronts = identificar_frentes("bb_2015_01_janeiro_2015_01_15_min_cidades_pj_pf")
    assert "PF" in fronts
    assert "PJ" in fronts


def test_identificar_frentes_no_front_in_name() -> None:
    """Nome sem keyword retorna conjunto vazio."""
    fronts = identificar_frentes("alguma_tabela_sem_keyword")
    assert fronts == set()


def test_identificar_frentes_from_column_values() -> None:
    """Coluna 'origem' com valores FAR e FGTS retorna ambos."""
    df = pd.DataFrame({"origem": ["FAR", "FAR", "FGTS"]})
    fronts = identificar_frentes("some_table", df)
    assert fronts == {"FAR", "FGTS"}


def test_identificar_frentes_product_column() -> None:
    """Coluna 'produto' com valores CCFGTS e FAR retorna ambos.
    Nota: 'CCFGTS' contém 'fgts' como substring, gerando falso-positivo 'FGTS'.
    """
    df = pd.DataFrame(
        {"produto": ["CCFGTS Im\u00f3vel na planta", "FAR Aliena\u00e7\u00e3o"]}
    )
    fronts = identificar_frentes("some_table", df)
    # 'CCFGTS' contém 'fgts' → FGTS também é detectado (substring match)
    assert "CCFGTS" in fronts
    assert "FAR" in fronts
    assert "FGTS" in fronts  # falso positivo de CCFGTS


def test_identificar_frentes_level3_bb_pf() -> None:
    """Tabela bb_ com _pf_ no nome recebe PF via Level 3."""
    fronts = identificar_frentes("bb_contratos_pf_2020")
    assert "PF" in fronts


def test_identificar_frentes_level3_caixa_far() -> None:
    """Tabela caixa_ com _001_ no nome recebe FAR via Level 3."""
    fronts = identificar_frentes("caixa_relatorio_001_2020")
    assert "FAR" in fronts


# ---------------------------------------------------------------------------
# 6.3: extrair_periodo_dados
# ---------------------------------------------------------------------------


def test_extrair_periodo_yyyy_mm_dd() -> None:
    """Coluna data_contratacao no formato YYYY-MM-DD extrai min e max."""
    df = pd.DataFrame(
        {
            "data_contratacao": ["2020-01-15", "2021-06-30", "2019-03-01"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    assert inicio == "2019-03-01"
    assert fim == "2021-06-30"


def test_extrair_periodo_dd_mm_yyyy() -> None:
    """Coluna dt_nascimento no formato DD/MM/YYYY extrai min e max."""
    df = pd.DataFrame(
        {
            "dt_nascimento": ["15/03/2010", "01/01/2005"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    assert inicio == "2005-01-01"
    assert fim == "2010-03-15"


def test_extrair_periodo_yyyymmdd() -> None:
    """Coluna dat_movimento no formato YYYYMMDD extrai min e max."""
    df = pd.DataFrame(
        {
            "dat_movimento": ["20200615", "20181201"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    assert inicio == "2018-12-01"
    assert fim == "2020-06-15"


def test_extrair_periodo_yyyymm() -> None:
    """Coluna ano_mes no formato YYYYMM extrai min e max (dia=01)."""
    df = pd.DataFrame(
        {
            "ano_mes": ["202001", "201812"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    assert inicio == "2018-12-01"
    assert fim == "2020-01-01"


def test_extrair_periodo_mixed_formats() -> None:
    """Múltiplas colunas com formatos diferentes retornam min/max global."""
    df = pd.DataFrame(
        {
            "data_iso": ["2020-06-15", "2021-01-01"],
            "dt_br": ["10/12/2019", "15/03/2022"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    # DD/MM/YYYY: 10/12/2019 -> 2019-12-10, 15/03/2022 -> 2022-03-15
    # YYYY-MM-DD: 2020-06-15, 2021-01-01
    # Min across both columns: 2019-12-10, Max: 2022-03-15
    assert inicio == "2019-12-10"
    assert fim == "2022-03-15"


def test_extrair_periodo_no_date_columns() -> None:
    """DataFrame sem colunas de data retorna (None, None)."""
    df = pd.DataFrame({"nome": ["Jo\u00e3o", "Maria"], "idade": ["30", "25"]})
    inicio, fim = extrair_periodo_dados(df)
    assert inicio is None
    assert fim is None


def test_extrair_periodo_skip_metadata_cols() -> None:
    """Metadados como source_table são ignorados; data_evento é considerada."""
    df = pd.DataFrame(
        {
            "source_table": ["t1", "t1"],
            "report_date": ["2020-01-01", "2020-06-01"],
            "data_evento": ["2021-01-01", "2022-06-15"],
        }
    )
    inicio, fim = extrair_periodo_dados(df)
    assert inicio == "2021-01-01"
    assert fim == "2022-06-15"


# ---------------------------------------------------------------------------
# 6.4: detectar_dimensoes
# ---------------------------------------------------------------------------


def test_detectar_dimensoes_all_present() -> None:
    """DataFrame com todas as dimensões retorna todas True e datas=3."""
    df = pd.DataFrame(
        {
            "situacao_obra": [],
            "dt_assinatura": [],
            "data_inicio": [],
            "data_fim": [],
            "percentual_obra_realizado": [],
            "unidades_concluidas": [],
            "valor_emprestimo": [],
            "vlr_subsidio": [],
            "municipio": [],
            "uf": [],
            "codigo_ibge": [],
            "nu_apf": [],
            "cod_contrato": [],
        }
    )
    dim = detectar_dimensoes(df)
    assert dim["status_obra"] is True
    assert dim["datas"] == 3
    assert dim["progresso"] is True
    assert dim["financeiro"] is True
    assert dim["geolocalizacao"] is True
    assert dim["granularidade"] is True


def test_detectar_dimensoes_none_present() -> None:
    """DataFrame sem colunas relevantes retorna todas False e datas=0."""
    df = pd.DataFrame({"nome": [], "idade": [], "cidade": []})
    dim = detectar_dimensoes(df)
    assert dim["status_obra"] is False
    assert dim["datas"] == 0
    assert dim["progresso"] is False
    assert dim["financeiro"] is False
    assert dim["geolocalizacao"] is False
    assert dim["granularidade"] is False


def test_detectar_dimensoes_geolocalizacao_municipio_and_uf() -> None:
    """municipio + uf -> geolocalizacao=True."""
    df = pd.DataFrame({"municipio": [], "uf": []})
    dim = detectar_dimensoes(df)
    assert dim["geolocalizacao"] is True


def test_detectar_dimensoes_geolocalizacao_only_municipio() -> None:
    """Apenas municipio sem uf -> geolocalizacao=False."""
    df = pd.DataFrame({"municipio": []})
    dim = detectar_dimensoes(df)
    assert dim["geolocalizacao"] is False


def test_detectar_dimensoes_geolocalizacao_codigo_ibge_and_uf() -> None:
    """codigo_ibge + uf -> geolocalizacao=True."""
    df = pd.DataFrame({"codigo_ibge": [], "uf": []})
    dim = detectar_dimensoes(df)
    assert dim["geolocalizacao"] is True


def test_detectar_dimensoes_case_insensitive() -> None:
    """Coluna DATA_ASSINATURA (maiúscula) deve ser detectada como datas."""
    df = pd.DataFrame({"DATA_ASSINATURA": []})
    dim = detectar_dimensoes(df)
    assert dim["datas"] >= 1


def test_detectar_dimensoes_datasets_count() -> None:
    """Colunas dt_ e data_ são contadas corretamente."""
    df = pd.DataFrame(
        {
            "dt_inicio": [],
            "data_fim": [],
            "dt_prevista": [],
            "nome": [],
        }
    )
    dim = detectar_dimensoes(df)
    assert dim["datas"] == 3


# ---------------------------------------------------------------------------
# 6.5: calcular_score_utilidade
# ---------------------------------------------------------------------------


def test_calcular_score_maximo() -> None:
    """Perfil colunar_denso com todas dimensões, sem penalidades -> Alta."""
    dim = {
        "status_obra": True,
        "datas": 3,
        "progresso": True,
        "financeiro": True,
        "geolocalizacao": True,
        "granularidade": True,
    }
    score, classificacao = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    assert score >= 8
    assert classificacao == "Alta"


def test_calcular_score_duplicate_penalty() -> None:
    """Duplicata perde 3 pontos em relação ao ideal."""
    dim = {
        "status_obra": True,
        "datas": 3,
        "progresso": True,
        "financeiro": True,
        "geolocalizacao": True,
        "granularidade": True,
    }
    score_ideal, _ = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    score_dup, _ = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=True,
        is_tratado=True,
    )
    assert score_dup == score_ideal - 3


def test_calcular_score_not_treated_penalty() -> None:
    """Não tratado perde 1 ponto em relação ao ideal."""
    dim = {
        "status_obra": True,
        "datas": 3,
        "progresso": True,
        "financeiro": True,
        "geolocalizacao": True,
        "granularidade": True,
    }
    score_ideal, _ = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    score_untreated, _ = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=False,
    )
    assert score_untreated == score_ideal - 1


def test_calcular_score_aggregated_penalty() -> None:
    """Perfil agregado_uf sofre -2, score menor que ideal por ao menos 2."""
    dim = {
        "status_obra": True,
        "datas": 3,
        "progresso": True,
        "financeiro": True,
        "geolocalizacao": True,
        "granularidade": True,
    }
    score_ideal, _ = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    score_aggr, _ = calcular_score_utilidade(
        "agregado_uf",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    assert score_ideal - score_aggr >= 2


def test_calcular_score_vazia() -> None:
    """Perfil vazia com dimensões vazias -> Nenhuma, score <= 1."""
    dim = {
        "status_obra": False,
        "datas": 0,
        "progresso": False,
        "financeiro": False,
        "geolocalizacao": False,
        "granularidade": False,
    }
    score, classificacao = calcular_score_utilidade(
        "vazia",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    assert classificacao == "Nenhuma"
    assert score <= 1


def test_calcular_score_lookup_penalty() -> None:
    """Perfil lookup sofre -3, resultando em Nenhuma."""
    dim = {
        "status_obra": False,
        "datas": 1,
        "progresso": False,
        "financeiro": False,
        "geolocalizacao": True,
        "granularidade": False,
    }
    score, classificacao = calcular_score_utilidade(
        "lookup",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    # geo +1, lookup -3 = -2
    assert score == -2
    assert classificacao == "Nenhuma"


def test_calcular_score_dados_sem_utilidade() -> None:
    """Perfil dados_sem_utilidade sofre -3."""
    dim = {
        "status_obra": False,
        "datas": 0,
        "progresso": False,
        "financeiro": False,
        "geolocalizacao": False,
        "granularidade": False,
    }
    score, classificacao = calcular_score_utilidade(
        "dados_sem_utilidade",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    assert score == -3
    assert classificacao == "Nenhuma"


def test_calcular_score_classificacao_media() -> None:
    """Score entre 5 e 7 -> Média."""
    dim = {
        "status_obra": True,
        "datas": 3,
        "progresso": True,
        "financeiro": False,
        "geolocalizacao": False,
        "granularidade": False,
    }
    score, classificacao = calcular_score_utilidade(
        "colunar_denso",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    # status +2, datas>=3 +2, progresso +2, colunar_denso +1 = 7
    assert 5 <= score <= 7
    assert classificacao == "M\u00e9dia"


def test_calcular_score_classificacao_baixa() -> None:
    """Score entre 2 e 4 -> Baixa."""
    dim = {
        "status_obra": True,
        "datas": 0,
        "progresso": False,
        "financeiro": True,
        "geolocalizacao": False,
        "granularidade": False,
    }
    score, classificacao = calcular_score_utilidade(
        "event_level",
        dim,
        is_duplicate=False,
        is_tratado=True,
    )
    # status +2, financeiro +1, event_level +1 = 4
    assert 2 <= score <= 4
    assert classificacao == "Baixa"


# ---------------------------------------------------------------------------
# 6.6: gerar_inventario
# ---------------------------------------------------------------------------


def test_gerar_inventario_basic(tmp_path: Path) -> None:
    """Inventário integrado com entradas tratadas, duplicata e não tratada."""
    # -- Setup directories ------------------------------------------------
    treated_dir = tmp_path / "treated"
    treated_dir.mkdir()

    # -- Treated CSV 1 (t1): tabela com dados relevantes ------------------
    t1_path = treated_dir / "t1_tratado.csv"
    t1_path.write_text(
        "uf\tmunicipio\tsituacao_obra\tdt_inicio\tdt_fim\t"
        "source_table\treport_date\tinstitution\tprofile\tcontent_hash\n"
        "SP\tSao Paulo\tEm andamento\t2020-01-01\t2020-12-31\t"
        "t1\t2020-01-01\tcaixa\tcolunar_denso\tabc123\n"
        "RJ\tRio de Janeiro\tConclu\u00edda\t2019-06-01\t2020-03-15\t"
        "t1\t2020-01-01\tcaixa\tcolunar_denso\tabc123\n"
        "MG\tBelo Horizonte\tParalisada\t2021-01-01\t2021-06-30\t"
        "t1\t2020-01-01\tcaixa\tcolunar_denso\tabc123\n"
    )

    # -- Treated CSV 2 (t2): tabela vazia sem dimensões úteis ------------
    t2_path = treated_dir / "t2_tratado.csv"
    t2_path.write_text(
        "nome\tidade\tsource_table\treport_date\tinstitution\tprofile\tcontent_hash\n"
        "Jo\u00e3o\t30\tt2\t2020-01-02\tcaixa\tvazia\tdef456\n"
        "Maria\t25\tt2\t2020-01-02\tcaixa\tvazia\tdef456\n"
    )

    # -- Quality report CSV (tab-separated) -------------------------------
    quality_path = tmp_path / "_quality_report.csv"
    quality_path.write_text(
        "table_name\tinstitution\tprofile\treport_date\tn_rows\tn_cols\t"
        "missing_pct\tencoding_issues\tdate_parse_errors\ttype_coercion_warnings\n"
        "t1\tcaixa\tcolunar_denso\t2020-01-01\t3\t10\t5.0\t0\t0\t0\n"
        "t2\tcaixa\tvazia\t2020-01-02\t2\t7\t10.0\t0\t0\t0\n"
    )

    # -- Dedup map CSV (comma-separated, como gerado por gerar_mapping) ---
    dedup_path = tmp_path / "_dedup_map.csv"
    dedup_path.write_text(
        "source_table,content_hash,canonical_table,is_duplicate\n"
        "t1,abc123,t2,True\n"
        "t2,def456,t2,False\n"
    )

    # -- Classificação CSV (comma-separated) ------------------------------
    classificacao_path = tmp_path / "classificacao_formacao.csv"
    classificacao_path.write_text(
        "table_name,formacao\nt1,bem_formada\nt2,bem_formada\nt3,sem_cabecalho\n"
    )

    output_path = tmp_path / "inventario.csv"

    # -- Execute ----------------------------------------------------------
    inventory_df = gerar_inventario(
        quality_path=str(quality_path),
        dedup_path=str(dedup_path),
        classificacao_path=str(classificacao_path),
        treated_dir=str(treated_dir),
        output_path=str(output_path),
    )

    # -- Assertions -------------------------------------------------------

    # 3 linhas: t1, t2, t3
    assert len(inventory_df) == 3

    # Colunas esperadas
    expected_columns = [
        "table_name",
        "formacao",
        "status_tratamento",
        "is_duplicate",
        "canonical_table",
        "frentes_cobertas",
        "instituicao",
        "perfil",
        "report_date",
        "periodo_dados_inicio",
        "periodo_dados_fim",
        "n_linhas",
        "n_colunas",
        "missing_pct",
        "encoding_issues",
        "date_parse_errors",
        "type_coercion_warnings",
        "score_utilidade_preditiva",
        "classificacao_utilidade",
        "tem_status_obra",
        "tem_datas",
        "tem_progresso",
        "tem_financeiro",
        "tem_geolocalizacao",
        "tem_granularidade_contrato",
        "campos_uteis_preditiva",
    ]
    assert list(inventory_df.columns) == expected_columns

    # -- t1: tratada, duplicata de t2 ------------------------------------
    t1_row = inventory_df[inventory_df["table_name"] == "t1"].iloc[0]
    assert t1_row["status_tratamento"] == "tratado"
    assert t1_row["is_duplicate"] == "True"
    assert t1_row["canonical_table"] == "t2"
    assert t1_row["instituicao"] == "caixa"
    assert t1_row["perfil"] == "colunar_denso"
    assert t1_row["report_date"] == "2020-01-01"
    assert t1_row["n_linhas"] == 3
    assert t1_row["n_colunas"] == 10
    # t1 tem dt_inicio e dt_fim; min=2019-06-01, max=2021-06-30
    assert t1_row["periodo_dados_inicio"] == "2019-06-01"
    assert t1_row["periodo_dados_fim"] == "2021-06-30"
    # tem_status_obra, tem_geolocalizacao, tem_datas devem estar ativos
    assert t1_row["tem_status_obra"] == "True"
    assert t1_row["tem_datas"] == 2
    assert t1_row["tem_progresso"] == "False"
    assert t1_row["tem_financeiro"] == "False"
    assert t1_row["tem_geolocalizacao"] == "True"
    assert t1_row["tem_granularidade_contrato"] == "False"

    # -- t2: tratada, não duplicata --------------------------------------
    t2_row = inventory_df[inventory_df["table_name"] == "t2"].iloc[0]
    assert t2_row["status_tratamento"] == "tratado"
    assert t2_row["is_duplicate"] == "False"
    assert t2_row["canonical_table"] == "t2"
    assert t2_row["perfil"] == "vazia"

    # -- t3: não tratada (não está no quality report) --------------------
    t3_row = inventory_df[inventory_df["table_name"] == "t3"].iloc[0]
    assert t3_row["status_tratamento"] == "nao_tratado"
    assert t3_row["is_duplicate"] == "False"
    assert t3_row["instituicao"] == ""
    assert t3_row["perfil"] == ""
    assert t3_row["n_linhas"] == 0
    assert t3_row["n_colunas"] == 0
    assert t3_row["missing_pct"] == 100.0

    # -- Output CSV existe e é tab-separated -----------------------------
    assert output_path.exists()
    raw = output_path.read_text(encoding="utf-8")
    first_line = raw.splitlines()[0]
    assert "\t" in first_line, "O CSV de inventário deve ser tab-separated"


def test_gerar_inventario_output_csv_readable(tmp_path: Path) -> None:
    """O CSV gerado por gerar_inventario pode ser lido de volta com sep='\\t'."""
    treated_dir = tmp_path / "treated"
    treated_dir.mkdir()

    # Tabela tratada mínima
    (treated_dir / "tx_tratado.csv").write_text(
        "nome\tsource_table\treport_date\tinstitution\tprofile\tcontent_hash\n"
        "A\ttx\t2020-01-01\tbb\tsem_cabecalho\th1\n"
    )

    quality_path = tmp_path / "_quality_report.csv"
    quality_path.write_text(
        "table_name\tinstitution\tprofile\treport_date\tn_rows\tn_cols\t"
        "missing_pct\tencoding_issues\tdate_parse_errors\ttype_coercion_warnings\n"
        "tx\tbb\tsem_cabecalho\t2020-01-01\t1\t6\t0.0\t0\t0\t0\n"
    )

    dedup_path = tmp_path / "_dedup_map.csv"
    dedup_path.write_text(
        "source_table,content_hash,canonical_table,is_duplicate\ntx,h1,tx,False\n"
    )

    classificacao_path = tmp_path / "classificacao_formacao.csv"
    classificacao_path.write_text("table_name,formacao\ntx,sem_cabecalho\n")

    output_path = tmp_path / "inventario.csv"

    gerar_inventario(
        quality_path=str(quality_path),
        dedup_path=str(dedup_path),
        classificacao_path=str(classificacao_path),
        treated_dir=str(treated_dir),
        output_path=str(output_path),
    )

    # Leitura de volta
    df_back = pd.read_csv(output_path, sep="\t")
    assert len(df_back) == 1
    assert df_back.iloc[0]["table_name"] == "tx"
    assert df_back.iloc[0]["status_tratamento"] == "tratado"
