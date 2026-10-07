# Relatório de Classificação Estrutural das Tabelas

**Total de tabelas classificadas:** 753
**Categorias representadas:** 16 de 17

---

## Resumo

| Categoria | Quantidade | % | Confiança |
|---|---|---|---|
| bem_formada | 351 | 46.6% | 296 high, 55 medium |
| sem_cabecalho | 3 | 0.4% | 3 high |
| cabecalho_na_primeira_linha_1 | 36 | 4.8% | 36 high |
| cabecalho_na_primeira_linha_2 | 1 | 0.1% | 1 high |
| cabecalho_na_segunda_linha | 2 | 0.3% | 2 high |
| cabecalho_na_terceira_linha_1 | 1 | 0.1% | 1 high |
| cabecalho_na_terceira_linha_2 | 1 | 0.1% | 1 high |
| cabecalho_composto_1 | 4 | 0.5% | 4 high |
| cabecalho_composto_2 | 50 | 6.6% | 0 high, 50 medium |
| sub_tabelas_1 | 273 | 36.3% | 273 high |
| sub_tabelas_2 | 1 | 0.1% | 1 high |
| sub_tabelas_3 | 19 | 2.5% | 19 high |
| sub_tabelas_4 | 1 | 0.1% | 0 high, 1 medium |
| separador_\| | 5 | 0.7% | 5 high |
| vazia | 1 | 0.1% | 1 high |
| dados_sem_utilidade | 4 | 0.5% | 4 high |
| **TOTAL** | **753** | **100%** | |

---

## Detalhamento por Categoria

### Bem Formada (`bem_formada`)

**Quantidade:** 351 tabelas (46.6% do total)
**Confiança:** 296 high, 55 medium

**Descrição:** Todas as colunas são nomeadas e cada célula tem valores consistentes (mesmo tipo e estrutura: data, nome, número, etc.). Formato ideal para processamento — representa tabelas colunares com cabeçalho real e tipos de dados consistentes.

**Heurística de identificação:** R3b (HEADER_IS_REAL): classificação de colunas + R4 (consistência de tipos entre colunas). O classificador de colunas (R3a) identifica que a maioria das colunas tem nomes reais (REAL_NAME), e a regra R4 verifica que não há mistura de tipos (numérico vs. texto) dentro das colunas.

**Exemplo:** `001_2012_10_outubro_20121009_bases_relatório_executivo_0910201`

<details>
<summary>Ver todas as 351 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `001_2012_10_outubro_20121009_bases_relatório_executivo_0910201` | high |  |
| `001_2012_10_outubro_20121009_bases_relatório_executivo_1610201` | high |  |
| `011_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópi` | high |  |
| `018_int040_ministeriocidades_far_caixa_empreendimentos_20181001` | high |  |
| `01_2012_10_outubro_20121009_bases_relatório_executivo_09102012` | high |  |
| `01_2012_10_outubro_20121009_bases_relatório_executivo_16102012` | high |  |
| `024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `11_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópia` | high |  |
| `1_2018_int054_ministeriocidades_far_bb_empreendimentos_20180831` | high |  |
| `202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202406_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202407_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202408_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202409_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202411_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202412_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202501_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202503_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202504_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202505_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202506_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202507_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202508_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202509_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202510_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202511_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202512_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202601_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202602_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `202603_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | high |  |
| `_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base_bd` | high |  |
| `_001_2012_04_abril_2012_04_18_5c_base_contratação_pf_18042012` | high |  |
| `a_001_2010_09___setembro_2010_contratação_pf_total___06092010` | high |  |
| `aixa_001_2013_02___fevereiro_bases_relatório_executivo_1502201` | high |  |
| `aixa_001_2013_02___fevereiro_bases_relatório_executivo_2802201` | high |  |
| `aixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v` | high |  |
| `aixa_001_2016_02_fevereiro_relatorio_cidades__entregas_20160229` | high |  |
| `atorio_executivo_30092012_v3_bases_relatório_executivo_3009201` | high |  |
| `bb_2012_05_maio_z_relatorio_caixa_3105` | high |  |
| `bb_2012_10_outubro_entrada_bb_20121023` | medium | inconsistência de tipos entre colunas |
| `bb_2012_10_outubro_entrada_bb_20121031` | medium | inconsistência de tipos entre colunas |
| `bb_2012_10_outubro_entrada_bb_20121031_ajustada` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121106` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121113` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121119` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121127` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121127v2` | medium | inconsistência de tipos entre colunas |
| `bb_2012_11_novembro_entrada_bb_20121130` | medium | inconsistência de tipos entre colunas |
| `bb_2012_12_dezembro_entrada_bb_20121210` | medium | inconsistência de tipos entre colunas |
| `bb_2012_12_dezembro_entrada_bb_20121217` | medium | inconsistência de tipos entre colunas |
| `bb_2012_12_dezembro_entrada_bb_20121228` | medium | inconsistência de tipos entre colunas |
| `bb_2013_01_janeiro_entrada_bb_20130107` | medium | inconsistência de tipos entre colunas |
| `bb_2013_01_janeiro_entrada_bb_20130113` | medium | inconsistência de tipos entre colunas |
| `bb_2013_01_janeiro_entrada_bb_20130121` | medium | inconsistência de tipos entre colunas |
| `bb_2013_01_janeiro_entrada_bb_20130128` | medium | inconsistência de tipos entre colunas |
| `bb_2013_02_fevereiro_entrada_bb_20130205` | medium | inconsistência de tipos entre colunas |
| `bb_2013_02_fevereiro_entrada_bb_20130219` | medium | inconsistência de tipos entre colunas |
| `bb_2013_02_fevereiro_entrada_bb_20130226` | medium | inconsistência de tipos entre colunas |
| `bb_2013_03_marco_entrada_bb_20130305` | medium | inconsistência de tipos entre colunas |
| `bb_2013_03_marco_entrada_bb_20130318` | medium | inconsistência de tipos entre colunas |
| `bb_2013_03_marco_entrada_bb_20130401` | medium | inconsistência de tipos entre colunas |
| `bb_2013_03_marco_entrada_bb_20130415` | medium | inconsistência de tipos entre colunas |
| `bb_2013_04_abril_entrada_bb_20130429` | medium | inconsistência de tipos entre colunas |
| `bb_2013_05_maio_entrada_bb_20130506` | medium | inconsistência de tipos entre colunas |
| `bb_2013_05_maio_entrada_bb_20130513` | medium | inconsistência de tipos entre colunas |
| `bb_2013_06_junho_entrada_bb_20130604` | medium | inconsistência de tipos entre colunas |
| `bb_2013_06_junho_entrada_bb_20130618` | medium | inconsistência de tipos entre colunas |
| `bb_2013_06_junho_entrada_bb_20130630` | medium | inconsistência de tipos entre colunas |
| `bb_2013_06_junho_pmcmv_18062013_tab_beneficiarios_fgts` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pf_fgts` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_empreendimentos` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_operacoes` | high |  |
| `bb_2013_07_julho_entrada_bb_20130716` | medium | inconsistência de tipos entre colunas |
| `bb_2013_08_agosto_entrada_bb_20130802` | medium | inconsistência de tipos entre colunas |
| `bb_2013_08_agosto_entrada_bb_20130819` | medium | inconsistência de tipos entre colunas |
| `bb_2013_08_agosto_entrada_bb_20130904` | medium | inconsistência de tipos entre colunas |
| `bb_2013_10_outubro_entrada_bb_20131105` | medium | inconsistência de tipos entre colunas |
| `bb_2013_11_novembro_entrada_bb_20131120` | medium | inconsistência de tipos entre colunas |
| `bb_2013_11_novembro_entrada_bb_20131204` | medium | inconsistência de tipos entre colunas |
| `bb_2013_12_dezembro_entrada_bb_20131218` | medium | inconsistência de tipos entre colunas |
| `bb_2013_12_dezembro_entrada_bb_20131223__aux_bb_far` | high |  |
| `bb_2013_12_dezembro_entrada_bb_20140102` | medium | inconsistência de tipos entre colunas |
| `bb_2014_01_janeiro_entrada_bb_20140116` | medium | inconsistência de tipos entre colunas |
| `bb_2014_02_fevereiro_entrada_bb_20140217` | medium | inconsistência de tipos entre colunas |
| `bb_2014_02_fevereiro_entrada_bb_20140306` | medium | inconsistência de tipos entre colunas |
| `bb_2014_03_marco_entrada_bb_20140401` | medium | inconsistência de tipos entre colunas |
| `bb_2014_04_abril_entrada_bb_20140415` | medium | inconsistência de tipos entre colunas |
| `bb_2014_04_abril_entrada_bb_20140430` | medium | inconsistência de tipos entre colunas |
| `bb_2014_05_maio_entrada_bb_20140516` | medium | inconsistência de tipos entre colunas |
| `bb_2014_05_maio_entrada_bb_20140530` | medium | inconsistência de tipos entre colunas |
| `bb_2014_06_junho_entrada_bb_20140615` | medium | inconsistência de tipos entre colunas |
| `bb_2014_06_junho_entrada_bb_20140625` | medium | inconsistência de tipos entre colunas |
| `bb_2014_06_junho_entrada_bb_20140701` | medium | inconsistência de tipos entre colunas |
| `bb_2014_07_julho_entrada_bb_20140731` | medium | inconsistência de tipos entre colunas |
| `bb_2014_08_agosto_entrada_bb_20140818_antigo` | medium | inconsistência de tipos entre colunas |
| `bb_2014_08_agosto_entrada_bb_20140829_antigo` | medium | inconsistência de tipos entre colunas |
| `bb_2014_09_setembro_entrada_bb_20140930` | medium | inconsistência de tipos entre colunas |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pf_pf` | high |  |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pj` | high |  |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pj_pf` | high |  |
| `bb_2014_10_outubro_2014_10_30_pnhr_30102014` | high |  |
| `bb_2014_11_novembro_3011_min_cidades_pf_pf` | high |  |
| `bb_2014_11_novembro_3011_min_cidades_pj` | high |  |
| `bb_2014_11_novembro_3011_min_cidades_pj_pf` | high |  |
| `bb_2014_11_novembro_3011_pnhr_30112014` | high |  |
| `bb_2014_12_dezembro_min_cidades_pf_pf` | high |  |
| `bb_2014_12_dezembro_min_cidades_pj` | high |  |
| `bb_2014_12_dezembro_min_cidades_pj_pf` | high |  |
| `bb_2014_12_dezembro_pnhr_15122014` | high |  |
| `bb_2014_12_dezembro_pnhr_20141231` | high |  |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pf_pf` | high |  |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pj` | high |  |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pj_pf` | high |  |
| `bb_2015_01_janeiro_2015_01_15_pnhr_15012015` | high |  |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pf_pf` | high |  |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pj` | high |  |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pj_pf` | high |  |
| `bb_2015_02_fevereiro_min_cidades_pf_pf` | high |  |
| `bb_2015_02_fevereiro_min_cidades_pj` | high |  |
| `bb_2015_02_fevereiro_min_cidades_pj_2015_02` | high |  |
| `bb_2015_02_fevereiro_min_cidades_pj_pf` | high |  |
| `bb_2015_02_fevereiro_pnhr_28022015` | high |  |
| `bb_2015_03_marco_min_cidades_pf_pf` | high |  |
| `bb_2015_03_marco_min_cidades_pj` | high |  |
| `bb_2015_03_marco_min_cidades_pj_2015_03` | high |  |
| `bb_2015_03_marco_min_cidades_pj_pf` | high |  |
| `bb_2015_03_marco_pnhr_15032015` | high |  |
| `bb_2015_03_marco_pnhr_31032015` | high |  |
| `bb_2015_04_abril_min_cidades_pf_pf` | high |  |
| `bb_2015_04_abril_min_cidades_pj` | high |  |
| `bb_2015_04_abril_min_cidades_pj_pf` | high |  |
| `bb_2015_04_abril_pnhr_30042015` | high |  |
| `bb_2015_05_maio_2015_05_31_min_cidades_pf_pf` | high |  |
| `bb_2015_05_maio_2015_05_31_min_cidades_pj` | high |  |
| `bb_2015_05_maio_2015_05_31_min_cidades_pj_pf` | high |  |
| `bb_2015_06_junho_min_cidades_pf_pf` | high |  |
| `bb_2015_06_junho_min_cidades_pj` | high |  |
| `bb_2015_06_junho_min_cidades_pj_pf` | high |  |
| `bb_2015_08_agosto_min_cidades_pf_pf` | high |  |
| `bb_2015_08_agosto_min_cidades_pj` | high |  |
| `bb_2015_08_agosto_min_cidades_pj_pf` | high |  |
| `bb_2015_08_agosto_min_cidades_pj_sem_aspas` | high |  |
| `bb_2015_09_setembro_min_cidades_pf_pf` | high |  |
| `bb_2015_09_setembro_min_cidades_pj` | high |  |
| `bb_2015_09_setembro_min_cidades_pj_pf` | high |  |
| `bb_2015_09_setembro_pnhr_30092015` | high |  |
| `bb_2015_10_outubro_min_cidades_pf_pf` | high |  |
| `bb_2015_10_outubro_min_cidades_pj` | high |  |
| `bb_2015_10_outubro_min_cidades_pj_pf` | high |  |
| `bb_2015_10_outubro_pnhr_31102015` | high |  |
| `bb_2015_12_dezembro_min_cidades_pf_pf` | high |  |
| `bb_2015_12_dezembro_min_cidades_pj` | high |  |
| `bb_2015_12_dezembro_min_cidades_pj_pf` | high |  |
| `bb_2015_12_dezembro_pnhr_31122015` | high |  |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pf_pf` | high |  |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pj` | high |  |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pj_pf` | high |  |
| `bb_2016_01_janeiro_pnhr_31012016` | high |  |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pf_pf` | high |  |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj` | high |  |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj_pf` | high |  |
| `bb_2016_02_fevereiro_pnhr_29022016` | high |  |
| `bb_2016_04_abril_min_cidades_pf_pf` | high |  |
| `bb_2016_04_abril_min_cidades_pj` | high |  |
| `bb_2016_04_abril_min_cidades_pj_pf` | high |  |
| `bb_2016_04_abril_pnhr_30042016` | high |  |
| `bb_2016_05_maio_2016_05_31_min_cidades_pf_pf` | high |  |
| `bb_2016_05_maio_2016_05_31_min_cidades_pj` | high |  |
| `bb_2016_05_maio_2016_05_31_min_cidades_pj_pf` | high |  |
| `bb_2016_05_maio_pnhr_31052016` | high |  |
| `bb_2016_06_junho_2016_06_30_min_cidades_pf_pf` | high |  |
| `bb_2016_06_junho_2016_06_30_min_cidades_pj` | high |  |
| `bb_2016_06_junho_2016_06_30_min_cidades_pj_pf` | high |  |
| `bb_2016_06_junho_pnhr_30062016` | high |  |
| `bb_2016_07_julho_2016_07_31_min_cidades_pj` | high |  |
| `bb_2016_07_julho_min_cidades_pf_pf` | high |  |
| `bb_2016_07_julho_min_cidades_pj_pf` | high |  |
| `bb_2016_07_julho_pnhr_31072016` | high |  |
| `bb_2018_2018_04_06_pf_pf` | high |  |
| `bb_2018_2018_04_06_pj` | high |  |
| `bb_2018_2018_04_06_pj_pf` | high |  |
| `bb_2018_2018_05_10_pf_pf` | high |  |
| `bb_2018_2018_05_10_pj` | high |  |
| `bb_2018_2018_05_10_pj_pf` | high |  |
| `bb_2018_2018_06_10_pf_pf` | high |  |
| `bb_2018_2018_06_10_pj` | high |  |
| `bb_2018_2018_06_10_pj_pf` | high |  |
| `bb_2018_2018_07_03_pf_pf` | high |  |
| `bb_2018_2018_07_03_pj` | high |  |
| `bb_2018_2018_07_03_pj_pf` | high |  |
| `bb_2018_2018_07_03_pnhr_04072018` | high |  |
| `bb_2018_2018_08_06_pf_pf` | high |  |
| `bb_2018_2018_08_06_pj` | high |  |
| `bb_2018_2018_08_06_pj_pf` | high |  |
| `bb_2018_2018_09_10_pf_pf` | high |  |
| `bb_2018_2018_09_10_pj` | high |  |
| `bb_2018_2018_09_10_pj_pf` | high |  |
| `bb_2018_2018_10_01_pf_pf` | high |  |
| `bb_2018_2018_10_01_pj` | high |  |
| `bb_2018_2018_10_01_pj_pf` | high |  |
| `bb_2018_2018_10_30_pf_pf` | high |  |
| `bb_2018_2018_10_30_pj` | high |  |
| `bb_2018_2018_10_30_pj_pf` | high |  |
| `bb_2019_2019_03_07_pf_pf` | high |  |
| `bb_2019_2019_03_07_pj_pf` | high |  |
| `caixa_001_2010_06___junho_2010_contratação30062010` | high |  |
| `caixa_001_2010_09___setembro_2010_pmcmv06092010_desligados` | high |  |
| `caixa_001_2011_06_junho_presidencia` | high |  |
| `caixa_001_2011_07_julho_presidencia_automatizado_01082011` | high |  |
| `caixa_001_2011_07_julho_presidencia_automatizado_28072011` | high |  |
| `caixa_001_2011_08_agosto_demanda_mc_far_pmcmv` | high |  |
| `caixa_001_2011_08_agosto_presidencia_automatizado_01082011` | high |  |
| `caixa_001_2011_08_agosto_presidencia_automatizado_mcmvii_082011` | high |  |
| `caixa_001_2012_04_abril_2012_04_18_5b1_base_empreend_contratado` | high |  |
| `caixa_001_2012_04_abril_2012_04_18_5b_empreend_contratado` | high |  |
| `caixa_001_2012_04_abril_2012_04_18_5c1__base_contrata__o_pf` | high |  |
| `caixa_001_2012_04_abril_2012_04_18_5c_base_contrata__o_pf` | high |  |
| `caixa_001_2012_04_abril_2012_04_18_rural_18042012` | high |  |
| `caixa_001_2012_04_abril_2012_04_30_ajustada_caixa_5d_base_bext` | high |  |
| `caixa_001_2012_04_abril_5b1_base_empreend_contratado` | high |  |
| `caixa_001_2012_04_abril_validacao_1604_1804` | high |  |
| `caixa_001_2012_05_maio_5b1__base_empreend_contratado` | high |  |
| `caixa_001_2012_07_julho_bases_relatório_executivo_31072012_v2` | high |  |
| `caixa_001_2012_07_julho_bases_relatório_executivo_31_07_12` | high |  |
| `caixa_001_2012_08_agosto_base_pf_e_pj_06_08_12` | high |  |
| `caixa_001_2012_08_agosto_base_pf_e_pj_14_08_12` | high |  |
| `caixa_001_2012_08_agosto_base_pf_e_pj_20_08_12` | high |  |
| `caixa_001_2012_08_agosto_base_pf_e_pj_24_08_12` | high |  |
| `caixa_001_2012_08_agosto_bases_relatório_executivo_31_08_12` | high |  |
| `caixa_001_2012_09_setembro_bases_relatório_executivo_10_09_12` | high |  |
| `caixa_001_2012_09_setembro_bases_relatório_executivo_15_09_12` | high |  |
| `caixa_001_2012_09_setembro_bases_relatório_executivo_26092012` | high |  |
| `caixa_001_2012_09_setembro_bases_relatório_executivo_30092012` | high |  |
| `caixa_001_2012_10_outubro_bases_relatório_executivo_31102012` | high |  |
| `caixa_001_2012_11_novembro_bases_relat_rio_executivo_12112012` | high |  |
| `caixa_001_2012_11_novembro_bases_relatório_executivo_28112012` | high |  |
| `caixa_001_2012_12_dezembro_bases_relatório_executivo_21122012` | high |  |
| `caixa_001_2012_12_dezembro_bases_relatório_executivo_31122012` | high |  |
| `caixa_001_2013_01___janeiro_bases_relatório_executivo_31012013` | high |  |
| `caixa_001_2013_02___fevereiro_propostas_base_far` | high |  |
| `caixa_001_2013_03___março_bases_relat_rio_executivo_28032013` | high |  |
| `caixa_001_2013_03___março_bases_relatório_executivo_15032013` | high |  |
| `caixa_001_2013_03___março_bases_relatório_executivo_28032013` | high |  |
| `caixa_001_2013_04___abril_bases_relatório_executivo_30042013` | high |  |
| `caixa_001_2013_05___maio_bases_relat_rio_executivo_150513` | high |  |
| `caixa_001_2013_12___dezembro_bases_relat_rio_executivo` | high |  |
| `caixa_001_2014_11_novembro_bases_relatório_executivo_15112014` | high |  |
| `caixa_001_2014_11_novembro_bases_relatório_executivo_30112014` | high |  |
| `caixa_001_2015_04_abril_bases_relatório_executivo_30042015` | high |  |
| `caixa_001_2015_04_abril_bases_relatório_executivo_30042015_v2` | high |  |
| `caixa_001_2015_06_junho_bext_30062015` | high |  |
| `caixa_001_2015_07_julho_min_cidades_pf_pf` | high |  |
| `caixa_001_2015_07_julho_min_cidades_pj` | high |  |
| `caixa_001_2015_07_julho_min_cidades_pj_pf` | high |  |
| `caixa_001_2015_10_outubro_bext_31102015` | high |  |
| `caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2` | high |  |
| `caixa_001_2016_bext_30042016` | high |  |
| `caixa_002_2015_07_julho_bext_31072015` | high |  |
| `caixa_002_2017_bext_abr2017` | high |  |
| `caixa_002_2017_bext_ago2017` | high |  |
| `caixa_002_2017_bext_dez17` | high |  |
| `caixa_002_2017_bext_fev2017` | high |  |
| `caixa_002_2018_bext_abr18` | high |  |
| `caixa_002_2018_bext_ago18` | high |  |
| `caixa_002_2018_bext_jun18` | high |  |
| `caixa_002_2018_bext_mai18` | high |  |
| `caixa_002_2018_bext_mar18` | high |  |
| `caixa_003_2017_bext_jan_2017` | high |  |
| `caixa_003_2017_bext_jul2017` | high |  |
| `caixa_003_2017_bext_mai2017` | high |  |
| `caixa_003_2017_bext_mar2017` | high |  |
| `caixa_003_2017_bext_out2017` | high |  |
| `ecente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02_correcao` | high |  |
| `elatorio_executivo_31102014_bases_relatório_executivo_31102014` | high |  |
| `historico_recente_202406_snh_pmcmv_dados_prioritarios_af_bb` | medium | inconsistência de tipos entre colunas |
| `historico_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202407_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202409_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202410_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202411_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202411_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202412_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202412_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_2024_08_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_2024_09_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_2024_10_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202501_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202502_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202503_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202503_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202504_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202504_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202505_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202505_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202506_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202506_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202507_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202507_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202508_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202508_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202509_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202509_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202510_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202510_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202511_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202511_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202512_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202512_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_2025_01_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_2025_02_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202601_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202601_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202602_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202602_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `historico_recente_202603_snh_pmcmv_dados_prioritarios_af_bb` | high |  |
| `historico_recente_202603_snh_pmcmv_dados_prioritarios_af_caixa` | high |  |
| `ixa_001_2010_09___setembro_2010_contratação_pf_total___060920` | high |  |
| `ixa_001_2011_09_setembro_presidencia_automatizado_mcmvii_082011` | high |  |
| `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_13012012_base` | high |  |
| `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base` | high |  |
| `ixa_001_2013_02___fevereiro_bases_relatório_executivo_15022013` | high |  |
| `ixa_001_2013_02___fevereiro_bases_relatório_executivo_28022013` | high |  |
| `ixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v2` | high |  |
| `o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202407_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202408_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202409_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202410_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202411_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202412_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202501_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202502_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202504_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202505_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202506_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202507_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202508_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202509_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202510_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202511_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202512_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202601_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202602_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `o_recente_202603_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | high |  |
| `relatorio_executivo_31102014_bases_relatório_executivo_3110201` | high |  |
| `storico_recente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02` | medium | inconsistência de tipos entre colunas |
| `torio_executivo_30092012_v3_bases_relatório_executivo_30092012` | high |  |
| `xa_001_2011_12_dezembro_pmcmv_relatorio_executivo_31122011_base` | high |  |
| `xa_001_2012_04_abril_2012_04_18_5a_propostas_recebidas_18042012` | high |  |
| `xa_001_2012_04_abril_2012_04_18_5b_empreend_contratado_18042012` | high |  |
| `xa_001_2012_04_abril_2012_04_18_5c_base_contratação_pf_180420` | high |  |

</details>

### Sem Cabeçalho (`sem_cabecalho`)

**Quantidade:** 3 tabelas (0.4% do total)
**Confiança:** 3 high

**Descrição:** Os nomes das colunas (lidos pelo pandas) são, na verdade, a primeira linha de dados. A verdadeira linha de cabeçalho está ausente ou foi perdida na extração. Dados possuem estrutura colunar densa, mas sem nomes de coluna originais.

**Heurística de identificação:** R3b (HEADER_IS_DATA): quando a maioria das colunas é classificada como dados (DATA_VALUE) e a tabela tem baixa proporção de linhas vazias (empty_ratio ≤ 0.15). Após o check R5 (sub-tabelas) falhar, retorna sem_cabecalho para dados densos, ou nao_colunares_tipo1 para dados esparsos.

**Exemplo:** `bb_2019_2019_05_07_2019_05_07_pj`

<details>
<summary>Ver todas as 3 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2019_2019_05_07_2019_05_07_pj` | high |  |
| `bb_2019_2019_05_07_pf_pf` | high |  |
| `bb_2019_2019_05_07_pj_pf` | high |  |

</details>

### Cabeçalho na Primeira Linha (Tipo 1) (`cabecalho_na_primeira_linha_1`)

**Quantidade:** 36 tabelas (4.8% do total)
**Confiança:** 36 high

**Descrição:** A primeira coluna tem um nome descritivo (ex.: "Município"), mas as demais colunas são `unnamed`. A primeira **linha** de dados contém nomes adequados para as colunas, e os dados começam na segunda linha.

**Heurística de identificação:** R7 (cabeçalho deslocado): detecta que as colunas têm cabeçalhos `unnamed` mas a primeira linha de dados contém valores que se parecem com nomes de colunas (texto com padrão de cabeçalho). Subtipo 1 identificado quando a primeira linha tem nomes de coluna padrão.

**Exemplo:** `aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2`

<details>
<summary>Ver todas as 36 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2` | high |  |
| `caixa_001_2015_01_janeiro_bases_relatório_executivo_31012015` | high |  |
| `caixa_001_2015_02_fevereiro_bases_relatório_executivo_28022015` | high |  |
| `caixa_001_2015_03_marco_bases_relatório_executivo_31032015` | high |  |
| `caixa_001_2015_03_marco_bases_relatório_executivo_31032015v2` | high |  |
| `caixa_001_2015_05_maio_bases_relat_rio_executivo_31052015` | high |  |
| `caixa_001_2015_06_junho_bases_relatório_executivo_30062015` | high |  |
| `caixa_001_2015_07_julho_bases_relatório_executivo_31072015` | high |  |
| `caixa_001_2015_10_outubro_bases_relatório_executivo_31102015` | high |  |
| `caixa_001_2015_11_novembro_bases_relatório_executivo_30112015` | high |  |
| `caixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v` | high |  |
| `caixa_001_2016_bases_relatório_executivo_30042016` | high |  |
| `caixa_001_2017_contratação_por_uf_nov_2017` | high |  |
| `caixa_002_2015_08_agosto_bases_relatório_executivo_12082015` | high |  |
| `caixa_002_2015_08_agosto_bases_relatório_executivo_31082015` | high |  |
| `caixa_002_2015_12_dezembro_bases_relatório_executivo_31122015` | high |  |
| `caixa_002_2015_12_dezembro_bext_31122015` | high |  |
| `caixa_002_2016_08_agosto_bases_relatório_executivo_31082016` | high |  |
| `caixa_002_2016_bases_relatório_executivo_31122016` | high |  |
| `caixa_002_2016_bext_31122016` | high |  |
| `caixa_002_2017_bases_relat_rio_executivo_abr2017` | high |  |
| `caixa_002_2017_bases_relat_rio_executivo_ago2017` | high |  |
| `caixa_002_2017_bases_relat_rio_executivo_fev2017` | high |  |
| `caixa_002_2017_bases_relatório_executivo_dez17` | high |  |
| `caixa_002_2018_bases_relat_rio_executivo_abr2018` | high |  |
| `caixa_002_2018_bases_relat_rio_executivo_abr2018_v2` | high |  |
| `caixa_002_2018_bases_relat_rio_executivo_jun2018` | high |  |
| `caixa_002_2018_bases_relat_rio_executivo_mai2018` | high |  |
| `caixa_002_2018_bases_relat_rio_executivo_mar18` | high |  |
| `caixa_002_2018_bases_relatório_executivo_ago2018` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_31012017` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_jul2017` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_jun2017` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_mai2017` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_mar2017` | high |  |
| `caixa_003_2017_bases_relat_rio_executivo_nov17` | high |  |

</details>

### Cabeçalho na Primeira Linha (Tipo 2) (`cabecalho_na_primeira_linha_2`)

**Quantidade:** 1 tabela (0.1% do total)
**Confiança:** 1 high

**Descrição:** Similar ao tipo 1: primeira coluna com nome descritivo, demais `unnamed`. A primeira linha contém nomes adequados. Possui várias linhas vazias antes da última linha, que contém agregação total dos dados com valores numéricos.

**Heurística de identificação:** Identificado manualmente via hard-coded check no `classificar_formacao()` para o nome de arquivo específico `dados_22022011`. É um caso especial que não se encaixa perfeitamente nas heurísticas gerais de R7.

**Exemplo:** `bb_2011_02_fevereiro_dados_22022011`

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2011_02_fevereiro_dados_22022011` | high |  |

</details>

### Cabeçalho na Segunda Linha (`cabecalho_na_segunda_linha`)

**Quantidade:** 2 tabelas (0.3% do total)
**Confiança:** 2 high

**Descrição:** Primeira linha contém texto "Posicao:" mais data no formato "DD/MM/YYYY", indicando a data do relatório. Segunda linha tem os nomes adequados para as colunas. Dados começam na terceira linha. Pode conter linha de totalização ao final.

**Heurística de identificação:** Heurística especial `_detecta_posicao_pattern(df)` executada antes de R5/R6 no fluxo AMBIGUOUS: verifica se a primeira célula da primeira linha contém o texto "Posicao:" ou "Posição:" seguido de uma data no formato DD/MM/YYYY.

**Exemplo:** `caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11`

<details>
<summary>Ver todas as 2 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11` | high |  |
| `caixa_001_2011_11_novembro_relatório_executivo_mcid_21_11_11` | high |  |

</details>

### Cabeçalho na Terceira Linha (Tipo 1) (`cabecalho_na_terceira_linha_1`)

**Quantidade:** 1 tabela (0.1% do total)
**Confiança:** 1 high

**Descrição:** Todas as colunas são `unnamed`. As duas primeiras linhas são vazias (ou têm poucos valores não nulos) e a terceira linha contém nomes adequados de colunas. Dados começam na quarta linha.

**Heurística de identificação:** R7 (cabeçalho deslocado): detecta que o cabeçalho real está deslocado para a terceira linha. O classificador identifica o padrão de linhas iniciais vazias seguidas por uma linha com nomes de coluna.

**Exemplo:** `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2`

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2` | high |  |

</details>

### Cabeçalho na Terceira Linha (Tipo 2) (`cabecalho_na_terceira_linha_2`)

**Quantidade:** 1 tabela (0.1% do total)
**Confiança:** 1 high

**Descrição:** Todas as colunas são `unnamed`. Primeira linha vazia, segunda linha possui nome descritivo, e a terceira linha contém nomes adequados. Contém dados sobre faixas do PMCMV com colunas específicas como "Público-alvo", "Concluída", e porcentagens de conclusão de obras.

**Heurística de identificação:** R7 (cabeçalho deslocado): subtipo 2 identificado por palavras-chave específicas nas primeiras linhas (ex.: "Público-alvo", "Concluída", "Total geral").

**Exemplo:** `bb_2011_08_agosto_balanço_23_08_2011_min__planejamento`

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2011_08_agosto_balanço_23_08_2011_min__planejamento` | high |  |

</details>

### Cabeçalho Composto (Tipo 1) (`cabecalho_composto_1`)

**Quantidade:** 4 tabelas (0.5% do total)
**Confiança:** 4 high

**Descrição:** Tabela com cabeçalho que utiliza as 3 primeiras linhas para gerar nomes das colunas, provenientes de tabelas Excel com células mescladas. Contém dados do PMCMV agregados por UF e região. Primeira coluna com nome descritivo, colunas iniciais vazias, dados começam na quarta linha.

**Heurística de identificação:** R6 (cabeçalho composto): detecta que múltiplas linhas iniciais precisam ser combinadas para formar os nomes corretos das colunas. Subtipo 1 usa 3 linhas de cabeçalho.

**Exemplo:** `001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo`

<details>
<summary>Ver todas as 4 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo` | high |  |
| `a_001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_mode` | high |  |
| `bb_2013_02_fevereiro_propostas_bb` | high |  |
| `caixa_001_2015_datas_entregas_pj_mcmv_30062015` | high |  |

</details>

### Cabeçalho Composto (Tipo 2) (`cabecalho_composto_2`)

**Quantidade:** 50 tabelas (6.6% do total)

**Descrição:** Tabela com cabeçalho que utiliza as 2 primeiras linhas para gerar nomes das colunas, provenientes de tabelas Excel com células mescladas. Característica comum dos relatórios `relatorio_min__cidades` do Banco do Brasil.

**Heurística de identificação:** R6 (cabeçalho composto): subtipo 2 usa 2 linhas de cabeçalho. Detectado quando colunas são `unnamed` mas as primeiras linhas de dados contêm texto estruturado que forma cabeçalhos compostos. Confidence tipicamente medium devido à ambiguidade na detecção.

**Exemplo:** `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd` *(confidence: medium)*

<details>
<summary>Ver todas as 50 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd` | medium |  |
| `bb_2011_08_agosto_relatorio_min__cidades_16ago11` | medium |  |
| `bb_2011_08_agosto_relatorio_min__cidades_30ago11` | medium |  |
| `bb_2011_09_setembro_relatorio_min__cidades_13set11_2` | medium |  |
| `bb_2011_10_outubro_relatorio_min__cidades_04out11` | medium |  |
| `bb_2011_10_outubro_relatorio_min__cidades_11out11` | medium |  |
| `bb_2011_10_outubro_relatorio_min__cidades_18out11` | medium |  |
| `bb_2011_10_outubro_relatorio_min__cidades_25out11` | medium |  |
| `bb_2012_01_janeiro_relatorio_min__cidades_17jan12` | medium |  |
| `bb_2012_01_janeiro_relatorio_min__cidades_24jan12` | medium |  |
| `bb_2012_01_janeiro_relatorio_min__cidades_31_01_2012` | medium |  |
| `bb_2012_01_janeiro_relatorio_min_cidades_03_jan_12` | medium |  |
| `bb_2012_02_fevereiro_relatorio_min__cidades_06_02_2012` | medium |  |
| `bb_2012_02_fevereiro_relatorio_min__cidades_28_02_2012` | medium |  |
| `bb_2012_03_março_relatorio_min__cidades_06_03_2012` | medium |  |
| `bb_2012_03_março_relatorio_min__cidades_13_03_2012` | medium |  |
| `bb_2012_03_março_relatorio_min__cidades_20_03_2012` | medium |  |
| `bb_2012_03_março_relatorio_min__cidades_27_03_2012` | medium |  |
| `bb_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_b` | medium |  |
| `bb_2012_04_abril_rel_min_cidades_valores_25_04_2012` | medium |  |
| `bb_2012_04_abril_rel_min_cidades_valores_25_04_2012_bd` | medium |  |
| `bb_2012_04_abril_relatorio_min__cidades_03_04_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_08_05_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_08_05_2012_bd` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_15_05_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_15_05_2012_bd` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_21_05_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_21_05_2012___bd` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_28_05_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_28_05_2012_ctr_caixa_bb` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_31_05_2012` | medium |  |
| `bb_2012_05_maio_relatorio_min__cidades_31_05_2012_bd` | medium |  |
| `bb_2012_06_junho_relatorio_min__cidades_04_06_2012` | medium |  |
| `bb_2012_06_junho_relatorio_min__cidades_18_06_2012` | medium |  |
| `bb_2012_06_junho_relatorio_min__cidades_18_06_2012_ret` | medium |  |
| `bb_2012_06_junho_relatorio_min__cidades_26_06_2012` | medium |  |
| `bb_2012_07_julho_relatorio_min__cidades_03_07_2012` | medium |  |
| `bb_2012_07_julho_relatorio_min__cidades_09_07_2012` | medium |  |
| `bb_2012_07_julho_relatorio_min__cidades_17_07_2012` | medium |  |
| `bb_2012_07_julho_relatorio_min__cidades_24_07_2012` | medium |  |
| `bb_2012_07_julho_relatorio_min__cidades_30_07_2012` | medium |  |
| `bb_2012_08_agosto_relatorio_min__cidades_07_08_2012__1` | medium |  |
| `bb_2012_08_agosto_relatorio_min__cidades_14_08_2012` | medium |  |
| `bb_2012_08_agosto_relatorio_min__cidades_21_08_2012` | medium |  |
| `bb_2012_08_agosto_relatorio_min__cidades_27_08_2012__1` | medium |  |
| `bb_2012_08_agosto_relatorio_min__cidades_31_08_2012` | medium |  |
| `bb_2012_09_setembro_relatorio_min__cidades_01_10_2012` | medium |  |
| `bb_2012_09_setembro_relatorio_min__cidades_11_09_2012` | medium |  |
| `bb_2012_10_outubro_relatorio_min__cidades_09_10_2012` | medium |  |
| `bb_2012_10_outubro_relatorio_min__cidades_23_10_2012` | medium |  |

</details>

### Sub-tabelas (Tipo 1) (`sub_tabelas_1`)

**Quantidade:** 273 tabelas (36.3% do total)
**Confiança:** 273 high

**Descrição:** Contém diversas "sub-tabelas" separadas por várias linhas vazias. Cabeçalho da tabela-mãe tem primeira coluna `unnamed_0` e as demais no formato timestamp `YYYYMMDD_hhmmss`. Cada sub-tabela tem sua própria estrutura com categorias (frentes, faixas, UFs) e valores numéricos de UH contratadas/entregues.

**Heurística de identificação:** R3a identifica colunas com padrão timestamp (YYYYMMDD_hhmmss). R5 (classificar_sub_tabelas) confirma a estrutura de sub-tabelas: detecta palavras-chave como "SÍNTESE", "Faixa", "Região", "Quadro de Valores" e padrões de linhas vazias entre blocos de dados.

**Exemplo:** `001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012`

<details>
<summary>Ver todas as 273 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012` | high |  |
| `001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópia` | high |  |
| `001_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_201` | high |  |
| `001_2012_10_outubro_20121009_pmcmv_relatório_executivo_0910201` | high |  |
| `001_2014_12_dezembro_rel_executivo_resumo_31122014_reprocessado` | high |  |
| `01_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_2012` | high |  |
| `01_2012_05_maio_pmcmv_relatorio_executivo_31_05_2012__version_1` | high |  |
| `01_2012_06_junho_pmcmv_relatorio_executivo_15_06_2012_corrigido` | high |  |
| `01_2012_10_outubro_20121009_pmcmv_relatório_executivo_09102012` | high |  |
| `1_2012_04_abril_2012_04_18_pmcmv_relatorio_executivo_18_04_2012` | high |  |
| `_001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópi` | high |  |
| `_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__2013` | high |  |
| `_001_2015_01_janeiro_rel_executivo_resumo_31012015_reprocessado` | high |  |
| `a_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__201` | high |  |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_17_02_2012` | high |  |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_27_02_2012` | high |  |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_29_02_2012` | high |  |
| `aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_1502201` | high |  |
| `aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_2802201` | high |  |
| `aixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_2802201` | high |  |
| `aixa_001_2015_11_novembro_pmcmv_3_relatório_executivo_30112015` | high |  |
| `aixa_002_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015` | high |  |
| `atorio_executivo_30092012_v3_pmcmv_relatório_executivo_3009201` | high |  |
| `caixa_001_2009_06_junho_2009_pmcmv_09_06_09___caixa` | high |  |
| `caixa_001_2009_06_junho_2009_pmcmv_19_06_2009___caixa` | high |  |
| `caixa_001_2009_06_junho_2009_pmcmv_26_06_2009___caixa` | high |  |
| `caixa_001_2009_07_julho_2009_pmcmv_02_07_09___caixa` | high |  |
| `caixa_001_2009_07_julho_2009_pmcmv_10_07_2009___casa_civil` | high |  |
| `caixa_001_2009_07_julho_2009_pmcmv_17_07_2009___caixa` | high |  |
| `caixa_001_2009_07_julho_2009_pmcmv_24_07_2009___caixa` | high |  |
| `caixa_001_2009_07_julho_2009_pmcmv_31_07_2009___caixa` | high |  |
| `caixa_001_2009_08_agosto_2009_pmcmv_07_08_2009___caixa` | high |  |
| `caixa_001_2009_08_agosto_2009_pmcmv_14_08_2009___caixa` | high |  |
| `caixa_001_2009_08_agosto_2009_pmcmv_20_08_2009___caixa` | high |  |
| `caixa_001_2009_08_agosto_2009_pmcmv_28_08_2009___caixa` | high |  |
| `caixa_001_2009_09_setembro_2009_pmcmv_04_09_2009___caixa` | high |  |
| `caixa_001_2009_09_setembro_2009_pmcmv_11_09_2009___caixa` | high |  |
| `caixa_001_2009_09_setembro_2009_pmcmv_17_09_2009___caixa` | high |  |
| `caixa_001_2009_09_setembro_2009_pmcmv_25_09_2009___caixa` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_09_10_2009___caixa` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_16_10_2009___caixa` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_23_10_2009___caixa` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_30_10_2009___caixa` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_gráfico` | high |  |
| `caixa_001_2009_11_novembro_2009_pmcmv_06_11_2009___caixa` | high |  |
| `caixa_001_2009_11_novembro_2009_pmcmv_16_11_2009` | high |  |
| `caixa_001_2009_11_novembro_2009_pmcmv_20_11_2009___caixa` | high |  |
| `caixa_001_2009_11_novembro_2009_pmcmv_30_11_2009___caixa` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_07_12_2009` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_14_12_2009` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_18_12_2009` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_20091207cc_análise` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte1` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_31_12_2009` | high |  |
| `caixa_001_2009_12_dezembro_2009_pmcmv_31_12_2009_2` | high |  |
| `caixa_001_2009_pmcmv_31_12_2009` | high |  |
| `caixa_001_2010_01__janeiro_2010_pmcmv_26_01_2010` | high |  |
| `caixa_001_2010_02___fevereiro_2010_pmcmv_05_02_2010___caixa` | high |  |
| `caixa_001_2010_02___fevereiro_2010_pmcmv_12_02_2010___caixa` | high |  |
| `caixa_001_2010_02___fevereiro_2010_pmcmv_19_02_10_caixa` | high |  |
| `caixa_001_2010_02___fevereiro_2010_pmcmv_20100219` | high |  |
| `caixa_001_2010_02___fevereiro_2010_pmcmv_20100226` | high |  |
| `caixa_001_2010_03___marco_2010_pmcmv_20100305` | high |  |
| `caixa_001_2010_03___marco_2010_pmcmv_20100312_cc` | high |  |
| `caixa_001_2010_03___marco_2010_pmcmv_20100319_cc` | high |  |
| `caixa_001_2010_04___abril_2010_pmcmv_15_04_2010` | high |  |
| `caixa_001_2010_04___abril_2010_pmcmv_20100430` | high |  |
| `caixa_001_2010_04___abril_2010_pmcmv_23_04_2010` | high |  |
| `caixa_001_2010_05___maio_2010_pmcmv_20100507` | high |  |
| `caixa_001_2010_05___maio_2010_pmcmv_20100521` | high |  |
| `caixa_001_2010_05___maio_2010_pmcmv_20100527` | high |  |
| `caixa_001_2010_06___junho_2010_pmcmv30062010_1semde2010` | high |  |
| `caixa_001_2010_06___junho_2010_pmcmv_04062010` | high |  |
| `caixa_001_2010_06___junho_2010_pmcmv_11062010` | high |  |
| `caixa_001_2010_07___julho_2010_pmcmv23072010` | high |  |
| `caixa_001_2010_07___julho_2010_pmcmv30062010_1semde2010` | high |  |
| `caixa_001_2010_07___julho_2010_pmcmv30072010` | high |  |
| `caixa_001_2010_07___julho_2010_pmcmv_16_07_2010` | high |  |
| `caixa_001_2010_07___julho_2010_pmcmv_30_07_2010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv06082010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv13082010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv20082010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv30082010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv_06_08_2010` | high |  |
| `caixa_001_2010_08___agosto_2010_pmcmv_13_08_2010` | high |  |
| `caixa_001_2010_09___setembro_2010_pmcmv06092010` | high |  |
| `caixa_001_2010_09___setembro_2010_pmcmv10092010` | high |  |
| `caixa_001_2010_09___setembro_2010_pmcmv21092010` | high |  |
| `caixa_001_2010_09___setembro_2010_pmcmv24092010` | high |  |
| `caixa_001_2010_10___outubro_2010_pmcmv15102010v1` | high |  |
| `caixa_001_2010_10___outubro_2010_pmcmv25102010` | high |  |
| `caixa_001_2010_11___novembro_2010_pmcmv_19112010` | high |  |
| `caixa_001_2010_11___novembro_2010_pmcmv_26112010` | high |  |
| `caixa_001_2010_11___novembro_2010_pmcmv_automatizado03112010` | high |  |
| `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_15122010` | high |  |
| `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_15_12_2010` | high |  |
| `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_20122010` | high |  |
| `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_20_12_2010` | high |  |
| `caixa_001_2010_12___dezembro_2010_pmcmv_pj_07_12_2010` | high |  |
| `caixa_001_2011_03_março_pmcmv_14032011` | high |  |
| `caixa_001_2011_04_abril_pmcmv_14042011` | high |  |
| `caixa_001_2011_05_maio__pmcmv_presidencia_2` | high |  |
| `caixa_001_2011_05_maio_pmcmv_20110531` | high |  |
| `caixa_001_2011_05_maio_pmcmv_2_20110531` | high |  |
| `caixa_001_2011_05_maio_pmcmv_acumulado` | high |  |
| `caixa_001_2011_05_maio_presidencia` | high |  |
| `caixa_001_2011_05_maio_presidencia1__16_05_2011` | high |  |
| `caixa_001_2011_06_junho_pmcmv_20110624` | high |  |
| `caixa_001_2011_06_junho_pmcmv_automatizado_continua_24062011` | high |  |
| `caixa_001_2011_07_julho_pmcmv_20110722` | high |  |
| `caixa_001_2011_07_julho_pmcmv_automatizado_continua_04072011` | high |  |
| `caixa_001_2011_07_julho_pmcmv_automatizado_continua_15072011` | high |  |
| `caixa_001_2011_07_julho_pmcmv_automatizado_continua_22072011` | high |  |
| `caixa_001_2011_07_julho_pmcmv_automatizado_novo_01082011` | high |  |
| `caixa_001_2011_07_julho_pmcmv_automatizado_novo_28072011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_20110826` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_acumulado_09_08_2011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_continua_09082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_01082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_09082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_15082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_19082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_26082011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_ii_09_08_2011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_ii_15_08_2011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_ii_19_08_2011` | high |  |
| `caixa_001_2011_08_agosto_pmcmv_ii_26_08_2011` | high |  |
| `caixa_001_2011_09_setembro_pmcmv_automatizado_novo_29092011` | high |  |
| `caixa_001_2011_09_setembro_pmcmv_ii_09092011` | high |  |
| `caixa_001_2011_09_setembro_pmcmv_ii_16092011` | high |  |
| `caixa_001_2011_09_setembro_pmcmv_ii_23092011` | high |  |
| `caixa_001_2011_09_setembro_pmcmv_ii_29092011` | high |  |
| `caixa_001_2011_10_outubro_pmcmv_20111025` | high |  |
| `caixa_001_2011_10_outubro_pmcmv_automatizado_novo_13102011` | high |  |
| `caixa_001_2011_10_outubro_pmcmv_automatizado_novo_20102011` | high |  |
| `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_23012012` | high |  |
| `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_27012012` | high |  |
| `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012` | high |  |
| `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_09_03_2012` | high |  |
| `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_16_03_2012` | high |  |
| `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012` | high |  |
| `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_31_03_2012` | high |  |
| `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_18_04_2012` | high |  |
| `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_24_04_2012` | high |  |
| `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_30_04_2012` | high |  |
| `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_07_05_2012` | high |  |
| `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_14_05_2012` | high |  |
| `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_28_05_2012` | high |  |
| `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_31_05_2012` | high |  |
| `caixa_001_2012_06_junho_pmcmv_relatorio_executivo_30_06_2012` | high |  |
| `caixa_001_2012_06_junho_pmcmv_relatorio_executivo_30_06_2012v2` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_11_07_2012` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_11_07_2012_bd` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_15_07_2012` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatório_executivo_24_07_12` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatório_executivo_31072012_v2` | high |  |
| `caixa_001_2012_07_julho_pmcmv_relatório_executivo_31_07_12` | high |  |
| `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_06_08_12` | high |  |
| `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_14_08_12` | high |  |
| `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_20_08_12` | high |  |
| `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_24_08_12` | high |  |
| `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_31_08_12` | high |  |
| `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_10_09_12` | high |  |
| `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_26092012` | high |  |
| `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_30092012` | high |  |
| `caixa_001_2012_09_setembro_rel_executivo_resumo_15092012` | high |  |
| `caixa_001_2012_09_setembro_rel_executivo_resumo_26092012` | high |  |
| `caixa_001_2012_10_outubro_pmcmv_relatório_executivo_31102012` | high |  |
| `caixa_001_2012_10_outubro_rel_executivo_resumo_31102012` | high |  |
| `caixa_001_2012_11_novembro_pmcmv_relat_rio_executivo_12112012` | high |  |
| `caixa_001_2012_11_novembro_pmcmv_relatório_executivo_28112012` | high |  |
| `caixa_001_2012_12_dezembro_pmcmv_relatório_executivo_21122012` | high |  |
| `caixa_001_2012_12_dezembro_pmcmv_relatório_executivo_31122012` | high |  |
| `caixa_001_2012_12_dezembro_rel_executivo_resumo_21122012` | high |  |
| `caixa_001_2013_01___janeiro_pmcmv_relatório_executivo_31012013` | high |  |
| `caixa_001_2013_01___janeiro_rel_executivo_resumo_31012013` | high |  |
| `caixa_001_2013_02___fevereiro_rel_executivo_resumo_15022013` | high |  |
| `caixa_001_2013_02___fevereiro_rel_executivo_resumo_15022013_v2` | high |  |
| `caixa_001_2013_02___fevereiro_rel_executivo_resumo_28022013` | high |  |
| `caixa_001_2013_03___março_pmcmv_relat_rio_executivo_28032013` | high |  |
| `caixa_001_2013_03___março_pmcmv_relatório_executivo_15032013` | high |  |
| `caixa_001_2013_03___março_pmcmv_relatório_executivo_28032013` | high |  |
| `caixa_001_2013_03___março_rel_executivo_resumo_15032013` | high |  |
| `caixa_001_2013_03___março_rel_executivo_resumo_28032013` | high |  |
| `caixa_001_2013_04___abril_pmcmv_relat_rio_executivo_30042013` | high |  |
| `caixa_001_2013_04___abril_pmcmv_relatório_executivo_30042013` | high |  |
| `caixa_001_2013_04___abril_rel_executivo_resumo_30042013` | high |  |
| `caixa_001_2013_04___abril_rel_executivo_resumo_30042013_v1` | high |  |
| `caixa_001_2013_05___maio_pmcmv_relat_rio_executivo_150513` | high |  |
| `caixa_001_2013_05___maio_rel_executivo_resumo_150513` | high |  |
| `caixa_001_2013_12___dezembro_rel_executivo_resumo` | high |  |
| `caixa_001_2014_11_novembro_pmcmv_relatório_executivo_15112014` | high |  |
| `caixa_001_2014_11_novembro_pmcmv_relatório_executivo_30112014` | high |  |
| `caixa_001_2014_11_novembro_rel_executivo_resumo_15112014` | high |  |
| `caixa_001_2014_11_novembro_rel_executivo_resumo_comerc_30112014` | high |  |
| `caixa_001_2014_12_dezembro_rel_executivo_resumo_31122014` | high |  |
| `caixa_001_2014_rel_executivo_resumo_28022015` | high |  |
| `caixa_001_2014_rel_executivo_resumo_31012015` | high |  |
| `caixa_001_2015_01_janeiro_pmcmv_3_relatório_executivo_31012015` | high |  |
| `caixa_001_2015_02_fevereiro_rel_executivo_resumo_28022015` | high |  |
| `caixa_001_2015_03_marco_pmcmv_3_relatório_executivo_31032015` | high |  |
| `caixa_001_2015_03_marco_rel_executivo_resumo_2015_31032015v2` | high |  |
| `caixa_001_2015_03_marco_rel_executivo_resumo_31032015` | high |  |
| `caixa_001_2015_04_abril_pmcmv_3_relatório_executivo_30042015` | high |  |
| `caixa_001_2015_04_abril_rel_executivo_resumo_30042015` | high |  |
| `caixa_001_2015_05_maio_pmcmv_3_relat_rio_executivo_31052015` | high |  |
| `caixa_001_2015_05_maio_rel_executivo_resumo_31052015` | high |  |
| `caixa_001_2015_06_junho_pmcmv_3_relatório_executivo_30062015` | high |  |
| `caixa_001_2015_06_junho_rel_executivo_resumo_30062015` | high |  |
| `caixa_001_2015_07_julho_pmcmv_3_relatório_executivo_31072015` | high |  |
| `caixa_001_2015_07_julho_rel_executivo_resumo_31072015` | high |  |
| `caixa_001_2015_10_outubro_pmcmv_3_relatório_executivo31102015` | high |  |
| `caixa_001_2015_10_outubro_pmcmv_3_relatório_executivo_31102015` | high |  |
| `caixa_001_2015_11_novembro_pmcmv_3_relatório_executivo_3011201` | high |  |
| `caixa_001_2015_11_novembro_rel_executivo_resumo_30112015` | high |  |
| `caixa_001_2015_12_dezembro_rel_executivo_resumo_31122015v2` | high |  |
| `caixa_001_2016_pmcmv_3_relatório_executivo_30042016` | high |  |
| `caixa_001_2016_pmcmv_3_relatório_executivo_31122016` | high |  |
| `caixa_001_2016_rel_executivo_resumo_30042016` | high |  |
| `caixa_001_2016_rel_executivo_resumo_31082016` | high |  |
| `caixa_001_2016_rel_executivo_resumo_31122016` | high |  |
| `caixa_001_2017_rel_executivo_resumo_dez17` | high |  |
| `caixa_001_2018_pmcmv_3_relatório_executivo_ago2018` | high |  |
| `caixa_001_2018_rel_executivo_resumo_ago2018` | high |  |
| `caixa_002_2015_08_agosto_pmcmv_3_relatório_executivo31082015` | high |  |
| `caixa_002_2015_08_agosto_pmcmv_3_relatório_executivo_12082015` | high |  |
| `caixa_002_2015_08_agosto_rel_executivo_resumo_12082015` | high |  |
| `caixa_002_2015_08_agosto_rel_executivo_resumo_31082015` | high |  |
| `caixa_002_2015_12_dezembro_pmcmv_3_relatório_executivo_3112201` | high |  |
| `caixa_002_2015_12_dezembro_rel_executivo_resumo_31122015` | high |  |
| `caixa_002_2017_pmcmv_3_relat_rio_executivo_abr2017` | high |  |
| `caixa_002_2017_pmcmv_3_relat_rio_executivo_ago2017` | high |  |
| `caixa_002_2017_pmcmv_3_relat_rio_executivo_fev2017` | high |  |
| `caixa_002_2017_pmcmv_3_relatório_executivo_dez17` | high |  |
| `caixa_002_2017_rel_executivo_resumo_ago2017` | high |  |
| `caixa_002_2017_rel_executivo_resumo_fev2017` | high |  |
| `caixa_002_2018_pmcmv_3_relat_rio_executivo_abr2018` | high |  |
| `caixa_002_2018_pmcmv_3_relat_rio_executivo_abr2018_v2` | high |  |
| `caixa_002_2018_pmcmv_3_relat_rio_executivo_jun2018` | high |  |
| `caixa_002_2018_pmcmv_3_relat_rio_executivo_mai2018` | high |  |
| `caixa_002_2018_pmcmv_3_relat_rio_executivo_mar18` | high |  |
| `caixa_002_2018_rel_executivo_resumo_abr2018` | high |  |
| `caixa_002_2018_rel_executivo_resumo_abr2018_v2` | high |  |
| `caixa_002_2018_rel_executivo_resumo_jun2018` | high |  |
| `caixa_002_2018_rel_executivo_resumo_mai2018` | high |  |
| `caixa_002_2018_rel_executivo_resumo_mar2018` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_31012017` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_jul2017` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_jun2017` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_mai2017` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_mar2017` | high |  |
| `caixa_003_2017_pmcmv_3_relat_rio_executivo_nov17` | high |  |
| `caixa_003_2017_rel_executivo_resumo_31012017` | high |  |
| `caixa_003_2017_rel_executivo_resumo_jul2017` | high |  |
| `caixa_003_2017_rel_executivo_resumo_jun2017` | high |  |
| `caixa_003_2017_rel_executivo_resumo_mai2017` | high |  |
| `caixa_003_2017_rel_executivo_resumo_mar2017` | high |  |
| `caixa_003_2017_rel_executivo_resumo_nov17` | high |  |
| `elatorio_executivo_31102014_pmcmv_relatório_executivo_31102014` | high |  |
| `ixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_15022013` | high |  |
| `ixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_28022013` | high |  |
| `ixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_28022015` | high |  |
| `ixa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v` | high |  |
| `julho_pmcmv_automatizado_continua_15072011_pmcmv__pr___15072011` | high |  |
| `matizado_continua_15072011_pmcmv_automatizado_continua_15072011` | high |  |
| `o_relatorio_executivo_30092012_v3_rel_executivo_resumo_30092012` | high |  |
| `relatorio_executivo_31102014_pmcmv_relatório_executivo_3110201` | high |  |
| `torio_executivo_18_05_2012_pmcmv_relatorio_executivo_18_05_2012` | high |  |
| `torio_executivo_30092012_v3_pmcmv_relatório_executivo_30092012` | high |  |
| `torio_executivo_31_05_2012_pmcmv_relatorio_executivo_31_05_2012` | high |  |
| `ubro_relatorio_executivo_31102014_rel_executivo_resumo_31102014` | high |  |
| `xa_001_2013_12___dezembro_pmcmv_relat_rio_executivo_31122013_v2` | high |  |
| `xa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v2` | high |  |

</details>

### Sub-tabelas (Tipo 2) (`sub_tabelas_2`)

**Quantidade:** 1 tabela (0.1% do total)
**Confiança:** 1 high

**Descrição:** Cabeçalho com todas as colunas nomeadas `unnamed_N` e estrutura de sub-tabelas separadas por linhas vazias. Contém sub-tabelas com cabeçalhos específicos: "SÍNTESE", "Faixa" com UH contratadas/entregues, "Renda" com distribuição por faixa, "Região" com agregação regional, e "Quadro de Valores do MCMV".

**Heurística de identificação:** R5 (classificar_sub_tabelas): detecta que a maioria das colunas são `unnamed_` com índices numéricos e identifica as palavras-chave específicas das sub-tabelas. Confirma o padrão de sub-tabelas aninhadas com cabeçalhos explícitos no conteúdo.

**Exemplo:** `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas`

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas` | high |  |

</details>

### Sub-tabelas (Tipo 3) (`sub_tabelas_3`)

**Quantidade:** 19 tabelas (2.5% do total)
**Confiança:** 19 high

**Descrição:** Possui sub-tabelas separadas por uma única linha vazia. Cabeçalho inclui um nome descritivo como "relatório_01_total_das_contratações_por_ufregião" e outras colunas `unnamed`. Contém agregações por UF/região e totais, com cabeçalhos compostos dentro das sub-tabelas.

**Heurística de identificação:** R5 (classificar_sub_tabelas): detecta o padrão de uma coluna com nome descritivo + colunas `unnamed` e sub-tabelas separadas por apenas uma linha vazia. Identifica a estrutura de múltiplos relatórios dentro da mesma tabela com cabeçalhos próprios.

**Exemplo:** `bb_2011_01_janeiro_rel_11jan2011`

<details>
<summary>Ver todas as 19 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2011_01_janeiro_rel_11jan2011` | high |  |
| `bb_2011_01_janeiro_rel_25jan2011` | high |  |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_01_02_2011` | high |  |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_08_02_2011` | high |  |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_16_02_2011` | high |  |
| `bb_2011_03_março_relatorio_min_cidades___22mar11` | high |  |
| `bb_2011_04_abril_relatorio_min_cidades___18abr11` | high |  |
| `bb_2011_04_abril_relatorio_min_cidades___26abr11` | high |  |
| `bb_2011_05_maio_relatorio_min_cidades___03mai11` | high |  |
| `bb_2011_05_maio_relatorio_min_cidades___10mai11` | high |  |
| `bb_2011_05_maio_relatorio_min_cidades___17mai11` | high |  |
| `bb_2011_05_maio_relatorio_min_cidades___24mai11` | high |  |
| `bb_2011_06_junho_relatorio_min_cidades___07jun11` | high |  |
| `bb_2011_07_julho_relatorio_min__cidades_19jul11` | high |  |
| `bb_2011_07_julho_relatorio_min_cidades___12jul11` | high |  |
| `caixa_001_2009_10_outubro_2009_pmcmv_08_10_2009___caixa` | high |  |
| `caixa_001_2010_08___agosto_2010_municípios_contratados_1308` | high |  |
| `caixa_001_2016_grafico_mcmv_31082016` | high |  |
| `mbro_2009_relatorio_automatizado_31dez09final_com_base_valor_mc` | high |  |

</details>

### Sub-tabelas (Tipo 4) (`sub_tabelas_4`)

**Quantidade:** 1 tabela (0.1% do total)

**Descrição:** Possui sub-tabelas em formato diferente das anteriores. Todas as colunas são `unnamed`. Sub-tabelas são separadas por uma única linha vazia e possuem cabeçalhos compostos (4-5 linhas cada). Última linha contém observação sobre UH entregues e frente do PMCMV.

**Heurística de identificação:** R5 (classificar_sub_tabelas): detecta estrutura de sub-tabelas com cabeçalhos profundos (múltiplas linhas) e todas as colunas `unnamed`. Confidence medium devido à complexidade e variação do padrão.

**Exemplo:** `caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12` *(confidence: medium)*

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12` | medium |  |

</details>

### Separador Pipe (|) (`separador_|`)

**Quantidade:** 5 tabelas (0.7% do total)
**Confiança:** 5 high

**Descrição:** Tabela cujas linhas possuem dados concatenados e separados pelo caractere "|" (pipe). Os dados estão essencialmente em formato delimitado dentro de células únicas, exigindo parsing adicional.

**Heurística de identificação:** R3 (r3_separador_pipe): verifica se as primeiras linhas de dados contêm o caractere "|" como separador entre valores. Detectado antes da análise de cabeçalho (R3b) pois é um padrão estrutural distinto.

**Exemplo:** `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras`

<details>
<summary>Ver todas as 5 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_proponentes` | high |  |
| `bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas` | high |  |

</details>

### Vazia (`vazia`)

**Quantidade:** 1 tabela (0.1% do total)
**Confiança:** 1 high

**Descrição:** Tabela sem dados ou com conteúdo insignificante. Identificada pelo tamanho do arquivo (< 5KB) e número de colunas (0 ou 1), indicando que não há informações úteis para processamento.

**Heurística de identificação:** R1 (r1_vazia): verifica `file_size < 5KB` E `n_cols ≤ 1`. É a segunda regra na árvore de decisão (após R2), capturando tabelas que não contêm dados estruturados aproveitáveis.

**Exemplo:** `caixa_001_2016_bext_31102016`

<details>
<summary>Ver todas as 1 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `caixa_001_2016_bext_31102016` | high |  |

</details>

### Dados Sem Utilidade (`dados_sem_utilidade`)

**Quantidade:** 4 tabelas (0.5% do total)
**Confiança:** 4 high

**Descrição:** Tabelas sem dados de interesse para a análise, identificadas pelo nome do arquivo. São tabelas de infraestrutura do banco de dados (ex.: `tab_arquivos_dados`, `loginfesta`) ou templates de relatório sem dados reais. Excluídas do processamento.

**Heurística de identificação:** R2 (r2_dados_sem_utilidade): compara o nome da tabela contra uma lista de padrões conhecidos (`PADROES_SEM_UTILIDADE`): `tab_arquivos_dados`, `loginfesta`, `novo_relat_rio_executivo`. É a primeira regra executada na árvore de decisão.

**Exemplo:** `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados`

<details>
<summary>Ver todas as 4 tabelas</summary>

| Tabela | Confidence | Observações |
|---|---|---|
| `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados` | high |  |
| `bb_2015_08_agosto_loginfesta` | high |  |
| `caixa_002_2018_novo_relat_rio_executivo` | high |  |
| `caixa_002_2018_novo_relat_rio_executivo_maio2018` | high |  |

</details>

---

## Árvore de Decisão

A classificação segue a árvore de decisão implementada:

```
                              ┌─────────────────────┐
                              │      ENTRADA:        │
                              │  tabela (nome+df)    │
                              └──────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   Caso especial:     │
                              │ "dados_22022011"?    │─── sim ──► cabecalho_na_primeira_linha_2
                              └──────────┬──────────┘
                                         │ não
                              ┌──────────▼──────────┐
                              │   R2: tabela em      │
                              │ padrões conhecidos?  │─── sim ──► dados_sem_utilidade
                              └──────────┬──────────┘
                                         │ não
                              ┌──────────▼──────────┐
                              │   R1: arquivo < 5KB  │
                              │    e ≤ 1 coluna?     │─── sim ──► vazia
                              └──────────┬──────────┘
                                         │ não
                              ┌──────────▼──────────┐
                              │   R3: separador "|"  │
                              │    nos dados?        │─── sim ──► separador_|
                              └──────────┬──────────┘
                                         │ não
                              ┌──────────▼──────────┐
                              │ R3a + R3b: análise   │
                              │  do cabeçalho        │
                              └──────────┬──────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
     HEADER_IS_REAL              HEADER_IS_DATA               AMBIGUOUS
              │                          │                          │
   ┌──────────▼──────────┐    ┌──────────▼──────────┐    ┌──────────▼──────────────┐
   │ R4: consistência de │    │ R5: estrutura de    │    │ Padrão "Posicao:" +     │
   │ tipos entre colunas?│    │ sub-tabelas?        │    │ data DD/MM/AAAA?        │
   └──────────┬──────────┘    └──────────┬──────────┘    └──────────┬──────────────┘
              │                          │                          │
     ┌────────┴────────┐        ┌────────┴────────┐        ┌────────┴────────┐
     │sim           │não        │sim               │não     │sim              │não
     ▼              ▼           ▼                  ▼        ▼                 │
  bem_formada  bem_formada  sub_tabelas_    ┌───────────┐ cabecalho_na_      │
  (high)       (medium)     1 / 2 / 3 / 4   │ densidade  │ segunda_linha     │
                                            │ > 15%?     │                   │
                                            └──┬────┬────┘                   │
                                         sim    │    │ não                    │
                                          ▼      │    ▼                      │
                                   nao_colunares │ sem_cabecalho              │
                                   _tipo1        │                            │
                                                 │                            │
                              ┌──────────────────┘                            │
                              │  (continua AMBIGUOUS)                         │
                              ▼                                               │
                   ┌──────────────────────┐                                   │
                   │ R5: estrutura de     │◄──────────────────────────────────┘
                   │ sub-tabelas?         │
                   └──────────┬───────────┘
                              │
                    ┌─────────┴─────────┐
                    │sim                │não
                    ▼                   ▼
             sub_tabelas_        ┌───────────────────────┐
             1 / 2 / 3 / 4       │ R6: cabeçalho         │
                                 │ composto?             │
                                 └───────────┬───────────┘
                                             │
                                   ┌─────────┴─────────┐
                                   │sim                │não
                                   ▼                   ▼
                            cabecalho_composto_  ┌────────────────────────┐
                            1 / 2                │ R7: cabeçalho          │
                                                 │ deslocado?             │
                                                 └────────────┬───────────┘
                                                              │
                                                    ┌─────────┴─────────┐
                                                    │sim                │não
                                                    ▼                   ▼
                                             cabecalho_na_      ┌───────────────────┐
                                             primeira_linha_1/2  │ R8: fallback      │
                                             cabecalho_na_       │ (densidade)       │
                                             terceira_linha_1/2   └─────────┬─────────┘
                                                                           │
                                                                 ┌─────────┴─────────┐
                                                                 │empty_ratio > 15%? │
                                                                 └─────────┬─────────┘
                                                                           │
                                                                   ┌───────┴───────┐
                                                                   │sim            │não
                                                                   ▼               ▼
                                                            nao_colunares_    sem_cabecalho
                                                            tipo1
```

### Resumo das regras

| Regra | Condição | Categoria de saída |
|---|---|---|
| Especial | `table_name` contém `"dados_22022011"` | `cabecalho_na_primeira_linha_2` |
| R2 | Nome da tabela em padrões conhecidos (`tab_arquivos_dados`, `loginfesta`, `novo_relat_rio_executivo`) | `dados_sem_utilidade` |
| R1 | `file_size < 5KB` E `n_cols ≤ 1` | `vazia` |
| R3 | Caractere `\|` presente nas primeiras linhas de dados | `separador_\|` |
| R3a+R3b → R4 | HEADER_IS_REAL (colunas com nomes reais) + consistência de tipos entre colunas | `bem_formada` |
| R3a+R3b → R5 | HEADER_IS_DATA: verifica sub-tabelas; se não, avalia densidade | `sub_tabelas_*` / `sem_cabecalho` / `nao_colunares_tipo1` |
| "Posicao:" | Primeira linha começa com `"Posicao:"` + data `DD/MM/AAAA` | `cabecalho_na_segunda_linha` |
| R5 | Estrutura de sub-tabelas (palavras-chave, timestamps, colunas `unnamed`) | `sub_tabelas_1` a `sub_tabelas_4` |
| R6 | Cabeçalho composto (2-3 linhas para formar nomes de colunas) | `cabecalho_composto_1` / `cabecalho_composto_2` |
| R7 | Cabeçalho deslocado (linhas vazias + nomes em linha N) | `cabecalho_na_primeira_linha_1/2` / `cabecalho_na_terceira_linha_1/2` |
| R8 | Fallback: `empty_ratio > 15%`? | `nao_colunares_tipo1` ou `sem_cabecalho` |

> **Nota:** A categoria `indeterminada` é reservada para tabelas que falham no carregamento (erro de parsing). Nenhuma tabela na amostra atual recebeu esta classificação.
> A categoria `nao_colunares_tipo1` não teve ocorrências na amostra atual — as tabelas com dados esparsos foram capturadas por outras regras (principalmente sub-tabelas ou cabeçalhos deslocados) antes de alcançar R8.
