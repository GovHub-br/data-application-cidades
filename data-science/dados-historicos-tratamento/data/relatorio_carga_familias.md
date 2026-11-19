# Relatório de revisão — classificação de carga por família

Artefato gerado por `scripts/classificar_carga_drift.py` para revisão
humana das fronteiras de família e da classificação full×incremental.

## Fonte de dados

- Colunas: banco de dados (`information_schema.columns`, schema `dados_historicos`)
- Contagens: `exato_count_star`

## Resumo

- Tabelas: **754**
- Famílias: **577** (multi-versão: **65**, versão única: **512**)
- Distribuição `modelo_carga`: **{'full_refresh': 751, 'incremental': 3}**

## Famílias multi-versão (revisão de trajetória)

Legenda de classificação: `nao_monotona(...)` = snapshot (flutua/constante); `crescimento_nao_consistente_com_append` = monotônico mas com salto (recomputo de snapshot); `append_monotono_estavel` = candidato a incremental (≥ 3 versões, crescimento estável).

### 2012_10_outubro_20121009_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `18244 → 18352`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=18244->18352; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-10-09 | `01_2012_10_outubro_20121009_bases_relatório_executivo_09102012` | 18244 |
| 2012-10-09 | `01_2012_10_outubro_20121009_bases_relatório_executivo_16102012` | 18352 |

### abril_bases_relatório_executivo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `23046 → 36882 → 37096`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=23046->36882->37096; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-04-30 | `caixa_001_2013_04___abril_bases_relatório_executivo_30042013` | 23046 |
| 2015-04-30 | `caixa_001_2015_04_abril_bases_relatório_executivo_30042015` | 36882 |
| 2015-04-30 | `caixa_001_2015_04_abril_bases_relatório_executivo_30042015_v2` | 37096 |

### abril_rel_executivo_resumo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-04-30 | `caixa_001_2013_04___abril_rel_executivo_resumo_30042013` | 536 |
| 2013-04-30 | `caixa_001_2013_04___abril_rel_executivo_resumo_30042013_v1` | 536 |
| 2015-04-30 | `caixa_001_2015_04_abril_rel_executivo_resumo_30042015` | 536 |

### agosto_bases_relatório_executivo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `37998 → 38246 → 42056`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=37998->38246->42056; fonte_contagem=exato_count_star; nome_periodo=YYYY:2015

| período | tabela | n_linhas |
|---|---|---|
| 2015-08-12 | `caixa_002_2015_08_agosto_bases_relatório_executivo_12082015` | 37998 |
| 2015-08-31 | `caixa_002_2015_08_agosto_bases_relatório_executivo_31082015` | 38246 |
| 2016-08-31 | `caixa_002_2016_08_agosto_bases_relatório_executivo_31082016` | 42056 |

### agosto_pmcmv_automatizado_novo — `full_refresh`

- Versões: **5**
- Trajetória de contagens: `536 → 536 → 536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-08-01 | `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_01082011` | 536 |
| 2011-08-09 | `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_09082011` | 536 |
| 2011-08-15 | `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_15082011` | 536 |
| 2011-08-19 | `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_19082011` | 536 |
| 2011-08-26 | `caixa_001_2011_08_agosto_pmcmv_automatizado_novo_26082011` | 536 |

### agosto_rel_executivo_resumo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2015

| período | tabela | n_linhas |
|---|---|---|
| 2015-08-12 | `caixa_002_2015_08_agosto_rel_executivo_resumo_12082015` | 536 |
| 2015-08-31 | `caixa_002_2015_08_agosto_rel_executivo_resumo_31082015` | 536 |

### bases_relat_rio_executivo_abr2018 — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `51146 → 51146`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=51146->51146; fonte_contagem=exato_count_star; nome_periodo=YYYY:2018

| período | tabela | n_linhas |
|---|---|---|
| 2018-01-01 | `caixa_002_2018_bases_relat_rio_executivo_abr2018` | 51146 |
| 2018-01-01 | `caixa_002_2018_bases_relat_rio_executivo_abr2018_v2` | 51146 |

### bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `40054 → 42672`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=40054->42672; fonte_contagem=exato_count_star; nome_periodo=YYYY:2016

| período | tabela | n_linhas |
|---|---|---|
| 2016-04-30 | `caixa_001_2016_bases_relatório_executivo_30042016` | 40054 |
| 2016-12-31 | `caixa_002_2016_bases_relatório_executivo_31122016` | 42672 |

### bb_2012_10_outubro_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `604 → 604`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=604->604; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-10-23 | `bb_2012_10_outubro_entrada_bb_20121023` | 604 |
| 2012-10-31 | `bb_2012_10_outubro_entrada_bb_20121031` | 604 |

### bb_2012_11_novembro_entrada_bb — `full_refresh`

- Versões: **5**
- Trajetória de contagens: `604 → 604 → 604 → 308 → 308`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=604->604->604->308->308; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-11-06 | `bb_2012_11_novembro_entrada_bb_20121106` | 604 |
| 2012-11-13 | `bb_2012_11_novembro_entrada_bb_20121113` | 604 |
| 2012-11-19 | `bb_2012_11_novembro_entrada_bb_20121119` | 604 |
| 2012-11-27 | `bb_2012_11_novembro_entrada_bb_20121127` | 308 |
| 2012-11-30 | `bb_2012_11_novembro_entrada_bb_20121130` | 308 |

### bb_2012_12_dezembro_entrada_bb — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `308 → 604 → 516`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=308->604->516; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-12-10 | `bb_2012_12_dezembro_entrada_bb_20121210` | 308 |
| 2012-12-17 | `bb_2012_12_dezembro_entrada_bb_20121217` | 604 |
| 2012-12-28 | `bb_2012_12_dezembro_entrada_bb_20121228` | 516 |

### bb_2013_01_janeiro_entrada_bb — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `604 → 516 → 516 → 516`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=604->516->516->516; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-01-07 | `bb_2013_01_janeiro_entrada_bb_20130107` | 604 |
| 2013-01-13 | `bb_2013_01_janeiro_entrada_bb_20130113` | 516 |
| 2013-01-21 | `bb_2013_01_janeiro_entrada_bb_20130121` | 516 |
| 2013-01-28 | `bb_2013_01_janeiro_entrada_bb_20130128` | 516 |

### bb_2013_02_fevereiro_entrada_bb — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `516 → 520 → 516`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=516->520->516; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-02-05 | `bb_2013_02_fevereiro_entrada_bb_20130205` | 516 |
| 2013-02-19 | `bb_2013_02_fevereiro_entrada_bb_20130219` | 520 |
| 2013-02-26 | `bb_2013_02_fevereiro_entrada_bb_20130226` | 516 |

### bb_2013_03_marco_entrada_bb — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `604 → 604 → 516 → 604`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=604->604->516->604; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-03-05 | `bb_2013_03_marco_entrada_bb_20130305` | 604 |
| 2013-03-18 | `bb_2013_03_marco_entrada_bb_20130318` | 604 |
| 2013-04-01 | `bb_2013_03_marco_entrada_bb_20130401` | 516 |
| 2013-04-15 | `bb_2013_03_marco_entrada_bb_20130415` | 604 |

### bb_2013_05_maio_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1232 → 1232`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1232->1232; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-05-06 | `bb_2013_05_maio_entrada_bb_20130506` | 1232 |
| 2013-05-13 | `bb_2013_05_maio_entrada_bb_20130513` | 1232 |

### bb_2013_06_junho_entrada_bb — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `1232 → 1232 → 1232`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1232->1232->1232; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-06-04 | `bb_2013_06_junho_entrada_bb_20130604` | 1232 |
| 2013-06-18 | `bb_2013_06_junho_entrada_bb_20130618` | 1232 |
| 2013-06-30 | `bb_2013_06_junho_entrada_bb_20130630` | 1232 |

### bb_2013_08_agosto_entrada_bb — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `1232 → 1232 → 1232`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1232->1232->1232; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-08-02 | `bb_2013_08_agosto_entrada_bb_20130802` | 1232 |
| 2013-08-19 | `bb_2013_08_agosto_entrada_bb_20130819` | 1232 |
| 2013-09-04 | `bb_2013_08_agosto_entrada_bb_20130904` | 1232 |

### bb_2013_11_novembro_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1198 → 1198`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1198->1198; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-11-20 | `bb_2013_11_novembro_entrada_bb_20131120` | 1198 |
| 2013-12-04 | `bb_2013_11_novembro_entrada_bb_20131204` | 1198 |

### bb_2013_12_dezembro_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1026 → 1140`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=1026->1140; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-12-18 | `bb_2013_12_dezembro_entrada_bb_20131218` | 1026 |
| 2014-01-02 | `bb_2013_12_dezembro_entrada_bb_20140102` | 1140 |

### bb_2014_02_fevereiro_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1060 → 1072`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=1060->1072; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-02-17 | `bb_2014_02_fevereiro_entrada_bb_20140217` | 1060 |
| 2014-03-06 | `bb_2014_02_fevereiro_entrada_bb_20140306` | 1072 |

### bb_2014_04_abril_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1198 → 1198`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1198->1198; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-04-15 | `bb_2014_04_abril_entrada_bb_20140415` | 1198 |
| 2014-04-30 | `bb_2014_04_abril_entrada_bb_20140430` | 1198 |

### bb_2014_05_maio_entrada_bb — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1198 → 1132`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=1198->1132; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-05-16 | `bb_2014_05_maio_entrada_bb_20140516` | 1198 |
| 2014-05-30 | `bb_2014_05_maio_entrada_bb_20140530` | 1132 |

### bb_2014_06_junho_entrada_bb — `incremental`

- Versões: **3**
- Trajetória de contagens: `1198 → 1214 → 1300`
- Evidência: classificacao=append_monotono_estavel; trajetoria=1198->1214->1300; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-06-15 | `bb_2014_06_junho_entrada_bb_20140615` | 1198 |
| 2014-06-25 | `bb_2014_06_junho_entrada_bb_20140625` | 1214 |
| 2014-07-01 | `bb_2014_06_junho_entrada_bb_20140701` | 1300 |

### bb_2014_12_dezembro_pnhr — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `1946 → 2032`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=1946->2032; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-12-15 | `bb_2014_12_dezembro_pnhr_15122014` | 1946 |
| 2014-12-31 | `bb_2014_12_dezembro_pnhr_20141231` | 2032 |

### bb_2015_03_marco_pnhr — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `2042 → 3194`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=2042->3194; fonte_contagem=exato_count_star; nome_periodo=YYYY:2015

| período | tabela | n_linhas |
|---|---|---|
| 2015-03-15 | `bb_2015_03_marco_pnhr_15032015` | 2042 |
| 2015-03-31 | `bb_2015_03_marco_pnhr_31032015` | 3194 |

### bext — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `548374 → 0 → 565558`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=548374->0->565558; fonte_contagem=exato_count_star; nome_periodo=YYYY:2016

| período | tabela | n_linhas |
|---|---|---|
| 2016-04-30 | `caixa_001_2016_bext_30042016` | 548374 |
| 2016-10-31 | `caixa_001_2016_bext_31102016` | 0 |
| 2016-12-31 | `caixa_002_2016_bext_31122016` | 565558 |

### dezembro_2010_balanco_pmcmv — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2010

| período | tabela | n_linhas |
|---|---|---|
| 2010-12-15 | `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_15122010` | 536 |
| 2010-12-20 | `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_20122010` | 536 |

### dezembro_bases_relatório_executivo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `20092 → 20904 → 39200`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=20092->20904->39200; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-12-21 | `caixa_001_2012_12_dezembro_bases_relatório_executivo_21122012` | 20092 |
| 2012-12-31 | `caixa_001_2012_12_dezembro_bases_relatório_executivo_31122012` | 20904 |
| 2015-12-31 | `caixa_002_2015_12_dezembro_bases_relatório_executivo_31122015` | 39200 |

### dezembro_pmcmv_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-12-21 | `caixa_001_2012_12_dezembro_pmcmv_relatório_executivo_21122012` | 536 |
| 2012-12-31 | `caixa_001_2012_12_dezembro_pmcmv_relatório_executivo_31122012` | 536 |

### dezembro_rel_executivo_resumo — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `536 → 536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-12-21 | `caixa_001_2012_12_dezembro_rel_executivo_resumo_21122012` | 536 |
| 2013-01-01 | `caixa_001_2013_12___dezembro_rel_executivo_resumo` | 536 |
| 2014-12-31 | `caixa_001_2014_12_dezembro_rel_executivo_resumo_31122014` | 536 |
| 2015-12-31 | `caixa_002_2015_12_dezembro_rel_executivo_resumo_31122015` | 536 |

### fevereiro_2010_pmcmv — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2010

| período | tabela | n_linhas |
|---|---|---|
| 2010-02-19 | `caixa_001_2010_02___fevereiro_2010_pmcmv_20100219` | 536 |
| 2010-02-26 | `caixa_001_2010_02___fevereiro_2010_pmcmv_20100226` | 536 |

### fevereiro_rel_executivo_resumo — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `536 → 536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-02-15 | `caixa_001_2013_02___fevereiro_rel_executivo_resumo_15022013` | 536 |
| 2013-02-15 | `caixa_001_2013_02___fevereiro_rel_executivo_resumo_15022013_v2` | 536 |
| 2013-02-28 | `caixa_001_2013_02___fevereiro_rel_executivo_resumo_28022013` | 536 |
| 2015-02-28 | `caixa_001_2015_02_fevereiro_rel_executivo_resumo_28022015` | 536 |

### ixa_001_2013_02_fevereiro_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `21596 → 21896`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=21596->21896; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-02-15 | `ixa_001_2013_02___fevereiro_bases_relatório_executivo_15022013` | 21596 |
| 2013-02-28 | `ixa_001_2013_02___fevereiro_bases_relatório_executivo_28022013` | 21896 |

### ixa_001_2013_02_fevereiro_pmcmv_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-02-15 | `ixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_15022013` | 536 |
| 2013-02-28 | `ixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_28022013` | 536 |

### janeiro_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `21308 → 36598`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=21308->36598; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-01-31 | `caixa_001_2013_01___janeiro_bases_relatório_executivo_31012013` | 21308 |
| 2015-01-31 | `caixa_001_2015_01_janeiro_bases_relatório_executivo_31012015` | 36598 |

### janeiro_pmcmv_relatorio_executivo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-01-23 | `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_23012012` | 536 |
| 2012-01-27 | `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_27012012` | 536 |
| 2012-01-31 | `caixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012` | 536 |

### julho_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `16436 → 37978`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=16436->37978; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-07-31 | `caixa_001_2012_07_julho_bases_relatório_executivo_31072012_v2` | 16436 |
| 2015-07-31 | `caixa_001_2015_07_julho_bases_relatório_executivo_31072015` | 37978 |

### julho_pmcmv_automatizado_continua — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-07-04 | `caixa_001_2011_07_julho_pmcmv_automatizado_continua_04072011` | 536 |
| 2011-07-15 | `caixa_001_2011_07_julho_pmcmv_automatizado_continua_15072011` | 536 |
| 2011-07-22 | `caixa_001_2011_07_julho_pmcmv_automatizado_continua_22072011` | 536 |

### julho_pmcmv_automatizado_novo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-07-28 | `caixa_001_2011_07_julho_pmcmv_automatizado_novo_28072011` | 536 |
| 2011-08-01 | `caixa_001_2011_07_julho_pmcmv_automatizado_novo_01082011` | 536 |

### julho_presidencia_automatizado — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `11128 → 11128`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=11128->11128; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-07-28 | `caixa_001_2011_07_julho_presidencia_automatizado_28072011` | 11128 |
| 2011-08-01 | `caixa_001_2011_07_julho_presidencia_automatizado_01082011` | 11128 |

### junho_2010_pmcmv — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2010

| período | tabela | n_linhas |
|---|---|---|
| 2010-06-04 | `caixa_001_2010_06___junho_2010_pmcmv_04062010` | 536 |
| 2010-06-11 | `caixa_001_2010_06___junho_2010_pmcmv_11062010` | 536 |

### maio_2010_pmcmv — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2010

| período | tabela | n_linhas |
|---|---|---|
| 2010-05-07 | `caixa_001_2010_05___maio_2010_pmcmv_20100507` | 536 |
| 2010-05-21 | `caixa_001_2010_05___maio_2010_pmcmv_20100521` | 536 |
| 2010-05-27 | `caixa_001_2010_05___maio_2010_pmcmv_20100527` | 536 |

### maio_bases_relat_rio_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `23314 → 37332`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=23314->37332; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-01-01 | `caixa_001_2013_05___maio_bases_relat_rio_executivo_150513` | 23314 |
| 2015-05-31 | `caixa_001_2015_05_maio_bases_relat_rio_executivo_31052015` | 37332 |

### maio_rel_executivo_resumo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-01-01 | `caixa_001_2013_05___maio_rel_executivo_resumo_150513` | 536 |
| 2015-05-31 | `caixa_001_2015_05_maio_rel_executivo_resumo_31052015` | 536 |

### março_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `22082 → 22602`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=22082->22602; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-03-15 | `caixa_001_2013_03___março_bases_relatório_executivo_15032013` | 22082 |
| 2013-03-28 | `caixa_001_2013_03___março_bases_relatório_executivo_28032013` | 22602 |

### março_pmcmv_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-03-15 | `caixa_001_2013_03___março_pmcmv_relatório_executivo_15032013` | 536 |
| 2013-03-28 | `caixa_001_2013_03___março_pmcmv_relatório_executivo_28032013` | 536 |

### março_rel_executivo_resumo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2013

| período | tabela | n_linhas |
|---|---|---|
| 2013-03-15 | `caixa_001_2013_03___março_rel_executivo_resumo_15032013` | 536 |
| 2013-03-28 | `caixa_001_2013_03___março_rel_executivo_resumo_28032013` | 536 |

### novembro_2010_pmcmv — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2010

| período | tabela | n_linhas |
|---|---|---|
| 2010-11-19 | `caixa_001_2010_11___novembro_2010_pmcmv_19112010` | 536 |
| 2010-11-26 | `caixa_001_2010_11___novembro_2010_pmcmv_26112010` | 536 |

### novembro_bases_relatório_executivo — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `19398 → 35188 → 35532 → 38898`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=19398->35188->35532->38898; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-11-28 | `caixa_001_2012_11_novembro_bases_relatório_executivo_28112012` | 19398 |
| 2014-11-15 | `caixa_001_2014_11_novembro_bases_relatório_executivo_15112014` | 35188 |
| 2014-11-30 | `caixa_001_2014_11_novembro_bases_relatório_executivo_30112014` | 35532 |
| 2015-11-30 | `caixa_001_2015_11_novembro_bases_relatório_executivo_30112015` | 38898 |

### novembro_pmcmv_relatório_executivo — `full_refresh`

- Versões: **3**
- Trajetória de contagens: `536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-11-28 | `caixa_001_2012_11_novembro_pmcmv_relatório_executivo_28112012` | 536 |
| 2014-11-15 | `caixa_001_2014_11_novembro_pmcmv_relatório_executivo_15112014` | 536 |
| 2014-11-30 | `caixa_001_2014_11_novembro_pmcmv_relatório_executivo_30112014` | 536 |

### novembro_rel_executivo_resumo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2014-11-15 | `caixa_001_2014_11_novembro_rel_executivo_resumo_15112014` | 536 |
| 2015-11-30 | `caixa_001_2015_11_novembro_rel_executivo_resumo_30112015` | 536 |

### outubro_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `18738 → 38636`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=18738->38636; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-10-31 | `caixa_001_2012_10_outubro_bases_relatório_executivo_31102012` | 18738 |
| 2015-10-31 | `caixa_001_2015_10_outubro_bases_relatório_executivo_31102015` | 38636 |

### outubro_pmcmv_automatizado_novo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-10-13 | `caixa_001_2011_10_outubro_pmcmv_automatizado_novo_13102011` | 536 |
| 2011-10-20 | `caixa_001_2011_10_outubro_pmcmv_automatizado_novo_20102011` | 536 |

### pmcmv_3_relat_rio_executivo_abr2018 — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2018

| período | tabela | n_linhas |
|---|---|---|
| 2018-01-01 | `caixa_002_2018_pmcmv_3_relat_rio_executivo_abr2018` | 536 |
| 2018-01-01 | `caixa_002_2018_pmcmv_3_relat_rio_executivo_abr2018_v2` | 536 |

### pmcmv_3_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2016

| período | tabela | n_linhas |
|---|---|---|
| 2016-04-30 | `caixa_001_2016_pmcmv_3_relatório_executivo_30042016` | 536 |
| 2016-12-31 | `caixa_001_2016_pmcmv_3_relatório_executivo_31122016` | 536 |

### rel_executivo_resumo — `full_refresh`

- Versões: **6**
- Trajetória de contagens: `536 → 536 → 536 → 536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536->536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2014

| período | tabela | n_linhas |
|---|---|---|
| 2015-01-31 | `caixa_001_2014_rel_executivo_resumo_31012015` | 536 |
| 2015-02-28 | `caixa_001_2014_rel_executivo_resumo_28022015` | 536 |
| 2016-04-30 | `caixa_001_2016_rel_executivo_resumo_30042016` | 536 |
| 2016-08-31 | `caixa_001_2016_rel_executivo_resumo_31082016` | 536 |
| 2016-12-31 | `caixa_001_2016_rel_executivo_resumo_31122016` | 536 |
| 2017-01-31 | `caixa_003_2017_rel_executivo_resumo_31012017` | 536 |

### rel_executivo_resumo_abr2018 — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2018

| período | tabela | n_linhas |
|---|---|---|
| 2018-01-01 | `caixa_002_2018_rel_executivo_resumo_abr2018` | 536 |
| 2018-01-01 | `caixa_002_2018_rel_executivo_resumo_abr2018_v2` | 536 |

### setembro_bases_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `17744 → 18016`
- Evidência: classificacao=crescimento_nao_consistente_com_append; trajetoria=17744->18016; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-09-26 | `caixa_001_2012_09_setembro_bases_relatório_executivo_26092012` | 17744 |
| 2012-09-30 | `caixa_001_2012_09_setembro_bases_relatório_executivo_30092012` | 18016 |

### setembro_pmcmv_ii — `full_refresh`

- Versões: **4**
- Trajetória de contagens: `536 → 536 → 536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536->536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2011

| período | tabela | n_linhas |
|---|---|---|
| 2011-09-09 | `caixa_001_2011_09_setembro_pmcmv_ii_09092011` | 536 |
| 2011-09-16 | `caixa_001_2011_09_setembro_pmcmv_ii_16092011` | 536 |
| 2011-09-23 | `caixa_001_2011_09_setembro_pmcmv_ii_23092011` | 536 |
| 2011-09-29 | `caixa_001_2011_09_setembro_pmcmv_ii_29092011` | 536 |

### setembro_pmcmv_relatório_executivo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-09-26 | `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_26092012` | 536 |
| 2012-09-30 | `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_30092012` | 536 |

### setembro_rel_executivo_resumo — `full_refresh`

- Versões: **2**
- Trajetória de contagens: `536 → 536`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=536->536; fonte_contagem=exato_count_star; nome_periodo=YYYY:2012

| período | tabela | n_linhas |
|---|---|---|
| 2012-09-15 | `caixa_001_2012_09_setembro_rel_executivo_resumo_15092012` | 536 |
| 2012-09-26 | `caixa_001_2012_09_setembro_rel_executivo_resumo_26092012` | 536 |

### snh_pmcmv_dados_prioritarios|af_bb|entrega_unidade — `full_refresh`

- Versões: **21**
- Trajetória de contagens: `11818 → 11654 → 11666 → 11676 → 11702 → 11740 → 11762 → 11790 → 11836 → 11850 → 11856 → 11860 → 11868 → 11870 → 11878 → 11882 → 11884 → 11884 → 11884 → 11898 → 11906`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=11818->11654->11666->11676->11702->11740->11762->11790->11836->11850->11856->11860->11868->11870->11878->11882->11884->11884->11884->11898->11906; fonte_contagem=exato_count_star; nome_periodo=YYYYMM:202402

| período | tabela | n_linhas |
|---|---|---|
| 2024-02-01 | `202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11818 |
| 2024-06-01 | `202406_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11654 |
| 2024-07-01 | `202407_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11666 |
| 2024-08-01 | `202408_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11676 |
| 2024-09-01 | `202409_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11702 |
| 2024-11-01 | `202411_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11740 |
| 2024-12-01 | `202412_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11762 |
| 2025-01-01 | `202501_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11790 |
| 2025-03-01 | `202503_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11836 |
| 2025-04-01 | `202504_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11850 |
| 2025-05-01 | `202505_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11856 |
| 2025-06-01 | `202506_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11860 |
| 2025-07-01 | `202507_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11868 |
| 2025-08-01 | `202508_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11870 |
| 2025-09-01 | `202509_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11878 |
| 2025-10-01 | `202510_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11882 |
| 2025-11-01 | `202511_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11884 |
| 2025-12-01 | `202512_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11884 |
| 2026-01-01 | `202601_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11884 |
| 2026-02-01 | `202602_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11898 |
| 2026-03-01 | `202603_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11906 |

### snh_pmcmv_dados_prioritarios|af_bb|historico_recente — `full_refresh`

- Versões: **21**
- Trajetória de contagens: `2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576 → 2576`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576->2576; fonte_contagem=exato_count_star; nome_periodo=YYYYMM:202406

| período | tabela | n_linhas |
|---|---|---|
| 2024-01-01 | `historico_recente_2024_08_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2024-01-01 | `historico_recente_2024_09_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2024-01-01 | `historico_recente_2024_10_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2024-06-01 | `historico_recente_202406_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2024-11-01 | `historico_recente_202411_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2024-12-01 | `historico_recente_202412_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-01-01 | `historico_recente_2025_01_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-01-01 | `historico_recente_2025_02_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-03-01 | `historico_recente_202503_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-04-01 | `historico_recente_202504_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-05-01 | `historico_recente_202505_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-06-01 | `historico_recente_202506_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-07-01 | `historico_recente_202507_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-08-01 | `historico_recente_202508_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-09-01 | `historico_recente_202509_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-10-01 | `historico_recente_202510_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-11-01 | `historico_recente_202511_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2025-12-01 | `historico_recente_202512_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2026-01-01 | `historico_recente_202601_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2026-02-01 | `historico_recente_202602_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |
| 2026-03-01 | `historico_recente_202603_snh_pmcmv_dados_prioritarios_af_bb` | 2576 |

### snh_pmcmv_dados_prioritarios|af_caixa|historico_recente — `full_refresh`

- Versões: **21**
- Trajetória de contagens: `24392 → 24452 → 24922 → 25710 → 25328 → 25668 → 25888 → 26072 → 26288 → 26704 → 27308 → 27718 → 28038 → 28532 → 29042 → 29624 → 29686 → 29826 → 29846 → 29866 → 29906`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=24392->24452->24922->25710->25328->25668->25888->26072->26288->26704->27308->27718->28038->28532->29042->29624->29686->29826->29846->29866->29906; fonte_contagem=exato_count_star; nome_periodo=YYYYMM:202406

| período | tabela | n_linhas |
|---|---|---|
| 2024-06-01 | `historico_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa` | 24392 |
| 2024-07-01 | `historico_recente_202407_snh_pmcmv_dados_prioritarios_af_caixa` | 24452 |
| 2024-09-01 | `historico_recente_202409_snh_pmcmv_dados_prioritarios_af_caixa` | 24922 |
| 2024-10-01 | `historico_recente_202410_snh_pmcmv_dados_prioritarios_af_caixa` | 25710 |
| 2024-11-01 | `historico_recente_202411_snh_pmcmv_dados_prioritarios_af_caixa` | 25328 |
| 2024-12-01 | `historico_recente_202412_snh_pmcmv_dados_prioritarios_af_caixa` | 25668 |
| 2025-01-01 | `historico_recente_202501_snh_pmcmv_dados_prioritarios_af_caixa` | 25888 |
| 2025-02-01 | `historico_recente_202502_snh_pmcmv_dados_prioritarios_af_caixa` | 26072 |
| 2025-03-01 | `historico_recente_202503_snh_pmcmv_dados_prioritarios_af_caixa` | 26288 |
| 2025-04-01 | `historico_recente_202504_snh_pmcmv_dados_prioritarios_af_caixa` | 26704 |
| 2025-05-01 | `historico_recente_202505_snh_pmcmv_dados_prioritarios_af_caixa` | 27308 |
| 2025-06-01 | `historico_recente_202506_snh_pmcmv_dados_prioritarios_af_caixa` | 27718 |
| 2025-07-01 | `historico_recente_202507_snh_pmcmv_dados_prioritarios_af_caixa` | 28038 |
| 2025-08-01 | `historico_recente_202508_snh_pmcmv_dados_prioritarios_af_caixa` | 28532 |
| 2025-09-01 | `historico_recente_202509_snh_pmcmv_dados_prioritarios_af_caixa` | 29042 |
| 2025-10-01 | `historico_recente_202510_snh_pmcmv_dados_prioritarios_af_caixa` | 29624 |
| 2025-11-01 | `historico_recente_202511_snh_pmcmv_dados_prioritarios_af_caixa` | 29686 |
| 2025-12-01 | `historico_recente_202512_snh_pmcmv_dados_prioritarios_af_caixa` | 29826 |
| 2026-01-01 | `historico_recente_202601_snh_pmcmv_dados_prioritarios_af_caixa` | 29846 |
| 2026-02-01 | `historico_recente_202602_snh_pmcmv_dados_prioritarios_af_caixa` | 29866 |
| 2026-03-01 | `historico_recente_202603_snh_pmcmv_dados_prioritarios_af_caixa` | 29906 |

### snh_pmcmv_dados_prioritarios|af_caixa|o_recente|entregas — `full_refresh`

- Versões: **21**
- Trajetória de contagens: `22566 → 22584 → 22604 → 22764 → 22778 → 22634 → 22670 → 22692 → 22706 → 22730 → 22744 → 22762 → 22766 → 22794 → 22818 → 22844 → 22920 → 22982 → 23080 → 23094 → 23150`
- Evidência: classificacao=nao_monotona(flutua_ou_constante); trajetoria=22566->22584->22604->22764->22778->22634->22670->22692->22706->22730->22744->22762->22766->22794->22818->22844->22920->22982->23080->23094->23150; fonte_contagem=exato_count_star; nome_periodo=YYYYMM:202406

| período | tabela | n_linhas |
|---|---|---|
| 2024-06-01 | `o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22566 |
| 2024-07-01 | `o_recente_202407_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22584 |
| 2024-08-01 | `o_recente_202408_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22604 |
| 2024-09-01 | `o_recente_202409_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22764 |
| 2024-10-01 | `o_recente_202410_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22778 |
| 2024-11-01 | `o_recente_202411_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22634 |
| 2024-12-01 | `o_recente_202412_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22670 |
| 2025-01-01 | `o_recente_202501_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22692 |
| 2025-02-01 | `o_recente_202502_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22706 |
| 2025-04-01 | `o_recente_202504_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22730 |
| 2025-05-01 | `o_recente_202505_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22744 |
| 2025-06-01 | `o_recente_202506_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22762 |
| 2025-07-01 | `o_recente_202507_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22766 |
| 2025-08-01 | `o_recente_202508_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22794 |
| 2025-09-01 | `o_recente_202509_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22818 |
| 2025-10-01 | `o_recente_202510_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22844 |
| 2025-11-01 | `o_recente_202511_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22920 |
| 2025-12-01 | `o_recente_202512_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 22982 |
| 2026-01-01 | `o_recente_202601_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 23080 |
| 2026-02-01 | `o_recente_202602_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 23094 |
| 2026-03-01 | `o_recente_202603_snh_pmcmv_dados_prioritarios_af_caixa_entregas` | 23150 |

## Famílias com versão única

| família | tabela | n_linhas | modelo_carga |
|---|---|---|---|
| `001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base_bd` | `_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base_bd` | 8208 | full_refresh |
| `001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012_cópi` | `_001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópi` | 536 | full_refresh |
| `001_2012_04_abril_2012_04_18_5c_base_contratação_pf` | `_001_2012_04_abril_2012_04_18_5c_base_contratação_pf_18042012` | 33914 | full_refresh |
| `001_2012_11_novembro_sintese_20121128_evento_1_milhao|entregas` | `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas` | 84 | full_refresh |
| `001_2013_01_janeiro_pmcmv_relatório_executivo_31_01` | `_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__2013` | 536 | full_refresh |
| `001_2015_01_janeiro_rel_executivo_resumo_31012015_reprocessado` | `_001_2015_01_janeiro_rel_executivo_resumo_31012015_reprocessado` | 536 | full_refresh |
| `10_snh_pmcmv_dados_prioritarios|af_bb|entrega_unidade` | `024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` | 11724 | full_refresh |
| `12_dezembro_pmcmv_relatorio_executivo_31122011_base_cópi` | `011_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópi` | 7032 | full_refresh |
| `12_dezembro_pmcmv_relatorio_executivo_31122011_base_cópia` | `11_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópia` | 7032 | full_refresh |
| `2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo` | `001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo` | 76 | full_refresh |
| `2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02` | `001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012` | 536 | full_refresh |
| `2012_03_marco_pmcmv_relatorio_executivo_27_03_2012_cópia` | `001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópia` | 536 | full_refresh |
| `2012_04_abril_2012_04_18_pmcmv_relatorio_executivo_18_04` | `1_2012_04_abril_2012_04_18_pmcmv_relatorio_executivo_18_04_2012` | 536 | full_refresh |
| `2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04` | `01_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_2012` | 536 | full_refresh |
| `2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_201` | `001_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_201` | 536 | full_refresh |
| `2012_05_maio_pmcmv_relatorio_executivo_31_05_2012_version_1` | `01_2012_05_maio_pmcmv_relatorio_executivo_31_05_2012__version_1` | 536 | full_refresh |
| `2012_06_junho_pmcmv_relatorio_executivo_15_06_2012_corrigido` | `01_2012_06_junho_pmcmv_relatorio_executivo_15_06_2012_corrigido` | 536 | full_refresh |
| `2012_10_outubro_20121009_bases_relatório_executivo_0910201` | `001_2012_10_outubro_20121009_bases_relatório_executivo_0910201` | 18244 | full_refresh |
| `2012_10_outubro_20121009_bases_relatório_executivo_1610201` | `001_2012_10_outubro_20121009_bases_relatório_executivo_1610201` | 18352 | full_refresh |
| `2012_10_outubro_20121009_pmcmv_relatório_executivo` | `01_2012_10_outubro_20121009_pmcmv_relatório_executivo_09102012` | 536 | full_refresh |
| `2012_10_outubro_20121009_pmcmv_relatório_executivo_0910201` | `001_2012_10_outubro_20121009_pmcmv_relatório_executivo_0910201` | 536 | full_refresh |
| `2014_12_dezembro_rel_executivo_resumo_31122014_reprocessado` | `001_2014_12_dezembro_rel_executivo_resumo_31122014_reprocessado` | 536 | full_refresh |
| `2018_int054_ministeriocidades_far_bb_empreendimentos` | `1_2018_int054_ministeriocidades_far_bb_empreendimentos_20180831` | 404 | full_refresh |
| `a_001_2010_09_setembro_2010_contratação_pf_total` | `a_001_2010_09___setembro_2010_contratação_pf_total___06092010` | 19928 | full_refresh |
| `a_001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_mode` | `a_001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_mode` | 76 | full_refresh |
| `a_001_2013_01_janeiro_pmcmv_relatório_executivo_31_01_201` | `a_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__201` | 536 | full_refresh |
| `abril_2010_pmcmv` | `caixa_001_2010_04___abril_2010_pmcmv_20100430` | 536 | full_refresh |
| `abril_2010_pmcmv_15_04` | `caixa_001_2010_04___abril_2010_pmcmv_15_04_2010` | 536 | full_refresh |
| `abril_2010_pmcmv_23_04` | `caixa_001_2010_04___abril_2010_pmcmv_23_04_2010` | 536 | full_refresh |
| `abril_2012_04_18_5b1_base_empreend_contratado` | `caixa_001_2012_04_abril_2012_04_18_5b1_base_empreend_contratado` | 7954 | full_refresh |
| `abril_2012_04_18_5b_empreend_contratado` | `caixa_001_2012_04_abril_2012_04_18_5b_empreend_contratado` | 6272 | full_refresh |
| `abril_2012_04_18_5c1_base_contrata_o_pf` | `caixa_001_2012_04_abril_2012_04_18_5c1__base_contrata__o_pf` | 30184 | full_refresh |
| `abril_2012_04_18_5c_base_contrata_o_pf` | `caixa_001_2012_04_abril_2012_04_18_5c_base_contrata__o_pf` | 33914 | full_refresh |
| `abril_2012_04_18_rural` | `caixa_001_2012_04_abril_2012_04_18_rural_18042012` | 2884 | full_refresh |
| `abril_2012_04_30_ajustada_caixa_5d_base_bext` | `caixa_001_2012_04_abril_2012_04_30_ajustada_caixa_5d_base_bext` | 6942 | full_refresh |
| `abril_5b1_base_empreend_contratado` | `caixa_001_2012_04_abril_5b1_base_empreend_contratado` | 7954 | full_refresh |
| `abril_pmcmv` | `caixa_001_2011_04_abril_pmcmv_14042011` | 536 | full_refresh |
| `abril_pmcmv_3_relatório_executivo` | `caixa_001_2015_04_abril_pmcmv_3_relatório_executivo_30042015` | 536 | full_refresh |
| `abril_pmcmv_relat_rio_executivo` | `caixa_001_2013_04___abril_pmcmv_relat_rio_executivo_30042013` | 536 | full_refresh |
| `abril_pmcmv_relatorio_executivo_18_04` | `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_18_04_2012` | 536 | full_refresh |
| `abril_pmcmv_relatorio_executivo_24_04` | `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_24_04_2012` | 536 | full_refresh |
| `abril_pmcmv_relatorio_executivo_30_04` | `caixa_001_2012_04_abril_pmcmv_relatorio_executivo_30_04_2012` | 536 | full_refresh |
| `abril_pmcmv_relatório_executivo` | `caixa_001_2013_04___abril_pmcmv_relatório_executivo_30042013` | 536 | full_refresh |
| `abril_validacao` | `caixa_001_2012_04_abril_validacao_1604_1804` | 6210 | full_refresh |
| `agosto_2009_pmcmv_07_08_2009_caixa` | `caixa_001_2009_08_agosto_2009_pmcmv_07_08_2009___caixa` | 536 | full_refresh |
| `agosto_2009_pmcmv_14_08_2009_caixa` | `caixa_001_2009_08_agosto_2009_pmcmv_14_08_2009___caixa` | 536 | full_refresh |
| `agosto_2009_pmcmv_20_08_2009_caixa` | `caixa_001_2009_08_agosto_2009_pmcmv_20_08_2009___caixa` | 536 | full_refresh |
| `agosto_2009_pmcmv_28_08_2009_caixa` | `caixa_001_2009_08_agosto_2009_pmcmv_28_08_2009___caixa` | 536 | full_refresh |
| `agosto_2010_municípios_contratados` | `caixa_001_2010_08___agosto_2010_municípios_contratados_1308` | 74 | full_refresh |
| `agosto_2010_pmcmv06082010` | `caixa_001_2010_08___agosto_2010_pmcmv06082010` | 536 | full_refresh |
| `agosto_2010_pmcmv13082010` | `caixa_001_2010_08___agosto_2010_pmcmv13082010` | 536 | full_refresh |
| `agosto_2010_pmcmv20082010` | `caixa_001_2010_08___agosto_2010_pmcmv20082010` | 536 | full_refresh |
| `agosto_2010_pmcmv30082010` | `caixa_001_2010_08___agosto_2010_pmcmv30082010` | 536 | full_refresh |
| `agosto_2010_pmcmv_06_08` | `caixa_001_2010_08___agosto_2010_pmcmv_06_08_2010` | 536 | full_refresh |
| `agosto_2010_pmcmv_13_08` | `caixa_001_2010_08___agosto_2010_pmcmv_13_08_2010` | 536 | full_refresh |
| `agosto_base_pf_e_pj_06_08_12` | `caixa_001_2012_08_agosto_base_pf_e_pj_06_08_12` | 16524 | full_refresh |
| `agosto_base_pf_e_pj_14_08_12` | `caixa_001_2012_08_agosto_base_pf_e_pj_14_08_12` | 16800 | full_refresh |
| `agosto_base_pf_e_pj_20_08_12` | `caixa_001_2012_08_agosto_base_pf_e_pj_20_08_12` | 16984 | full_refresh |
| `agosto_base_pf_e_pj_24_08_12` | `caixa_001_2012_08_agosto_base_pf_e_pj_24_08_12` | 17038 | full_refresh |
| `agosto_bases_relatório_executivo_31_08_12` | `caixa_001_2012_08_agosto_bases_relatório_executivo_31_08_12` | 17220 | full_refresh |
| `agosto_demanda_mc_far_pmcmv` | `caixa_001_2011_08_agosto_demanda_mc_far_pmcmv` | 2610 | full_refresh |
| `agosto_pmcmv` | `caixa_001_2011_08_agosto_pmcmv_20110826` | 536 | full_refresh |
| `agosto_pmcmv_3_relatório_executivo` | `caixa_002_2015_08_agosto_pmcmv_3_relatório_executivo_12082015` | 536 | full_refresh |
| `agosto_pmcmv_3_relatório_executivo31082015` | `caixa_002_2015_08_agosto_pmcmv_3_relatório_executivo31082015` | 536 | full_refresh |
| `agosto_pmcmv_acumulado_09_08` | `caixa_001_2011_08_agosto_pmcmv_acumulado_09_08_2011` | 536 | full_refresh |
| `agosto_pmcmv_automatizado_continua` | `caixa_001_2011_08_agosto_pmcmv_automatizado_continua_09082011` | 536 | full_refresh |
| `agosto_pmcmv_ii_09_08` | `caixa_001_2011_08_agosto_pmcmv_ii_09_08_2011` | 536 | full_refresh |
| `agosto_pmcmv_ii_15_08` | `caixa_001_2011_08_agosto_pmcmv_ii_15_08_2011` | 536 | full_refresh |
| `agosto_pmcmv_ii_19_08` | `caixa_001_2011_08_agosto_pmcmv_ii_19_08_2011` | 536 | full_refresh |
| `agosto_pmcmv_ii_26_08` | `caixa_001_2011_08_agosto_pmcmv_ii_26_08_2011` | 536 | full_refresh |
| `agosto_pmcmv_relatório_executivo_06_08_12` | `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_06_08_12` | 536 | full_refresh |
| `agosto_pmcmv_relatório_executivo_14_08_12` | `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_14_08_12` | 536 | full_refresh |
| `agosto_pmcmv_relatório_executivo_20_08_12` | `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_20_08_12` | 536 | full_refresh |
| `agosto_pmcmv_relatório_executivo_24_08_12` | `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_24_08_12` | 536 | full_refresh |
| `agosto_pmcmv_relatório_executivo_31_08_12` | `caixa_001_2012_08_agosto_pmcmv_relatório_executivo_31_08_12` | 536 | full_refresh |
| `agosto_presidencia_automatizado` | `caixa_001_2011_08_agosto_presidencia_automatizado_01082011` | 11128 | full_refresh |
| `agosto_presidencia_automatizado_mcmvii` | `caixa_001_2011_08_agosto_presidencia_automatizado_mcmvii_082011` | 11128 | full_refresh |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_17_02` | `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_17_02_2012` | 536 | full_refresh |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_27_02` | `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_27_02_2012` | 536 | full_refresh |
| `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_29_02` | `aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_29_02_2012` | 536 | full_refresh |
| `aixa_001_2013_02_fevereiro_bases_relatório_executivo_1502201` | `aixa_001_2013_02___fevereiro_bases_relatório_executivo_1502201` | 21596 | full_refresh |
| `aixa_001_2013_02_fevereiro_bases_relatório_executivo_2802201` | `aixa_001_2013_02___fevereiro_bases_relatório_executivo_2802201` | 21896 | full_refresh |
| `aixa_001_2013_02_fevereiro_pmcmv_relatório_executivo_1502201` | `aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_1502201` | 536 | full_refresh |
| `aixa_001_2013_02_fevereiro_pmcmv_relatório_executivo_2802201` | `aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_2802201` | 536 | full_refresh |
| `aixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v` | `aixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v` | 36202 | full_refresh |
| `aixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_2802201` | `aixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_2802201` | 536 | full_refresh |
| `aixa_001_2015_11_novembro_pmcmv_3_relatório_executivo` | `aixa_001_2015_11_novembro_pmcmv_3_relatório_executivo_30112015` | 536 | full_refresh |
| `aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2` | `aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2` | 39200 | full_refresh |
| `aixa_001_2016_02_fevereiro_relatorio_cidades` | `aixa_001_2016_02_fevereiro_relatorio_cidades__entregas_20160229` | 289584 | full_refresh |
| `aixa_002_2015_12_dezembro_pmcmv_3_relatório_executivo` | `aixa_002_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015` | 536 | full_refresh |
| `atorio_executivo_30092012_v3_bases_relatório_executivo_3009201` | `atorio_executivo_30092012_v3_bases_relatório_executivo_3009201` | 18124 | full_refresh |
| `atorio_executivo_30092012_v3_pmcmv_relatório_executivo_3009201` | `atorio_executivo_30092012_v3_pmcmv_relatório_executivo_3009201` | 536 | full_refresh |
| `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd` | `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd` | 178 | full_refresh |
| `bases_relat_rio_executivo` | `caixa_003_2017_bases_relat_rio_executivo_31012017` | 42820 | full_refresh |
| `bases_relat_rio_executivo_abr2017` | `caixa_002_2017_bases_relat_rio_executivo_abr2017` | 43344 | full_refresh |
| `bases_relat_rio_executivo_ago2017` | `caixa_002_2017_bases_relat_rio_executivo_ago2017` | 44756 | full_refresh |
| `bases_relat_rio_executivo_fev2017` | `caixa_002_2017_bases_relat_rio_executivo_fev2017` | 42862 | full_refresh |
| `bases_relat_rio_executivo_jul2017` | `caixa_003_2017_bases_relat_rio_executivo_jul2017` | 44358 | full_refresh |
| `bases_relat_rio_executivo_jun2017` | `caixa_003_2017_bases_relat_rio_executivo_jun2017` | 43742 | full_refresh |
| `bases_relat_rio_executivo_jun2018` | `caixa_002_2018_bases_relat_rio_executivo_jun2018` | 51958 | full_refresh |
| `bases_relat_rio_executivo_mai2017` | `caixa_003_2017_bases_relat_rio_executivo_mai2017` | 43690 | full_refresh |
| `bases_relat_rio_executivo_mai2018` | `caixa_002_2018_bases_relat_rio_executivo_mai2018` | 51546 | full_refresh |
| `bases_relat_rio_executivo_mar18` | `caixa_002_2018_bases_relat_rio_executivo_mar18` | 49154 | full_refresh |
| `bases_relat_rio_executivo_mar2017` | `caixa_003_2017_bases_relat_rio_executivo_mar2017` | 42990 | full_refresh |
| `bases_relat_rio_executivo_nov17` | `caixa_003_2017_bases_relat_rio_executivo_nov17` | 47524 | full_refresh |
| `bases_relatório_executivo_ago2018` | `caixa_002_2018_bases_relatório_executivo_ago2018` | 52840 | full_refresh |
| `bases_relatório_executivo_dez17` | `caixa_002_2017_bases_relatório_executivo_dez17` | 47884 | full_refresh |
| `bb_2011_01_janeiro_rel_11jan2011` | `bb_2011_01_janeiro_rel_11jan2011` | 322 | full_refresh |
| `bb_2011_01_janeiro_rel_25jan2011` | `bb_2011_01_janeiro_rel_25jan2011` | 316 | full_refresh |
| `bb_2011_02_fevereiro_dados` | `bb_2011_02_fevereiro_dados_22022011` | 32 | full_refresh |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_01_02` | `bb_2011_02_fevereiro_relatório_mcmv_bb_01_02_2011` | 316 | full_refresh |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_08_02` | `bb_2011_02_fevereiro_relatório_mcmv_bb_08_02_2011` | 316 | full_refresh |
| `bb_2011_02_fevereiro_relatório_mcmv_bb_16_02` | `bb_2011_02_fevereiro_relatório_mcmv_bb_16_02_2011` | 316 | full_refresh |
| `bb_2011_03_março_relatorio_min_cidades_22mar11` | `bb_2011_03_março_relatorio_min_cidades___22mar11` | 316 | full_refresh |
| `bb_2011_04_abril_relatorio_min_cidades_18abr11` | `bb_2011_04_abril_relatorio_min_cidades___18abr11` | 316 | full_refresh |
| `bb_2011_04_abril_relatorio_min_cidades_26abr11` | `bb_2011_04_abril_relatorio_min_cidades___26abr11` | 316 | full_refresh |
| `bb_2011_05_maio_relatorio_min_cidades_03mai11` | `bb_2011_05_maio_relatorio_min_cidades___03mai11` | 316 | full_refresh |
| `bb_2011_05_maio_relatorio_min_cidades_10mai11` | `bb_2011_05_maio_relatorio_min_cidades___10mai11` | 316 | full_refresh |
| `bb_2011_05_maio_relatorio_min_cidades_17mai11` | `bb_2011_05_maio_relatorio_min_cidades___17mai11` | 316 | full_refresh |
| `bb_2011_05_maio_relatorio_min_cidades_24mai11` | `bb_2011_05_maio_relatorio_min_cidades___24mai11` | 316 | full_refresh |
| `bb_2011_06_junho_relatorio_min_cidades_07jun11` | `bb_2011_06_junho_relatorio_min_cidades___07jun11` | 316 | full_refresh |
| `bb_2011_07_julho_relatorio_min_cidades_12jul11` | `bb_2011_07_julho_relatorio_min_cidades___12jul11` | 316 | full_refresh |
| `bb_2011_07_julho_relatorio_min_cidades_19jul11` | `bb_2011_07_julho_relatorio_min__cidades_19jul11` | 316 | full_refresh |
| `bb_2011_08_agosto_balanço_23_08_2011_min_planejamento` | `bb_2011_08_agosto_balanço_23_08_2011_min__planejamento` | 12 | full_refresh |
| `bb_2011_08_agosto_relatorio_min_cidades_16ago11` | `bb_2011_08_agosto_relatorio_min__cidades_16ago11` | 106 | full_refresh |
| `bb_2011_08_agosto_relatorio_min_cidades_30ago11` | `bb_2011_08_agosto_relatorio_min__cidades_30ago11` | 106 | full_refresh |
| `bb_2011_09_setembro_relatorio_min_cidades_13set11_2` | `bb_2011_09_setembro_relatorio_min__cidades_13set11_2` | 106 | full_refresh |
| `bb_2011_10_outubro_relatorio_min_cidades_04out11` | `bb_2011_10_outubro_relatorio_min__cidades_04out11` | 106 | full_refresh |
| `bb_2011_10_outubro_relatorio_min_cidades_11out11` | `bb_2011_10_outubro_relatorio_min__cidades_11out11` | 106 | full_refresh |
| `bb_2011_10_outubro_relatorio_min_cidades_18out11` | `bb_2011_10_outubro_relatorio_min__cidades_18out11` | 106 | full_refresh |
| `bb_2011_10_outubro_relatorio_min_cidades_25out11` | `bb_2011_10_outubro_relatorio_min__cidades_25out11` | 106 | full_refresh |
| `bb_2012_01_janeiro_relatorio_min_cidades_03_jan_12` | `bb_2012_01_janeiro_relatorio_min_cidades_03_jan_12` | 110 | full_refresh |
| `bb_2012_01_janeiro_relatorio_min_cidades_17jan12` | `bb_2012_01_janeiro_relatorio_min__cidades_17jan12` | 110 | full_refresh |
| `bb_2012_01_janeiro_relatorio_min_cidades_24jan12` | `bb_2012_01_janeiro_relatorio_min__cidades_24jan12` | 110 | full_refresh |
| `bb_2012_01_janeiro_relatorio_min_cidades_31_01` | `bb_2012_01_janeiro_relatorio_min__cidades_31_01_2012` | 110 | full_refresh |
| `bb_2012_02_fevereiro_relatorio_min_cidades_06_02` | `bb_2012_02_fevereiro_relatorio_min__cidades_06_02_2012` | 110 | full_refresh |
| `bb_2012_02_fevereiro_relatorio_min_cidades_28_02` | `bb_2012_02_fevereiro_relatorio_min__cidades_28_02_2012` | 110 | full_refresh |
| `bb_2012_03_março_relatorio_min_cidades_06_03` | `bb_2012_03_março_relatorio_min__cidades_06_03_2012` | 110 | full_refresh |
| `bb_2012_03_março_relatorio_min_cidades_13_03` | `bb_2012_03_março_relatorio_min__cidades_13_03_2012` | 110 | full_refresh |
| `bb_2012_03_março_relatorio_min_cidades_20_03` | `bb_2012_03_março_relatorio_min__cidades_20_03_2012` | 110 | full_refresh |
| `bb_2012_03_março_relatorio_min_cidades_27_03` | `bb_2012_03_março_relatorio_min__cidades_27_03_2012` | 110 | full_refresh |
| `bb_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_b` | `bb_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_b` | 178 | full_refresh |
| `bb_2012_04_abril_rel_min_cidades_valores_25_04` | `bb_2012_04_abril_rel_min_cidades_valores_25_04_2012` | 178 | full_refresh |
| `bb_2012_04_abril_rel_min_cidades_valores_25_04_2012_bd` | `bb_2012_04_abril_rel_min_cidades_valores_25_04_2012_bd` | 178 | full_refresh |
| `bb_2012_04_abril_relatorio_min_cidades_03_04` | `bb_2012_04_abril_relatorio_min__cidades_03_04_2012` | 110 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_08_05` | `bb_2012_05_maio_relatorio_min__cidades_08_05_2012` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_08_05_2012_bd` | `bb_2012_05_maio_relatorio_min__cidades_08_05_2012_bd` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_15_05` | `bb_2012_05_maio_relatorio_min__cidades_15_05_2012` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_15_05_2012_bd` | `bb_2012_05_maio_relatorio_min__cidades_15_05_2012_bd` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_21_05` | `bb_2012_05_maio_relatorio_min__cidades_21_05_2012` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_21_05_2012_bd` | `bb_2012_05_maio_relatorio_min__cidades_21_05_2012___bd` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_28_05` | `bb_2012_05_maio_relatorio_min__cidades_28_05_2012` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_28_05_2012_ctr_caixa_bb` | `bb_2012_05_maio_relatorio_min__cidades_28_05_2012_ctr_caixa_bb` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_31_05` | `bb_2012_05_maio_relatorio_min__cidades_31_05_2012` | 178 | full_refresh |
| `bb_2012_05_maio_relatorio_min_cidades_31_05_2012_bd` | `bb_2012_05_maio_relatorio_min__cidades_31_05_2012_bd` | 178 | full_refresh |
| `bb_2012_05_maio_z_relatorio_caixa_3105a` | `bb_2012_05_maio_z_relatorio_caixa_3105a` | 436 | full_refresh |
| `bb_2012_06_junho_relatorio_min_cidades_04_06` | `bb_2012_06_junho_relatorio_min__cidades_04_06_2012` | 178 | full_refresh |
| `bb_2012_06_junho_relatorio_min_cidades_18_06` | `bb_2012_06_junho_relatorio_min__cidades_18_06_2012` | 178 | full_refresh |
| `bb_2012_06_junho_relatorio_min_cidades_18_06_2012_ret` | `bb_2012_06_junho_relatorio_min__cidades_18_06_2012_ret` | 178 | full_refresh |
| `bb_2012_06_junho_relatorio_min_cidades_26_06` | `bb_2012_06_junho_relatorio_min__cidades_26_06_2012` | 178 | full_refresh |
| `bb_2012_07_julho_relatorio_min_cidades_03_07` | `bb_2012_07_julho_relatorio_min__cidades_03_07_2012` | 178 | full_refresh |
| `bb_2012_07_julho_relatorio_min_cidades_09_07` | `bb_2012_07_julho_relatorio_min__cidades_09_07_2012` | 178 | full_refresh |
| `bb_2012_07_julho_relatorio_min_cidades_17_07` | `bb_2012_07_julho_relatorio_min__cidades_17_07_2012` | 178 | full_refresh |
| `bb_2012_07_julho_relatorio_min_cidades_24_07` | `bb_2012_07_julho_relatorio_min__cidades_24_07_2012` | 178 | full_refresh |
| `bb_2012_07_julho_relatorio_min_cidades_30_07` | `bb_2012_07_julho_relatorio_min__cidades_30_07_2012` | 178 | full_refresh |
| `bb_2012_08_agosto_relatorio_min_cidades_07_08_2012_1` | `bb_2012_08_agosto_relatorio_min__cidades_07_08_2012__1` | 178 | full_refresh |
| `bb_2012_08_agosto_relatorio_min_cidades_14_08` | `bb_2012_08_agosto_relatorio_min__cidades_14_08_2012` | 178 | full_refresh |
| `bb_2012_08_agosto_relatorio_min_cidades_21_08` | `bb_2012_08_agosto_relatorio_min__cidades_21_08_2012` | 178 | full_refresh |
| `bb_2012_08_agosto_relatorio_min_cidades_27_08_2012_1` | `bb_2012_08_agosto_relatorio_min__cidades_27_08_2012__1` | 178 | full_refresh |
| `bb_2012_08_agosto_relatorio_min_cidades_31_08` | `bb_2012_08_agosto_relatorio_min__cidades_31_08_2012` | 178 | full_refresh |
| `bb_2012_09_setembro_relatorio_min_cidades_01_10` | `bb_2012_09_setembro_relatorio_min__cidades_01_10_2012` | 178 | full_refresh |
| `bb_2012_09_setembro_relatorio_min_cidades_11_09` | `bb_2012_09_setembro_relatorio_min__cidades_11_09_2012` | 178 | full_refresh |
| `bb_2012_10_outubro_entrada_bb_20121031_ajustada` | `bb_2012_10_outubro_entrada_bb_20121031_ajustada` | 176 | full_refresh |
| `bb_2012_10_outubro_relatorio_min_cidades_09_10` | `bb_2012_10_outubro_relatorio_min__cidades_09_10_2012` | 178 | full_refresh |
| `bb_2012_10_outubro_relatorio_min_cidades_23_10` | `bb_2012_10_outubro_relatorio_min__cidades_23_10_2012` | 178 | full_refresh |
| `bb_2012_11_novembro_entrada_bb_20121127v2` | `bb_2012_11_novembro_entrada_bb_20121127v2` | 604 | full_refresh |
| `bb_2013_02_fevereiro_propostas_bb` | `bb_2013_02_fevereiro_propostas_bb` | 178 | full_refresh |
| `bb_2013_04_abril_entrada_bb` | `bb_2013_04_abril_entrada_bb_20130429` | 604 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras` | `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras` | 1022 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados` | `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados` | 2 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_beneficiarios_fgts` | `bb_2013_06_junho_pmcmv_18062013_tab_beneficiarios_fgts` | 30552 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos` | `bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos` | 292 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pf_fgts` | `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pf_fgts` | 38232 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj` | `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj` | 596 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_empreendimentos` | `bb_2013_06_junho_pmcmv_18062013_tab_empreendimentos` | 1374 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_operacoes` | `bb_2013_06_junho_pmcmv_18062013_tab_operacoes` | 1338 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_proponentes` | `bb_2013_06_junho_pmcmv_18062013_tab_proponentes` | 526 | full_refresh |
| `bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas` | `bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas` | 1374 | full_refresh |
| `bb_2013_07_julho_entrada_bb` | `bb_2013_07_julho_entrada_bb_20130716` | 1232 | full_refresh |
| `bb_2013_10_outubro_entrada_bb` | `bb_2013_10_outubro_entrada_bb_20131105` | 1198 | full_refresh |
| `bb_2013_12_dezembro_entrada_bb_20131223_aux_bb_far` | `bb_2013_12_dezembro_entrada_bb_20131223__aux_bb_far` | 694 | full_refresh |
| `bb_2014_01_janeiro_entrada_bb` | `bb_2014_01_janeiro_entrada_bb_20140116` | 1154 | full_refresh |
| `bb_2014_03_marco_entrada_bb` | `bb_2014_03_marco_entrada_bb_20140401` | 1198 | full_refresh |
| `bb_2014_07_julho_entrada_bb` | `bb_2014_07_julho_entrada_bb_20140731` | 1278 | full_refresh |
| `bb_2014_08_agosto_entrada_bb_20140818_antigo` | `bb_2014_08_agosto_entrada_bb_20140818_antigo` | 1468 | full_refresh |
| `bb_2014_08_agosto_entrada_bb_20140829_antigo` | `bb_2014_08_agosto_entrada_bb_20140829_antigo` | 1468 | full_refresh |
| `bb_2014_09_setembro_entrada_bb` | `bb_2014_09_setembro_entrada_bb_20140930` | 1468 | full_refresh |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pf_pf` | `bb_2014_10_outubro_2014_10_30_min_cidades_pf_pf` | 98270 | full_refresh |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pj` | `bb_2014_10_outubro_2014_10_30_min_cidades_pj` | 1706 | full_refresh |
| `bb_2014_10_outubro_2014_10_30_min_cidades_pj_pf` | `bb_2014_10_outubro_2014_10_30_min_cidades_pj_pf` | 148948 | full_refresh |
| `bb_2014_10_outubro_2014_10_30_pnhr` | `bb_2014_10_outubro_2014_10_30_pnhr_30102014` | 1678 | full_refresh |
| `bb_2014_11_novembro_3011_min_cidades_pf_pf` | `bb_2014_11_novembro_3011_min_cidades_pf_pf` | 104880 | full_refresh |
| `bb_2014_11_novembro_3011_min_cidades_pj` | `bb_2014_11_novembro_3011_min_cidades_pj` | 1760 | full_refresh |
| `bb_2014_11_novembro_3011_min_cidades_pj_pf` | `bb_2014_11_novembro_3011_min_cidades_pj_pf` | 157446 | full_refresh |
| `bb_2014_11_novembro_3011_pnhr` | `bb_2014_11_novembro_3011_pnhr_30112014` | 1864 | full_refresh |
| `bb_2014_12_dezembro_min_cidades_pf_pf` | `bb_2014_12_dezembro_min_cidades_pf_pf` | 111596 | full_refresh |
| `bb_2014_12_dezembro_min_cidades_pj` | `bb_2014_12_dezembro_min_cidades_pj` | 1898 | full_refresh |
| `bb_2014_12_dezembro_min_cidades_pj_pf` | `bb_2014_12_dezembro_min_cidades_pj_pf` | 164470 | full_refresh |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pf_pf` | `bb_2015_01_janeiro_2015_01_15_min_cidades_pf_pf` | 114306 | full_refresh |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pj` | `bb_2015_01_janeiro_2015_01_15_min_cidades_pj` | 1882 | full_refresh |
| `bb_2015_01_janeiro_2015_01_15_min_cidades_pj_pf` | `bb_2015_01_janeiro_2015_01_15_min_cidades_pj_pf` | 166454 | full_refresh |
| `bb_2015_01_janeiro_2015_01_15_pnhr` | `bb_2015_01_janeiro_2015_01_15_pnhr_15012015` | 2042 | full_refresh |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pf_pf` | `bb_2015_01_janeiro_2015_01_31_min_cidades_pf_pf` | 117338 | full_refresh |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pj` | `bb_2015_01_janeiro_2015_01_31_min_cidades_pj` | 1864 | full_refresh |
| `bb_2015_01_janeiro_2015_01_31_min_cidades_pj_pf` | `bb_2015_01_janeiro_2015_01_31_min_cidades_pj_pf` | 169260 | full_refresh |
| `bb_2015_02_fevereiro_min_cidades_pf_pf` | `bb_2015_02_fevereiro_min_cidades_pf_pf` | 123090 | full_refresh |
| `bb_2015_02_fevereiro_min_cidades_pj` | `bb_2015_02_fevereiro_min_cidades_pj` | 1852 | full_refresh |
| `bb_2015_02_fevereiro_min_cidades_pj_2015_02` | `bb_2015_02_fevereiro_min_cidades_pj_2015_02` | 1852 | full_refresh |
| `bb_2015_02_fevereiro_min_cidades_pj_pf` | `bb_2015_02_fevereiro_min_cidades_pj_pf` | 175916 | full_refresh |
| `bb_2015_02_fevereiro_pnhr` | `bb_2015_02_fevereiro_pnhr_28022015` | 2046 | full_refresh |
| `bb_2015_03_marco_cgu_of_6263_tab_emp` | `bb_2015_03_marco_cgu_of_6263_tab_emp_20150831` | 0 | full_refresh |
| `bb_2015_03_marco_min_cidades_pf_pf` | `bb_2015_03_marco_min_cidades_pf_pf` | 126234 | full_refresh |
| `bb_2015_03_marco_min_cidades_pj` | `bb_2015_03_marco_min_cidades_pj` | 1884 | full_refresh |
| `bb_2015_03_marco_min_cidades_pj_2015_03` | `bb_2015_03_marco_min_cidades_pj_2015_03` | 1884 | full_refresh |
| `bb_2015_03_marco_min_cidades_pj_pf` | `bb_2015_03_marco_min_cidades_pj_pf` | 180232 | full_refresh |
| `bb_2015_04_abril_min_cidades_pf_pf` | `bb_2015_04_abril_min_cidades_pf_pf` | 135648 | full_refresh |
| `bb_2015_04_abril_min_cidades_pj` | `bb_2015_04_abril_min_cidades_pj` | 1878 | full_refresh |
| `bb_2015_04_abril_min_cidades_pj_pf` | `bb_2015_04_abril_min_cidades_pj_pf` | 194400 | full_refresh |
| `bb_2015_04_abril_pnhr` | `bb_2015_04_abril_pnhr_30042015` | 2054 | full_refresh |
| `bb_2015_05_maio_2015_05_31_min_cidades_pf_pf` | `bb_2015_05_maio_2015_05_31_min_cidades_pf_pf` | 141620 | full_refresh |
| `bb_2015_05_maio_2015_05_31_min_cidades_pj` | `bb_2015_05_maio_2015_05_31_min_cidades_pj` | 1884 | full_refresh |
| `bb_2015_05_maio_2015_05_31_min_cidades_pj_pf` | `bb_2015_05_maio_2015_05_31_min_cidades_pj_pf` | 200688 | full_refresh |
| `bb_2015_06_junho_min_cidades_pf_pf` | `bb_2015_06_junho_min_cidades_pf_pf` | 148010 | full_refresh |
| `bb_2015_06_junho_min_cidades_pj` | `bb_2015_06_junho_min_cidades_pj` | 1940 | full_refresh |
| `bb_2015_06_junho_min_cidades_pj_pf` | `bb_2015_06_junho_min_cidades_pj_pf` | 208324 | full_refresh |
| `bb_2015_08_agosto_loginfesta` | `bb_2015_08_agosto_loginfesta` | 256 | full_refresh |
| `bb_2015_08_agosto_min_cidades_pf_pf` | `bb_2015_08_agosto_min_cidades_pf_pf` | 157924 | full_refresh |
| `bb_2015_08_agosto_min_cidades_pj` | `bb_2015_08_agosto_min_cidades_pj` | 1978 | full_refresh |
| `bb_2015_08_agosto_min_cidades_pj_pf` | `bb_2015_08_agosto_min_cidades_pj_pf` | 219008 | full_refresh |
| `bb_2015_08_agosto_min_cidades_pj_sem_aspas` | `bb_2015_08_agosto_min_cidades_pj_sem_aspas` | 1978 | full_refresh |
| `bb_2015_09_setembro_min_cidades_pf_pf` | `bb_2015_09_setembro_min_cidades_pf_pf` | 163772 | full_refresh |
| `bb_2015_09_setembro_min_cidades_pj` | `bb_2015_09_setembro_min_cidades_pj` | 1982 | full_refresh |
| `bb_2015_09_setembro_min_cidades_pj_pf` | `bb_2015_09_setembro_min_cidades_pj_pf` | 225374 | full_refresh |
| `bb_2015_09_setembro_pnhr` | `bb_2015_09_setembro_pnhr_30092015` | 1774 | full_refresh |
| `bb_2015_10_outubro_min_cidades_pf_pf` | `bb_2015_10_outubro_min_cidades_pf_pf` | 169482 | full_refresh |
| `bb_2015_10_outubro_min_cidades_pj` | `bb_2015_10_outubro_min_cidades_pj` | 2012 | full_refresh |
| `bb_2015_10_outubro_min_cidades_pj_pf` | `bb_2015_10_outubro_min_cidades_pj_pf` | 229446 | full_refresh |
| `bb_2015_10_outubro_pnhr` | `bb_2015_10_outubro_pnhr_31102015` | 1942 | full_refresh |
| `bb_2015_12_dezembro_min_cidades_pf_pf` | `bb_2015_12_dezembro_min_cidades_pf_pf` | 176774 | full_refresh |
| `bb_2015_12_dezembro_min_cidades_pj` | `bb_2015_12_dezembro_min_cidades_pj` | 2060 | full_refresh |
| `bb_2015_12_dezembro_min_cidades_pj_pf` | `bb_2015_12_dezembro_min_cidades_pj_pf` | 243122 | full_refresh |
| `bb_2015_12_dezembro_pnhr` | `bb_2015_12_dezembro_pnhr_31122015` | 1982 | full_refresh |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pf_pf` | `bb_2016_01_janeiro_2016_01_31_min_cidades_pf_pf` | 178032 | full_refresh |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pj` | `bb_2016_01_janeiro_2016_01_31_min_cidades_pj` | 2074 | full_refresh |
| `bb_2016_01_janeiro_2016_01_31_min_cidades_pj_pf` | `bb_2016_01_janeiro_2016_01_31_min_cidades_pj_pf` | 244616 | full_refresh |
| `bb_2016_01_janeiro_pnhr` | `bb_2016_01_janeiro_pnhr_31012016` | 1988 | full_refresh |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pf_pf` | `bb_2016_02_fevereiro_2016_02_29_min_cidades_pf_pf` | 181296 | full_refresh |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj` | `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj` | 2066 | full_refresh |
| `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj_pf` | `bb_2016_02_fevereiro_2016_02_29_min_cidades_pj_pf` | 246374 | full_refresh |
| `bb_2016_02_fevereiro_pnhr` | `bb_2016_02_fevereiro_pnhr_29022016` | 1982 | full_refresh |
| `bb_2016_04_abril_min_cidades_pf_pf` | `bb_2016_04_abril_min_cidades_pf_pf` | 190980 | full_refresh |
| `bb_2016_04_abril_min_cidades_pj` | `bb_2016_04_abril_min_cidades_pj` | 2006 | full_refresh |
| `bb_2016_04_abril_min_cidades_pj_pf` | `bb_2016_04_abril_min_cidades_pj_pf` | 286836 | full_refresh |
| `bb_2016_04_abril_pnhr` | `bb_2016_04_abril_pnhr_30042016` | 1994 | full_refresh |
| `bb_2016_05_maio_2016_05_31_min_cidades_pf_pf` | `bb_2016_05_maio_2016_05_31_min_cidades_pf_pf` | 192822 | full_refresh |
| `bb_2016_05_maio_2016_05_31_min_cidades_pj` | `bb_2016_05_maio_2016_05_31_min_cidades_pj` | 1984 | full_refresh |
| `bb_2016_05_maio_2016_05_31_min_cidades_pj_pf` | `bb_2016_05_maio_2016_05_31_min_cidades_pj_pf` | 293560 | full_refresh |
| `bb_2016_05_maio_pnhr` | `bb_2016_05_maio_pnhr_31052016` | 1996 | full_refresh |
| `bb_2016_06_junho_2016_06_30_min_cidades_pf_pf` | `bb_2016_06_junho_2016_06_30_min_cidades_pf_pf` | 196188 | full_refresh |
| `bb_2016_06_junho_2016_06_30_min_cidades_pj` | `bb_2016_06_junho_2016_06_30_min_cidades_pj` | 1990 | full_refresh |
| `bb_2016_06_junho_2016_06_30_min_cidades_pj_pf` | `bb_2016_06_junho_2016_06_30_min_cidades_pj_pf` | 301590 | full_refresh |
| `bb_2016_06_junho_pnhr` | `bb_2016_06_junho_pnhr_30062016` | 2016 | full_refresh |
| `bb_2016_07_julho_2016_07_31_min_cidades_pj` | `bb_2016_07_julho_2016_07_31_min_cidades_pj` | 1986 | full_refresh |
| `bb_2016_07_julho_min_cidades_pf_pf` | `bb_2016_07_julho_min_cidades_pf_pf` | 199028 | full_refresh |
| `bb_2016_07_julho_min_cidades_pj_pf` | `bb_2016_07_julho_min_cidades_pj_pf` | 310302 | full_refresh |
| `bb_2016_07_julho_pnhr` | `bb_2016_07_julho_pnhr_31072016` | 2016 | full_refresh |
| `bb_2018_2018_04_06_pf_pf` | `bb_2018_2018_04_06_pf_pf` | 230204 | full_refresh |
| `bb_2018_2018_04_06_pj` | `bb_2018_2018_04_06_pj` | 1962 | full_refresh |
| `bb_2018_2018_04_06_pj_pf` | `bb_2018_2018_04_06_pj_pf` | 406360 | full_refresh |
| `bb_2018_2018_05_10_pf_pf` | `bb_2018_2018_05_10_pf_pf` | 232432 | full_refresh |
| `bb_2018_2018_05_10_pj` | `bb_2018_2018_05_10_pj` | 1964 | full_refresh |
| `bb_2018_2018_05_10_pj_pf` | `bb_2018_2018_05_10_pj_pf` | 413964 | full_refresh |
| `bb_2018_2018_06_10_pf_pf` | `bb_2018_2018_06_10_pf_pf` | 234842 | full_refresh |
| `bb_2018_2018_06_10_pj` | `bb_2018_2018_06_10_pj` | 1972 | full_refresh |
| `bb_2018_2018_06_10_pj_pf` | `bb_2018_2018_06_10_pj_pf` | 429472 | full_refresh |
| `bb_2018_2018_07_03_pf_pf` | `bb_2018_2018_07_03_pf_pf` | 236850 | full_refresh |
| `bb_2018_2018_07_03_pj` | `bb_2018_2018_07_03_pj` | 1960 | full_refresh |
| `bb_2018_2018_07_03_pj_pf` | `bb_2018_2018_07_03_pj_pf` | 436298 | full_refresh |
| `bb_2018_2018_07_03_pnhr` | `bb_2018_2018_07_03_pnhr_04072018` | 1998 | full_refresh |
| `bb_2018_2018_08_06_pf_pf` | `bb_2018_2018_08_06_pf_pf` | 239848 | full_refresh |
| `bb_2018_2018_08_06_pj` | `bb_2018_2018_08_06_pj` | 1962 | full_refresh |
| `bb_2018_2018_08_06_pj_pf` | `bb_2018_2018_08_06_pj_pf` | 439992 | full_refresh |
| `bb_2018_2018_09_10_pf_pf` | `bb_2018_2018_09_10_pf_pf` | 242928 | full_refresh |
| `bb_2018_2018_09_10_pj` | `bb_2018_2018_09_10_pj` | 1968 | full_refresh |
| `bb_2018_2018_09_10_pj_pf` | `bb_2018_2018_09_10_pj_pf` | 447388 | full_refresh |
| `bb_2018_2018_10_01_pf_pf` | `bb_2018_2018_10_01_pf_pf` | 244860 | full_refresh |
| `bb_2018_2018_10_01_pj` | `bb_2018_2018_10_01_pj` | 1972 | full_refresh |
| `bb_2018_2018_10_01_pj_pf` | `bb_2018_2018_10_01_pj_pf` | 453934 | full_refresh |
| `bb_2018_2018_10_30_pf_pf` | `bb_2018_2018_10_30_pf_pf` | 247770 | full_refresh |
| `bb_2018_2018_10_30_pj` | `bb_2018_2018_10_30_pj` | 1980 | full_refresh |
| `bb_2018_2018_10_30_pj_pf` | `bb_2018_2018_10_30_pj_pf` | 463290 | full_refresh |
| `bb_2019_2019_03_07_pf_pf` | `bb_2019_2019_03_07_pf_pf` | 256854 | full_refresh |
| `bb_2019_2019_03_07_pj_pf` | `bb_2019_2019_03_07_pj_pf` | 2004 | full_refresh |
| `bb_2019_2019_05_07_2019_05_07_pj` | `bb_2019_2019_05_07_2019_05_07_pj` | 2008 | full_refresh |
| `bb_2019_2019_05_07_pf_pf` | `bb_2019_2019_05_07_pf_pf` | 261114 | full_refresh |
| `bb_2019_2019_05_07_pj_pf` | `bb_2019_2019_05_07_pj_pf` | 482750 | full_refresh |
| `bext_abr18` | `caixa_002_2018_bext_abr18` | 590228 | full_refresh |
| `bext_abr2017` | `caixa_002_2017_bext_abr2017` | 569426 | full_refresh |
| `bext_ago18` | `caixa_002_2018_bext_ago18` | 600794 | full_refresh |
| `bext_ago2017` | `caixa_002_2017_bext_ago2017` | 578078 | full_refresh |
| `bext_dez17` | `caixa_002_2017_bext_dez17` | 584348 | full_refresh |
| `bext_fev2017` | `caixa_002_2017_bext_fev2017` | 567468 | full_refresh |
| `bext_jan` | `caixa_003_2017_bext_jan_2017` | 567468 | full_refresh |
| `bext_jul2017` | `caixa_003_2017_bext_jul2017` | 569426 | full_refresh |
| `bext_jun18` | `caixa_002_2018_bext_jun18` | 594684 | full_refresh |
| `bext_mai18` | `caixa_002_2018_bext_mai18` | 592670 | full_refresh |
| `bext_mai2017` | `caixa_003_2017_bext_mai2017` | 569426 | full_refresh |
| `bext_mar18` | `caixa_002_2018_bext_mar18` | 587988 | full_refresh |
| `bext_mar2017` | `caixa_003_2017_bext_mar2017` | 569426 | full_refresh |
| `bext_out2017` | `caixa_003_2017_bext_out2017` | 583308 | full_refresh |
| `contratação_por_uf_nov` | `caixa_001_2017_contratação_por_uf_nov_2017` | 226 | full_refresh |
| `datas_entregas_pj_mcmv` | `caixa_001_2015_datas_entregas_pj_mcmv_30062015` | 98964 | full_refresh |
| `dezembro_2009_pmcmv_07_12` | `caixa_001_2009_12_dezembro_2009_pmcmv_07_12_2009` | 536 | full_refresh |
| `dezembro_2009_pmcmv_14_12` | `caixa_001_2009_12_dezembro_2009_pmcmv_14_12_2009` | 536 | full_refresh |
| `dezembro_2009_pmcmv_18_12` | `caixa_001_2009_12_dezembro_2009_pmcmv_18_12_2009` | 536 | full_refresh |
| `dezembro_2009_pmcmv_20091207cc_análise` | `caixa_001_2009_12_dezembro_2009_pmcmv_20091207cc_análise` | 536 | full_refresh |
| `dezembro_2009_pmcmv_24_12_2009_parte1` | `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte1` | 536 | full_refresh |
| `dezembro_2009_pmcmv_24_12_2009_parte2` | `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2` | 19016 | full_refresh |
| `dezembro_2009_pmcmv_31_12` | `caixa_001_2009_12_dezembro_2009_pmcmv_31_12_2009` | 536 | full_refresh |
| `dezembro_2009_pmcmv_31_12_2009_2` | `caixa_001_2009_12_dezembro_2009_pmcmv_31_12_2009_2` | 536 | full_refresh |
| `dezembro_2010_balanco_pmcmv_15_12` | `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_15_12_2010` | 536 | full_refresh |
| `dezembro_2010_balanco_pmcmv_20_12` | `caixa_001_2010_12___dezembro_2010_balanco_pmcmv_20_12_2010` | 536 | full_refresh |
| `dezembro_2010_pmcmv_pj_07_12` | `caixa_001_2010_12___dezembro_2010_pmcmv_pj_07_12_2010` | 536 | full_refresh |
| `dezembro_bases_relat_rio_executivo` | `caixa_001_2013_12___dezembro_bases_relat_rio_executivo` | 29888 | full_refresh |
| `dezembro_bases_relatório_executivo_31122015v` | `caixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v` | 39200 | full_refresh |
| `dezembro_bext` | `caixa_002_2015_12_dezembro_bext_31122015` | 531576 | full_refresh |
| `dezembro_pmcmv_3_relatório_executivo_3112201` | `caixa_002_2015_12_dezembro_pmcmv_3_relatório_executivo_3112201` | 536 | full_refresh |
| `dezembro_rel_executivo_resumo_31122015v2` | `caixa_001_2015_12_dezembro_rel_executivo_resumo_31122015v2` | 536 | full_refresh |
| `elatorio_executivo_31102014_bases_relatório_executivo` | `elatorio_executivo_31102014_bases_relatório_executivo_31102014` | 34270 | full_refresh |
| `elatorio_executivo_31102014_pmcmv_relatório_executivo` | `elatorio_executivo_31102014_pmcmv_relatório_executivo_31102014` | 536 | full_refresh |
| `fevereiro_2010_pmcmv_05_02_2010_caixa` | `caixa_001_2010_02___fevereiro_2010_pmcmv_05_02_2010___caixa` | 536 | full_refresh |
| `fevereiro_2010_pmcmv_12_02_2010_caixa` | `caixa_001_2010_02___fevereiro_2010_pmcmv_12_02_2010___caixa` | 536 | full_refresh |
| `fevereiro_2010_pmcmv_19_02_10_caixa` | `caixa_001_2010_02___fevereiro_2010_pmcmv_19_02_10_caixa` | 536 | full_refresh |
| `fevereiro_bases_relatório_executivo` | `caixa_001_2015_02_fevereiro_bases_relatório_executivo_28022015` | 36706 | full_refresh |
| `fevereiro_propostas_base_far` | `caixa_001_2013_02___fevereiro_propostas_base_far` | 1928 | full_refresh |
| `fevereiro_relatorio_cidades` | `caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2` | 533706 | full_refresh |
| `grafico_mcmv` | `caixa_001_2016_grafico_mcmv_31082016` | 16 | full_refresh |
| `int040_ministeriocidades_far_caixa_empreendimentos` | `018_int040_ministeriocidades_far_caixa_empreendimentos_20181001` | 8224 | full_refresh |
| `ixa_001_2010_09_setembro_2010_contratação_pf_total` | `ixa_001_2010_09___setembro_2010_contratação_pf_total___060920` | 19928 | full_refresh |
| `ixa_001_2011_09_setembro_presidencia_automatizado_mcmvii` | `ixa_001_2011_09_setembro_presidencia_automatizado_mcmvii_082011` | 11128 | full_refresh |
| `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_13012012_base` | `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_13012012_base` | 8006 | full_refresh |
| `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base` | `ixa_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base` | 8208 | full_refresh |
| `ixa_001_2014_12_dezembro_bases_relatório_executivo` | `ixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v2` | 36202 | full_refresh |
| `ixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo` | `ixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_28022015` | 536 | full_refresh |
| `ixa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v` | `ixa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v` | 536 | full_refresh |
| `janeiro_2010_pmcmv_26_01` | `caixa_001_2010_01__janeiro_2010_pmcmv_26_01_2010` | 536 | full_refresh |
| `janeiro_pmcmv_3_relatório_executivo` | `caixa_001_2015_01_janeiro_pmcmv_3_relatório_executivo_31012015` | 536 | full_refresh |
| `janeiro_pmcmv_relatório_executivo` | `caixa_001_2013_01___janeiro_pmcmv_relatório_executivo_31012013` | 536 | full_refresh |
| `janeiro_rel_executivo_resumo` | `caixa_001_2013_01___janeiro_rel_executivo_resumo_31012013` | 536 | full_refresh |
| `julho_2009_pmcmv_02_07_09_caixa` | `caixa_001_2009_07_julho_2009_pmcmv_02_07_09___caixa` | 536 | full_refresh |
| `julho_2009_pmcmv_10_07_2009_casa_civil` | `caixa_001_2009_07_julho_2009_pmcmv_10_07_2009___casa_civil` | 536 | full_refresh |
| `julho_2009_pmcmv_17_07_2009_caixa` | `caixa_001_2009_07_julho_2009_pmcmv_17_07_2009___caixa` | 536 | full_refresh |
| `julho_2009_pmcmv_24_07_2009_caixa` | `caixa_001_2009_07_julho_2009_pmcmv_24_07_2009___caixa` | 536 | full_refresh |
| `julho_2009_pmcmv_31_07_2009_caixa` | `caixa_001_2009_07_julho_2009_pmcmv_31_07_2009___caixa` | 536 | full_refresh |
| `julho_2010_pmcmv23072010` | `caixa_001_2010_07___julho_2010_pmcmv23072010` | 536 | full_refresh |
| `julho_2010_pmcmv30062010_1semde2010` | `caixa_001_2010_07___julho_2010_pmcmv30062010_1semde2010` | 536 | full_refresh |
| `julho_2010_pmcmv30072010` | `caixa_001_2010_07___julho_2010_pmcmv30072010` | 536 | full_refresh |
| `julho_2010_pmcmv_16_07` | `caixa_001_2010_07___julho_2010_pmcmv_16_07_2010` | 536 | full_refresh |
| `julho_2010_pmcmv_30_07` | `caixa_001_2010_07___julho_2010_pmcmv_30_07_2010` | 536 | full_refresh |
| `julho_bases_relatório_executivo_24_07_12` | `caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12` | 90 | full_refresh |
| `julho_bases_relatório_executivo_31_07_12` | `caixa_001_2012_07_julho_bases_relatório_executivo_31_07_12` | 16408 | full_refresh |
| `julho_bext` | `caixa_002_2015_07_julho_bext_31072015` | 510334 | full_refresh |
| `julho_min_cidades_pf_pf` | `caixa_001_2015_07_julho_min_cidades_pf_pf` | 153170 | full_refresh |
| `julho_min_cidades_pj` | `caixa_001_2015_07_julho_min_cidades_pj` | 1992 | full_refresh |
| `julho_min_cidades_pj_pf` | `caixa_001_2015_07_julho_min_cidades_pj_pf` | 213794 | full_refresh |
| `julho_pmcmv` | `caixa_001_2011_07_julho_pmcmv_20110722` | 536 | full_refresh |
| `julho_pmcmv_3_relatório_executivo` | `caixa_001_2015_07_julho_pmcmv_3_relatório_executivo_31072015` | 536 | full_refresh |
| `julho_pmcmv_automatizado_continua_15072011_pmcmv_pr` | `julho_pmcmv_automatizado_continua_15072011_pmcmv__pr___15072011` | 536 | full_refresh |
| `julho_pmcmv_relatorio_executivo_11_07` | `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_11_07_2012` | 536 | full_refresh |
| `julho_pmcmv_relatorio_executivo_11_07_2012_bd` | `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_11_07_2012_bd` | 536 | full_refresh |
| `julho_pmcmv_relatorio_executivo_15_07` | `caixa_001_2012_07_julho_pmcmv_relatorio_executivo_15_07_2012` | 536 | full_refresh |
| `julho_pmcmv_relatório_executivo` | `caixa_001_2012_07_julho_pmcmv_relatório_executivo_31072012_v2` | 536 | full_refresh |
| `julho_pmcmv_relatório_executivo_24_07_12` | `caixa_001_2012_07_julho_pmcmv_relatório_executivo_24_07_12` | 536 | full_refresh |
| `julho_pmcmv_relatório_executivo_31_07_12` | `caixa_001_2012_07_julho_pmcmv_relatório_executivo_31_07_12` | 536 | full_refresh |
| `julho_rel_executivo_resumo` | `caixa_001_2015_07_julho_rel_executivo_resumo_31072015` | 536 | full_refresh |
| `junho_2009_pmcmv_09_06_09_caixa` | `caixa_001_2009_06_junho_2009_pmcmv_09_06_09___caixa` | 536 | full_refresh |
| `junho_2009_pmcmv_19_06_2009_caixa` | `caixa_001_2009_06_junho_2009_pmcmv_19_06_2009___caixa` | 536 | full_refresh |
| `junho_2009_pmcmv_26_06_2009_caixa` | `caixa_001_2009_06_junho_2009_pmcmv_26_06_2009___caixa` | 536 | full_refresh |
| `junho_2010_contratação30062010` | `caixa_001_2010_06___junho_2010_contratação30062010` | 4450 | full_refresh |
| `junho_2010_pmcmv30062010_1semde2010` | `caixa_001_2010_06___junho_2010_pmcmv30062010_1semde2010` | 536 | full_refresh |
| `junho_bases_relatório_executivo` | `caixa_001_2015_06_junho_bases_relatório_executivo_30062015` | 37604 | full_refresh |
| `junho_bext` | `caixa_001_2015_06_junho_bext_30062015` | 505104 | full_refresh |
| `junho_pmcmv` | `caixa_001_2011_06_junho_pmcmv_20110624` | 536 | full_refresh |
| `junho_pmcmv_3_relatório_executivo` | `caixa_001_2015_06_junho_pmcmv_3_relatório_executivo_30062015` | 536 | full_refresh |
| `junho_pmcmv_automatizado_continua` | `caixa_001_2011_06_junho_pmcmv_automatizado_continua_24062011` | 536 | full_refresh |
| `junho_pmcmv_relatorio_executivo_30_06` | `caixa_001_2012_06_junho_pmcmv_relatorio_executivo_30_06_2012` | 536 | full_refresh |
| `junho_pmcmv_relatorio_executivo_30_06_2012v2` | `caixa_001_2012_06_junho_pmcmv_relatorio_executivo_30_06_2012v2` | 536 | full_refresh |
| `junho_presidencia` | `caixa_001_2011_06_junho_presidencia` | 11128 | full_refresh |
| `junho_rel_executivo_resumo` | `caixa_001_2015_06_junho_rel_executivo_resumo_30062015` | 536 | full_refresh |
| `maio_5b1_base_empreend_contratado` | `caixa_001_2012_05_maio_5b1__base_empreend_contratado` | 7954 | full_refresh |
| `maio_pmcmv` | `caixa_001_2011_05_maio_pmcmv_20110531` | 536 | full_refresh |
| `maio_pmcmv_2` | `caixa_001_2011_05_maio_pmcmv_2_20110531` | 536 | full_refresh |
| `maio_pmcmv_3_relat_rio_executivo` | `caixa_001_2015_05_maio_pmcmv_3_relat_rio_executivo_31052015` | 536 | full_refresh |
| `maio_pmcmv_acumulado` | `caixa_001_2011_05_maio_pmcmv_acumulado` | 536 | full_refresh |
| `maio_pmcmv_presidencia_2` | `caixa_001_2011_05_maio__pmcmv_presidencia_2` | 536 | full_refresh |
| `maio_pmcmv_relat_rio_executivo` | `caixa_001_2013_05___maio_pmcmv_relat_rio_executivo_150513` | 536 | full_refresh |
| `maio_pmcmv_relatorio_executivo_07_05` | `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_07_05_2012` | 536 | full_refresh |
| `maio_pmcmv_relatorio_executivo_14_05` | `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_14_05_2012` | 536 | full_refresh |
| `maio_pmcmv_relatorio_executivo_28_05` | `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_28_05_2012` | 536 | full_refresh |
| `maio_pmcmv_relatorio_executivo_31_05` | `caixa_001_2012_05_maio_pmcmv_relatorio_executivo_31_05_2012` | 536 | full_refresh |
| `maio_presidencia` | `caixa_001_2011_05_maio_presidencia` | 536 | full_refresh |
| `maio_presidencia1_16_05` | `caixa_001_2011_05_maio_presidencia1__16_05_2011` | 536 | full_refresh |
| `marco_2010_pmcmv` | `caixa_001_2010_03___marco_2010_pmcmv_20100305` | 536 | full_refresh |
| `marco_2010_pmcmv_20100312_cc` | `caixa_001_2010_03___marco_2010_pmcmv_20100312_cc` | 536 | full_refresh |
| `marco_2010_pmcmv_20100319_cc` | `caixa_001_2010_03___marco_2010_pmcmv_20100319_cc` | 536 | full_refresh |
| `marco_bases_relatório_executivo` | `caixa_001_2015_03_marco_bases_relatório_executivo_31032015` | 36884 | full_refresh |
| `marco_bases_relatório_executivo_31032015v2` | `caixa_001_2015_03_marco_bases_relatório_executivo_31032015v2` | 36884 | full_refresh |
| `marco_pmcmv_3_relatório_executivo` | `caixa_001_2015_03_marco_pmcmv_3_relatório_executivo_31032015` | 536 | full_refresh |
| `marco_pmcmv_relatorio_executivo_09_03` | `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_09_03_2012` | 536 | full_refresh |
| `marco_pmcmv_relatorio_executivo_16_03` | `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_16_03_2012` | 536 | full_refresh |
| `marco_pmcmv_relatorio_executivo_27_03` | `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012` | 536 | full_refresh |
| `marco_pmcmv_relatorio_executivo_31_03` | `caixa_001_2012_03_marco_pmcmv_relatorio_executivo_31_03_2012` | 536 | full_refresh |
| `marco_rel_executivo_resumo` | `caixa_001_2015_03_marco_rel_executivo_resumo_31032015` | 536 | full_refresh |
| `marco_rel_executivo_resumo_2015_31032015v2` | `caixa_001_2015_03_marco_rel_executivo_resumo_2015_31032015v2` | 536 | full_refresh |
| `março_bases_relat_rio_executivo` | `caixa_001_2013_03___março_bases_relat_rio_executivo_28032013` | 22602 | full_refresh |
| `março_pmcmv` | `caixa_001_2011_03_março_pmcmv_14032011` | 536 | full_refresh |
| `março_pmcmv_relat_rio_executivo` | `caixa_001_2013_03___março_pmcmv_relat_rio_executivo_28032013` | 536 | full_refresh |
| `matizado_continua_15072011_pmcmv_automatizado_continua` | `matizado_continua_15072011_pmcmv_automatizado_continua_15072011` | 536 | full_refresh |
| `mbro_2009_relatorio_automatizado_31dez09final_com_base_valor_mc` | `mbro_2009_relatorio_automatizado_31dez09final_com_base_valor_mc` | 58 | full_refresh |
| `novembro_2009_pmcmv_06_11_2009_caixa` | `caixa_001_2009_11_novembro_2009_pmcmv_06_11_2009___caixa` | 536 | full_refresh |
| `novembro_2009_pmcmv_16_11` | `caixa_001_2009_11_novembro_2009_pmcmv_16_11_2009` | 536 | full_refresh |
| `novembro_2009_pmcmv_20_11_2009_caixa` | `caixa_001_2009_11_novembro_2009_pmcmv_20_11_2009___caixa` | 536 | full_refresh |
| `novembro_2009_pmcmv_30_11_2009_caixa` | `caixa_001_2009_11_novembro_2009_pmcmv_30_11_2009___caixa` | 536 | full_refresh |
| `novembro_2010_pmcmv_automatizado03112010` | `caixa_001_2010_11___novembro_2010_pmcmv_automatizado03112010` | 536 | full_refresh |
| `novembro_bases_relat_rio_executivo` | `caixa_001_2012_11_novembro_bases_relat_rio_executivo_12112012` | 19036 | full_refresh |
| `novembro_pmcmv_3_relatório_executivo_3011201` | `caixa_001_2015_11_novembro_pmcmv_3_relatório_executivo_3011201` | 536 | full_refresh |
| `novembro_pmcmv_relat_rio_executivo` | `caixa_001_2012_11_novembro_pmcmv_relat_rio_executivo_12112012` | 536 | full_refresh |
| `novembro_rel_executivo_resumo_comerc` | `caixa_001_2014_11_novembro_rel_executivo_resumo_comerc_30112014` | 536 | full_refresh |
| `novembro_relat_rio_executivo_mcid_21_11_11` | `caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11` | 13686 | full_refresh |
| `novembro_relatório_executivo_mcid_21_11_11` | `caixa_001_2011_11_novembro_relatório_executivo_mcid_21_11_11` | 13686 | full_refresh |
| `novo_relat_rio_executivo` | `caixa_002_2018_novo_relat_rio_executivo` | 34 | full_refresh |
| `novo_relat_rio_executivo_maio2018` | `caixa_002_2018_novo_relat_rio_executivo_maio2018` | 34 | full_refresh |
| `o_relatorio_executivo_30092012_v3_rel_executivo_resumo` | `o_relatorio_executivo_30092012_v3_rel_executivo_resumo_30092012` | 536 | full_refresh |
| `outubro_2009_pmcmv_08_10_2009_caixa` | `caixa_001_2009_10_outubro_2009_pmcmv_08_10_2009___caixa` | 30 | full_refresh |
| `outubro_2009_pmcmv_09_10_2009_caixa` | `caixa_001_2009_10_outubro_2009_pmcmv_09_10_2009___caixa` | 536 | full_refresh |
| `outubro_2009_pmcmv_16_10_2009_caixa` | `caixa_001_2009_10_outubro_2009_pmcmv_16_10_2009___caixa` | 536 | full_refresh |
| `outubro_2009_pmcmv_23_10_2009_caixa` | `caixa_001_2009_10_outubro_2009_pmcmv_23_10_2009___caixa` | 536 | full_refresh |
| `outubro_2009_pmcmv_30_10_2009_caixa` | `caixa_001_2009_10_outubro_2009_pmcmv_30_10_2009___caixa` | 536 | full_refresh |
| `outubro_2009_pmcmv_gráfico` | `caixa_001_2009_10_outubro_2009_pmcmv_gráfico` | 536 | full_refresh |
| `outubro_2010_pmcmv15102010v1` | `caixa_001_2010_10___outubro_2010_pmcmv15102010v1` | 536 | full_refresh |
| `outubro_2010_pmcmv25102010` | `caixa_001_2010_10___outubro_2010_pmcmv25102010` | 536 | full_refresh |
| `outubro_bext` | `caixa_001_2015_10_outubro_bext_31102015` | 526740 | full_refresh |
| `outubro_pmcmv` | `caixa_001_2011_10_outubro_pmcmv_20111025` | 536 | full_refresh |
| `outubro_pmcmv_3_relatório_executivo` | `caixa_001_2015_10_outubro_pmcmv_3_relatório_executivo_31102015` | 536 | full_refresh |
| `outubro_pmcmv_3_relatório_executivo31102015` | `caixa_001_2015_10_outubro_pmcmv_3_relatório_executivo31102015` | 536 | full_refresh |
| `outubro_pmcmv_relatório_executivo` | `caixa_001_2012_10_outubro_pmcmv_relatório_executivo_31102012` | 536 | full_refresh |
| `outubro_rel_executivo_resumo` | `caixa_001_2012_10_outubro_rel_executivo_resumo_31102012` | 536 | full_refresh |
| `pmcmv_31_12` | `caixa_001_2009_pmcmv_31_12_2009` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_31012017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_abr2017` | `caixa_002_2017_pmcmv_3_relat_rio_executivo_abr2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_ago2017` | `caixa_002_2017_pmcmv_3_relat_rio_executivo_ago2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_fev2017` | `caixa_002_2017_pmcmv_3_relat_rio_executivo_fev2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_jul2017` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_jul2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_jun2017` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_jun2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_jun2018` | `caixa_002_2018_pmcmv_3_relat_rio_executivo_jun2018` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_mai2017` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_mai2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_mai2018` | `caixa_002_2018_pmcmv_3_relat_rio_executivo_mai2018` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_mar18` | `caixa_002_2018_pmcmv_3_relat_rio_executivo_mar18` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_mar2017` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_mar2017` | 536 | full_refresh |
| `pmcmv_3_relat_rio_executivo_nov17` | `caixa_003_2017_pmcmv_3_relat_rio_executivo_nov17` | 536 | full_refresh |
| `pmcmv_3_relatório_executivo_ago2018` | `caixa_001_2018_pmcmv_3_relatório_executivo_ago2018` | 536 | full_refresh |
| `pmcmv_3_relatório_executivo_dez17` | `caixa_002_2017_pmcmv_3_relatório_executivo_dez17` | 536 | full_refresh |
| `rel_executivo_resumo_ago2017` | `caixa_002_2017_rel_executivo_resumo_ago2017` | 536 | full_refresh |
| `rel_executivo_resumo_ago2018` | `caixa_001_2018_rel_executivo_resumo_ago2018` | 536 | full_refresh |
| `rel_executivo_resumo_dez17` | `caixa_001_2017_rel_executivo_resumo_dez17` | 536 | full_refresh |
| `rel_executivo_resumo_fev2017` | `caixa_002_2017_rel_executivo_resumo_fev2017` | 536 | full_refresh |
| `rel_executivo_resumo_jul2017` | `caixa_003_2017_rel_executivo_resumo_jul2017` | 536 | full_refresh |
| `rel_executivo_resumo_jun2017` | `caixa_003_2017_rel_executivo_resumo_jun2017` | 536 | full_refresh |
| `rel_executivo_resumo_jun2018` | `caixa_002_2018_rel_executivo_resumo_jun2018` | 536 | full_refresh |
| `rel_executivo_resumo_mai2017` | `caixa_003_2017_rel_executivo_resumo_mai2017` | 536 | full_refresh |
| `rel_executivo_resumo_mai2018` | `caixa_002_2018_rel_executivo_resumo_mai2018` | 536 | full_refresh |
| `rel_executivo_resumo_mar2017` | `caixa_003_2017_rel_executivo_resumo_mar2017` | 536 | full_refresh |
| `rel_executivo_resumo_mar2018` | `caixa_002_2018_rel_executivo_resumo_mar2018` | 536 | full_refresh |
| `rel_executivo_resumo_nov17` | `caixa_003_2017_rel_executivo_resumo_nov17` | 536 | full_refresh |
| `relatorio_executivo_31102014_bases_relatório_executivo_3110201` | `relatorio_executivo_31102014_bases_relatório_executivo_3110201` | 34270 | full_refresh |
| `relatorio_executivo_31102014_pmcmv_relatório_executivo_3110201` | `relatorio_executivo_31102014_pmcmv_relatório_executivo_3110201` | 536 | full_refresh |
| `setembro_2009_pmcmv_04_09_2009_caixa` | `caixa_001_2009_09_setembro_2009_pmcmv_04_09_2009___caixa` | 536 | full_refresh |
| `setembro_2009_pmcmv_11_09_2009_caixa` | `caixa_001_2009_09_setembro_2009_pmcmv_11_09_2009___caixa` | 536 | full_refresh |
| `setembro_2009_pmcmv_17_09_2009_caixa` | `caixa_001_2009_09_setembro_2009_pmcmv_17_09_2009___caixa` | 536 | full_refresh |
| `setembro_2009_pmcmv_25_09_2009_caixa` | `caixa_001_2009_09_setembro_2009_pmcmv_25_09_2009___caixa` | 536 | full_refresh |
| `setembro_2010_pmcmv06092010` | `caixa_001_2010_09___setembro_2010_pmcmv06092010` | 536 | full_refresh |
| `setembro_2010_pmcmv06092010_desligados` | `caixa_001_2010_09___setembro_2010_pmcmv06092010_desligados` | 5354 | full_refresh |
| `setembro_2010_pmcmv10092010` | `caixa_001_2010_09___setembro_2010_pmcmv10092010` | 536 | full_refresh |
| `setembro_2010_pmcmv21092010` | `caixa_001_2010_09___setembro_2010_pmcmv21092010` | 536 | full_refresh |
| `setembro_2010_pmcmv24092010` | `caixa_001_2010_09___setembro_2010_pmcmv24092010` | 536 | full_refresh |
| `setembro_bases_relatório_executivo_10_09_12` | `caixa_001_2012_09_setembro_bases_relatório_executivo_10_09_12` | 17408 | full_refresh |
| `setembro_bases_relatório_executivo_15_09_12` | `caixa_001_2012_09_setembro_bases_relatório_executivo_15_09_12` | 17566 | full_refresh |
| `setembro_pmcmv_automatizado_novo` | `caixa_001_2011_09_setembro_pmcmv_automatizado_novo_29092011` | 536 | full_refresh |
| `setembro_pmcmv_relatório_executivo_10_09_12` | `caixa_001_2012_09_setembro_pmcmv_relatório_executivo_10_09_12` | 536 | full_refresh |
| `snh_pmcmv_dados_prioritarios_af_bb_vs02_correcao|af_bb|historico_recente` | `ecente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02_correcao` | 2576 | full_refresh |
| `snh_pmcmv_dados_prioritarios_af_bb_vs02|af_bb|historico_recente` | `storico_recente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02` | 2576 | full_refresh |
| `torio_executivo_18_05_2012_pmcmv_relatorio_executivo_18_05` | `torio_executivo_18_05_2012_pmcmv_relatorio_executivo_18_05_2012` | 536 | full_refresh |
| `torio_executivo_30092012_v3_bases_relatório_executivo` | `torio_executivo_30092012_v3_bases_relatório_executivo_30092012` | 18124 | full_refresh |
| `torio_executivo_30092012_v3_pmcmv_relatório_executivo` | `torio_executivo_30092012_v3_pmcmv_relatório_executivo_30092012` | 536 | full_refresh |
| `torio_executivo_31_05_2012_pmcmv_relatorio_executivo_31_05` | `torio_executivo_31_05_2012_pmcmv_relatorio_executivo_31_05_2012` | 536 | full_refresh |
| `ubro_relatorio_executivo_31102014_rel_executivo_resumo` | `ubro_relatorio_executivo_31102014_rel_executivo_resumo_31102014` | 536 | full_refresh |
| `xa_001_2011_12_dezembro_pmcmv_relatorio_executivo_31122011_base` | `xa_001_2011_12_dezembro_pmcmv_relatorio_executivo_31122011_base` | 7032 | full_refresh |
| `xa_001_2012_04_abril_2012_04_18_5a_propostas_recebidas` | `xa_001_2012_04_abril_2012_04_18_5a_propostas_recebidas_18042012` | 4402 | full_refresh |
| `xa_001_2012_04_abril_2012_04_18_5b_empreend_contratado` | `xa_001_2012_04_abril_2012_04_18_5b_empreend_contratado_18042012` | 6272 | full_refresh |
| `xa_001_2012_04_abril_2012_04_18_5c_base_contratação_pf` | `xa_001_2012_04_abril_2012_04_18_5c_base_contratação_pf_180420` | 33914 | full_refresh |
| `xa_001_2013_12_dezembro_pmcmv_relat_rio_executivo` | `xa_001_2013_12___dezembro_pmcmv_relat_rio_executivo_31122013_v2` | 536 | full_refresh |
| `xa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v2` | `xa_001_2015_12_dezembro_pmcmv_3_relatório_executivo_31122015v2` | 536 | full_refresh |
