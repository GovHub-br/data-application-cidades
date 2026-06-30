## Categorias de Classificação quanto à formação

As categorias abaixo foram verificadas manualmente e são a referência autoritativa. O objetivo de classificar quanto à formação é poder definir um tipo de tratamento para cada categoria, fazendo com que as tabelas fiquem formatadas de forma adequada para uso posterior para ML.

### `bem_formada`
Todas as colunas são nomeadas e cada célula tem valores consistentes (mesmo tipo e
estrutura: data, nome, número, etc.). Formato ideal para processamento.

**Exemplos:** `_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base_bd`, `_001_2012_04_abril_2012_04_18_5c_base_contratação_pf_18042012`,`001_2012_10_outubro_20121009_bases_relatório_executivo_0910201`,`001_2012_10_outubro_20121009_bases_relatório_executivo_1610201`,`01_2012_10_outubro_20121009_bases_relatório_executivo_09102012`,`01_2012_10_outubro_20121009_bases_relatório_executivo_16102012`,`1_2018_int054_ministeriocidades_far_bb_empreendimentos_20180831`,`011_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópi`,`11_12_dezembro_pmcmv_relatorio_executivo_31122011_base___cópia`,`018_int040_ministeriocidades_far_caixa_empreendimentos_20181001`,`024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202411_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202412_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202501_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202503_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202504_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202505_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202506_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202507_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202508_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202509_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202510_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202511_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202512_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202601_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202602_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`202603_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb`,`a_001_2010_09___setembro_2010_contratação_pf_total___06092010`,`aixa_001_2013_02___fevereiro_bases_relatório_executivo_1502201`,`aixa_001_2013_02___fevereiro_bases_relatório_executivo_2802201`,`aixa_001_2014_12_dezembro_bases_relatório_executivo_31122014_v`,`aixa_001_2016_02_fevereiro_relatorio_cidades__entregas_20160229`,`bb_2012_05_maio_z_relatorio_caixa_3105`,`bb_2012_10_outubro_entrada_bb_20121031_ajustada`,
`ecente_2024_07_snh_pmcmv_dados_prioritarios_af_bb_vs02_correcao`,`historico_recente_2024_08_snh_pmcmv_dados_prioritarios_af_bb`,`historico_recente_2024_09_snh_pmcmv_dados_prioritarios_af_bb`,`


### `sem_cabecalho`
Os nomes das colunas (lidos pelo pandas) são, na verdade, a primeira linha de
dados. A verdadeira linha de cabeçalho está ausente ou foi perdida na extração.
Quando a primeira linha é numérica, nomes são inferidos via referência cruzada.
Há colunas com os seguintes tipos de dados:
- Numérico (int) com 6 dígitos em todos ou quase todos os valores,indicando um possível código de apf ou código de empreendimento que pode ser utilizado como chave entre tabelas para levantamento do histórico dos empreendimentos, mas também pode ser código ibge de município (exemplo de valor: 330455);
- Numérico (int) com quantidade de dígitos variável, indicando um possível quantidade de unidades de habitação (UH) entregues ou contratadas (exemplo de valor: 3355);
- Numérico (int) com apenas um ou dois dígitos, indicando uma categoria, possivelmente categoria de faixa de renda do programa MCMV (exemplo de valor: 2);
- Texto (str) com formato de moeda corrente, há caracteres numéricos, um separador decimal e mais dois caracteres numéricos (exemplo de valor: "12094,00");
- Texto (str) com formato de data "YYYY-MM-DD" (exemplo de valor: "2012-08-27").

**Exemplos:** `bb_2019_2019_05_07_pf_pf`, `bb_2019_2019_05_07_pj_pf`


### `cabecalho_na_primeira_linha_1`
A primeira coluna tem um nome descritivo (ex.: "Município"), mas as demais colunas são `unnamed`. A primeira **linha** contém nomes adequados para as colunas, e os dados começam na segunda linha.

**Exemplos:** `caixa_002_2016_bext_31122016`, `caixa_001_2017_contratação_por_uf_nov_2017`,`aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2`,
`caixa_002_2016_08_agosto_bases_relatório_executivo_31082016`,
`caixa_002_2016_bases_relatório_executivo_31122016`,
`caixa_002_2017_bases_relat_rio_executivo_abr2017`,`caixa_002_2017_bases_relat_rio_executivo_ago2017`

### `cabecalho_na_primeira_linha_2`
A primeira coluna tem um nome descritivo (ex.: "desligamento__plano_piloto_pmcmv__contratações"), mas as demais colunas são `unnamed`. A primeira **linha** contém nomes adequados para as colunas, e os dados começam na segunda linha. Além disso, possui várias linhas vazias antes da última linha, que possui agregação total dos dados da tabela com dois valores numéricos (primeiro e int e segundo parece ser valor monetário).

**Exemplos:** `bb_2011_02_fevereiro_dados_22022011`

### `cabecalho_na_segunda_linha`
Primeira coluna pode ter nome descritivo (ex.: "relatório_executivo__pmcmv__ministério_das_cidades"), demais colunas "unnamed".

Primeira linha contém texto "Posicao:" mais data no formato "DD/MM/YYYY", indicando a data do relatório.

Segunda linha tem os nomes adequados para as colunas.

Terceira linha em diante possui os dados.

Ao final do arquivo, pode conter uma linha com a palavra "total"

**Exemplos:** `caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11`,`caixa_001_2011_11_novembro_relatório_executivo_mcid_21_11_11`,

### `cabecalho_na_terceira_linha_1` 
Todas as colunas são `unnamed`. As duas primeiras linhas são vazias (ou têm poucos
valores não nulos) e a terceira linha contém nomes adequados. Dados começam na quarta linha. 
 
**Exemplo:** `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2` 

### `cabecalho_na_terceira_linha_2`
Todas as colunas são `unnamed`. Primeiras linha vazia, segunda linha possui nome descritivo como "Nº unidades	Faixa Porcent." e a terceira linha contém nomes adequados. 
Primeira coluna "Público-alvo" (str) possui dados sobre faixa do PMCMV (ex.: "Faixa 2"), 
Segunda coluna "Concluída" (int) tem informações de obras concluidas, 
Próximas colunas (int): "100% a 75%", "75% a 50%", "50% a 25%", "25% a 10%" contém quantidade de empreendimentos por porcentagem de conclusão da obra
Última coluna "Total geral" (int) contém total de obras por faixa do PMCMV.
 
**Exemplo:** `bb_2011_08_agosto_balanço_23_08_2011_min__planejamento`


### `cabecalho_composto_1`
Tabela com cabeçalho mais as 3 primeiras linhas para gerar nomes das colunas, provavelmente provenientes de tabelas excel com células mescladas.

Possui dados do PMCMV por agregados por UF, região e nacional sobre quantidade de empreendimentos contratados, concluídos e entregues.

Cabeçalho contém um nome descritivo da tabela(ex.: "tabela_1__distribuição_pmcmvfar_por_uf" ou as palavras "relatório_01") e outras colunas são "unnamed". 

Colunas iniciais estão vazias.

Dados iniciam na quarta linha, e tem a estrutura: 
- colunas iniciais vazias;
- primeira coluna não vazia contém texto (str) que indica região ou UF;
- demais colunas são numéricas (int), aparentemente com informações sobre quantidade de empreendimentos e unidades habitacionais (UH), contratadas, concluídas e entregues.
- Anti-penúltima linha contém agregação dos dados no Brasil.
- Duas últimas linhas não contém dados estruturados em formato de tabela; 
- Penúltima linha indica fonte dos dados (banco), órgão que elaborou relatório, data em que relatório foi gerado. 
- Última linha contém observações.

**Exemplos:** `001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo`, `a_001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_mode`,


### `cabecalho_composto_2`

Tabela com cabeçalho mais as 2 primeiras linhas para gerar nomes das colunas, provavelmente provenientes de tabelas excel com células mescladas.

**Exemplos:** `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd`,`bb_2011_08_agosto_relatorio_min__cidades_16ago11`, `bb_2011_08_agosto_relatorio_min__cidades_30ago11`,`bb_2011_09_setembro_relatorio_min__cidades_13set11_2`,`bb_2011_10_outubro_relatorio_min__cidades_04out11`,`bb_2011_10_outubro_relatorio_min__cidades_11out11`,`bb_2011_10_outubro_relatorio_min__cidades_18out11`,`bb_2011_10_outubro_relatorio_min__cidades_25out11`,`bb_2012_01_janeiro_relatorio_min__cidades_17jan12`,`bb_2012_01_janeiro_relatorio_min__cidades_24jan12`,`bb_2012_01_janeiro_relatorio_min__cidades_31_01_2012`,`bb_2012_01_janeiro_relatorio_min_cidades_03_jan_12`,`bb_2012_02_fevereiro_relatorio_min__cidades_06_02_2012`


### `sub_tabelas_1`
Contém diversas "sub-tabelas" em sua estrutura. Essas "sub-tabelas" são separadas por diversas linha vazias.

Parece conter somente agregações de dados dos empreendimentos, e parece não conter códigos de apf ou empreendimento que possam ser utilizados para identificar histórico das construções, porém pode ser útil para comparar agregações.

As colunas de uma tabela desse tipo possuem o primeiro nome "unnamed_0" e as outras colunas no formato timestamp "YYYYMMDD_hhmmss" (por exemplo "20090401_000000"), sendo que este é o cabeçalho da primeira sub-tabela presente nesta tabela (cabeçalho neste formato é forte indício de tabela deste com tipo "sub-tabelas tipo 1").

Estrutura das sub-tabelas:
Primeira linha após linhas vazias contém a primeira coluna vazia ou "", e outras colunas que se parecem com timestamp, como data e hora no formato "YYYY-MM-DD hh:mm:ss". 
Após o cabeçalho da sub-tabela e antes de outras linhas vázias há linhas com dados.
A primeira coluna da sub-tabela, não nomeada, contém um texto representando categoria. Essa coluna que pode ser uma frente de financiamento do PMCMV (como "FGTS" ou "FAR"), uma faixa do PMCMV (exemplos: "0 a 3 SM", onde SM deve significar "Salários Mínimos" ou "SUAT A" ) ou uma acregação no período (quando há o texto "Total"), UF (como "AC"), região do Brasil(como "Norte") ou "Total Nacional".
As outras colunas da sub-tabela , cujo cabeçalho é nomeado com o timestamp, possuem valores numéricos, e provalvemente representam quantidade de unidades habitacionais (UH) contratadas, em construção e entregues no período representado pela timestamp que nomeia a coluna da sub-tabela.

Há sub-tabelas que agrupam UFs de uma mesma região, sub-tabela com agregação por região do Brasil e sub-tabela com agregação nacional.

**Indícios para identificação deste tipo:** 
- Cabeçalho da tabela com primeira coluna nomeada "unnamed_0" e as outras colunas no formato timestamp "YYYYMMDD_hhmmss" é forte indício de tabela deste tipo;
- Estrutura de sub-tabelas separadas por algumas linhas vazias.

**Exemplos:** `_001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópi`,`_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__2013`,`001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012`,`001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópia`,`1_2012_04_abril_2012_04_18_pmcmv_relatorio_executivo_18_04_2012`,`001_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_201`,`01_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_2012`,`01_2012_05_maio_pmcmv_relatorio_executivo_31_05_2012__version_1`,`01_2012_06_junho_pmcmv_relatorio_executivo_15_06_2012_corrigido`,`001_2012_10_outubro_20121009_pmcmv_relatório_executivo_0910201`,`01_2012_10_outubro_20121009_pmcmv_relatório_executivo_09102012`,`001_2014_12_dezembro_rel_executivo_resumo_31122014_reprocessado`,`a_001_2013_01___janeiro_pmcmv_relatório_executivo__31__01__201`,`aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_17_02_2012`,`aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_27_02_2012`,`aixa_001_2012_02_fevereiro_pmcmv_relatorio_executivo_29_02_2012`,`aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_1502201`,`aixa_001_2013_02___fevereiro_pmcmv_relatório_executivo_2802201`,`aixa_001_2015_02_fevereiro_pmcmv_3_relatório_executivo_2802201`,`aixa_001_2015_11_novembro_pmcmv_3_relatório_executivo_30112015`,
`_001_2015_01_janeiro_rel_executivo_resumo_31012015_reprocessado`,


### `sub_tabelas_2`
Cabeçalho com todas as colunas da tabelas nomeadas "unnamed_" + caractere numérico e estrutura de sub-tabela, com agrupamento de linhas com dados separadas por linhas vazias.

Parece conter somente agregações de dados dos empreendimentos, e parece não conter códigos de apf ou empreendimento que possam ser utilizados para identificar histórico das construções, porém pode ser útil para comparar agregações.

No caso do  primeiro exemplo abaixo, primeira linha com dados contém texto "1 milhão de moradias entregues e 2 milhões de contratadas", indicando um marco de entrega. Pode haver outras tabelas do mesmo tipo que não possuam esta linha.

Primeira sub-tabela contém cabeçalho escrito "SÍNTESE" e uma linha abaixo com timestamp no formato "YYYY-MM-DD hh:mm:ss", que provavelmente indica as entregas até a data indicada.

Segunda Sub-tabela contém as colunas: "Faixa" (texto que representa faixa de renda do PMCMV como "Faixa 1"), "UH Contratadas" (int), "UH Entregues" (int), "% Contratadas" (float), "% Entregues" (float). Última linha desta subtabela contém o total agregado.

Terceira sub-tabela contém cabeçalho com os seguinte cabeçalho: "Renda", "UH Entregues", "%" e nas linhas subsequentes a primeira coluna é texto indicando faixa de renda do PMCMV (exemplo de valor: "Até R$ 1.600,00"), segunda coluna contém um número (int) com quantidade de unidades habitacionais entregues (exemplo de valor: 542266) e última linha contem número (float com duas casas decimais) representando porcentagem de UH entregues. Última linha desta subtabela contém o total agregado.

Quarta sub-tabela contém as colunas: "Região" (texto representando região do Brasil, como "Norte"), "UH Contratadas" (int), "% Contratação" (float),"UH Entregues" (int),  "% Entregas" (float). Última linha desta subtabela contém o total agregado.

Quinta e última sub-tabela contém cabeçalho escrito "Quadro de Valores do MCMV" e três linhas subsequentes. Primeira linha: texto "Investimento Total" e valor (float); Segunda linha: texto "Desembolsado" e valor (float); terceira linha: texto "Subsídio" e valor float.

**Indícios para identificação deste tipo:** 
- Todas as colunas da tabelas nomeadas "unnamed_" + caractere numérico e estrutura de sub-tabela e sub-tabelas com os cabeçalhos indicados acima.

**Exemplos:** `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas`


### `sub_tabelas_3`

Possui sub-tabelas separadas por uma linha vazia. Cabeçalho inclui um nome descritivo como "relatório_01_total_das_contratações_por_ufregião_geográfic" e outras colunas se chamam "unnamed".

Primeiras colunas são vazias.

Primeira sub-tabela contém cabeçalho composto, incluindo nome das colunas da tabela e duas primeiras linhas. Dados por UF e região (primeira coluna com dados) e outras colunas numéricas sobre quantidade de unidades habitacionais, finaciamento de pessoa fisica contratada, etc.

Segunda sub-tabela parece uma agregação da primeira sub-tabela, com valores de "UH TOTAL" (int) em sua primeira linha, "VALOR TOTAL" (float) na segunda linha, e números (float) na terceira e quarta linha

Terceira sub-tabela como a primeira sub-tabela deste tipo, possui cabeçalho composto pelas 3 primeiras linhas da sub-tabela. Primeira linha contém as palavras "Relatório 2:". Primeira coluna com dados se refere à UF ou total por UF, segunda coluna com dados se refere a faixa do programa MCMV e demais colunas são numéricas com quantidades de UH aprovadas e contratadas.

**Indícios para identificação deste tipo:** 
- Todas as colunas da tabelas nomeadas "unnamed_" + caractere numérico, exceto uma que possui nome descritivo;
- Subtabelas são separadas por somente uma linha vazia

**Exemplos:** `bb_2011_01_janeiro_rel_11jan2011`, `bb_2011_01_janeiro_rel_25jan2011`, `bb_2011_02_fevereiro_relatório_mcmv_bb_01_02_2011`, `bb_2011_02_fevereiro_relatório_mcmv_bb_08_02_2011`, `bb_2011_02_fevereiro_relatório_mcmv_bb_16_02_2011`,`bb_2011_03_março_relatorio_min_cidades___22mar11`,`bb_2011_04_abril_relatorio_min_cidades___18abr11`,`bb_2011_04_abril_relatorio_min_cidades___26abr11`,`bb_2011_05_maio_relatorio_min_cidades___03mai11`,`bb_2011_05_maio_relatorio_min_cidades___10mai11`,`bb_2011_05_maio_relatorio_min_cidades___17mai11`,`bb_2011_05_maio_relatorio_min_cidades___24mai11`, `bb_2011_06_junho_relatorio_min_cidades___07jun11`, `bb_2011_07_julho_relatorio_min__cidades_19jul11`,`bb_2011_07_julho_relatorio_min_cidades___12jul11`

### `sub_tabelas_4`
Possui sub-tabelas em formato diferente das anteriores. Todas colunas da tabela são "unnamed". Todas as sub-tabelas são separadas por uma única linha vazia e possuem cabeçalhos compostos.

Primeira sub-tabela tem cabeçalho composto por 5 linhas.

Segunda sub-tabela tem cabeçalho composto por 4 linhas.

Terceira sub-tabela tem cabeçalho composto por 4 linhas.

Última linha contem observação sobre unidades habitacionais entregues e frente do PMCMV.

**Exemplo:** `caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12`


### `separador_|`
Tabela com nomes das colunas adequados, porém linhas possuem dados separados pelo caractere "|".

**Exemplos**: `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras`,`bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos`,`bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj`,`bb_2013_06_junho_pmcmv_18062013_tab_proponentes`,`bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas`,`

### `vazia`
Tabela sem dados. Detectada pelo tamanho do arquivo (< 5KB) e número de colunas
(0 ou 1).

**Exemplos:** `bb_2015_03_marco_cgu_of_6263_tab_emp_20150831`, `caixa_001_2016_bext_31102016`

### `dados_sem_utilidade`
Tabelas sem dados de interesse para a análise, identificadas manualmente pelo nome
do arquivo. Excluídas do processamento.

**Exemplos:** `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados`, `bb_2015_08_agosto_loginfesta`,`caixa_002_2018_novo_relat_rio_executivo_maio2018`,`caixa_002_2018_novo_relat_rio_executivo`

---

## Outras Classificações

### Possui APF, código de empreendimento, ou código de identificação
Verificar quais tabelas possuem colunas que podem identificar os empreendimentos. Verificar se coluna tem nome com "apf" ou "empreendimento" e/ou possui registros numéricos que poder utilizados como chaves.
