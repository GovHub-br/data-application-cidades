# Relatório de Batimento: Dump Histórico × SFTP

## 1. Tabelas Relacionadas

### Hash Exato (Confiança Alta)

- **1299** pares com hash estrutural idêntico
- Score médio: 1.00

| Método | Confiança | Qtd. Pares | Score Médio |
|--------|-----------|------------|-------------|
| Hash Exato | Alta | 1299 | 1.00 |

| SFTP | Dump | Score |
|------|------|-------|
| gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un | 024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 1.00 |
| gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un | 202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 1.00 |
| gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un | 202406_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 1.00 |
| gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un | 202407_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 1.00 |
| gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un | 202408_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 1.00 |

### Similaridade de Colunas (Jaccard) (Confiança Baixa)

- **123** pares identificados por similaridade de colunas (Jaccard ≥ 0,5)
- Score médio: 0.66

| Método | Confiança | Qtd. Pares | Score Médio |
|--------|-----------|------------|-------------|
| Similaridade de Colunas (Jaccard) | Baixa | 123 | 0.66 |

| SFTP | Dump | Score |
|------|------|-------|
| gefus_anteriores_202310_int054_ministeriocidades_far_bb_e | _1_2018_int054_ministeriocidades_far_bb_empreendimentos_2018... | 0.50 |
| gefus_anteriores_202311_int054_ministeriocidades_far_bb_e | _1_2018_int054_ministeriocidades_far_bb_empreendimentos_2018... | 0.50 |
| gefus_anteriores_202312_caixa_int040_ministeriocidades_fa | _018_int040_ministeriocidades_far_caixa_empreendimentos_2018... | 0.70 |
| gefus_anteriores_202406_snh_pmcmv_dados_prioritarios_af_ca_0... | historico_recente_202406_snh_pmcmv_dados_prioritarios_af_cai... | 0.79 |
| gefus_anteriores_202407_snh_pmcmv_dados_prioritarios_af_ca_0... | historico_recente_202407_snh_pmcmv_dados_prioritarios_af_cai... | 0.81 |

### Stem Canônico (Confiança Media)

- **88** pares com stem canônico compatível e Jaccard ≥ 0,3
- Score médio: 0.71

| Método | Confiança | Qtd. Pares | Score Médio |
|--------|-----------|------------|-------------|
| Stem Canônico | Media | 88 | 0.71 |

| SFTP | Dump | Score |
|------|------|-------|
| _202406_snh_pmcmv_dados_prioritarios_af_caixa | historico_recente_202406_snh_pmcmv_dados_prioritarios_af_cai... | 0.74 |
| _202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas | o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entre... | 0.42 |
| _202407_snh_pmcmv_dados_prioritarios_af_caixa | historico_recente_202407_snh_pmcmv_dados_prioritarios_af_cai... | 0.76 |
| _202409_snh_pmcmv_dados_prioritarios_af_caixa | historico_recente_202407_snh_pmcmv_dados_prioritarios_af_cai... | 0.76 |
| _202409_snh_pmcmv_dados_prioritarios_af_caixa_entregas | o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entre... | 0.42 |

### Totais Gerais

- **Total de pares:** 1510
- **Tabelas SFTP relacionadas:** 273
- **Tabelas dump relacionadas:** 54

## 2. Campos em Comum

### Top 20 Campos Mais Frequentes

| Campo | Qtd. Pares | Match Exato | Match Normalizado |
|-------|-----------|-------------|-------------------|
| agente_financeiro | 140 | 558 | 0 |
| apf | 140 | 558 | 0 |
| data_de_movimento | 140 | 558 | 0 |
| cnpj_proponente | 94 | 94 | 0 |
| dt_movimento | 94 | 94 | 0 |
| dt_ultima_liberacao_recurso | 94 | 94 | 0 |
| dt_ultima_entrega | 94 | 94 | 0 |
| dt_primeira_entrega | 94 | 94 | 0 |
| cod_agente_financeiro | 94 | 94 | 0 |
| origem | 94 | 94 | 0 |
| no_empreendimento | 94 | 94 | 0 |
| nu_apf | 94 | 94 | 0 |
| razao_social_proponente | 94 | 94 | 0 |
| percentual_obra_realizado | 94 | 94 | 0 |
| qt_unidades_entregues | 94 | 94 | 0 |
| qt_unidades_concluidas | 94 | 94 | 0 |
| qt_unidades_ociosas | 94 | 94 | 0 |
| situacao_retomada | 94 | 94 | 0 |
| vr_investimento | 94 | 94 | 0 |
| nome_empreendimento | 89 | 89 | 0 |

### Campos por Família de Tabela

**_202406_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| classificacao_dos_paralisados | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ✅ |
| nome_empreendimento | text | text | ✅ |

*... e mais 8 campos*

**_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202407_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| classificacao_dos_paralisados | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ✅ |

*... e mais 10 campos*

**_202407_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202409_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| classificacao_dos_paralisados | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ✅ |

*... e mais 10 campos*

**_202409_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202410_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | text | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ✅ |

*... e mais 17 campos*

**_202411_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202411_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202412_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202412_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202501_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202501_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202503_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202503_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202504_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202504_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202505_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202506_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202506_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202507_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202507_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202509_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202509_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202509_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202510_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202510_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202511_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202511_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202511_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202511_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202512_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202512_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202601_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202601_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202601_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202602_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202602_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202602_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202602_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202603_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202603_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202603_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202603_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202604_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202604_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202604_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202604_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**_202605_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| bairro_do_imovel | text | text | ✅ |
| cep_do_imovel | text | text | ✅ |
| codigo_ibge_do_municipio | text | text | ✅ |
| complemento_do_endereco_do_imovel | text | text | ✅ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ✅ |
| data_de_contratacao | text | timestamp without time zone | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ✅ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ✅ |
| logradouro_do_imovel | text | text | ✅ |
| longitude_do_imovel | text | text | ✅ |
| modalidade | text | text | ✅ |

*... e mais 24 campos*

**_202605_snh_pmcmv_dados_prioritarios_af_caixa_entregas**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | text | bigint | ✅ |

**_202605_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_ass_doc | text | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | text | bigint | ✅ |

**gefus_202601_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_202601_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202601_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202601_snh_pmcmv_dados_prioritarios_af_caixa_entrega**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_202602_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_202602_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202602_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202602_snh_pmcmv_dados_prioritarios_af_caixa_entrega**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_202603_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_202603_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202603_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202603_snh_pmcmv_dados_prioritarios_af_caixa_entrega**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_202604_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_202604_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202604_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202604_snh_pmcmv_dados_prioritarios_af_caixa_entrega**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_202605_snh_pmcmv_dados_prioritarios_af_caixa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_202605_snh_pmcmv_dados_prioritarios_af_caixa_entrega**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202310_int054_ministeriocidades_far_bb_e**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | text | text | ✅ |
| cod_agente_financeiro | text | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | text | text | ✅ |
| nu_empreendimento | text | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_202311_int054_ministeriocidades_far_bb_e**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | text | text | ✅ |
| cod_agente_financeiro | text | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | text | text | ✅ |
| nu_empreendimento | text | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_202312_caixa_int040_ministeriocidades_fa**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | text | text | ✅ |
| cod_agente_financeiro | text | text | ✅ |
| cod_municipio_ibge | text | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | text | text | ✅ |
| cod_tipo_contrato | text | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_202406_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |
| quantidade_de_uhs_contratadas_em_janeiro_do_ano_de_referencia | bigint | text | ⚠️ |
| quantidade_de_uhs_distratadas | bigint | text | ✅ |

*... e mais 17 campos*

**gefus_anteriores_202406_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202406_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| classificacao_dos_paralisados | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 8 campos*

**gefus_anteriores_202407_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202407_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| classificacao_dos_paralisados | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |

*... e mais 10 campos*

**gefus_anteriores_202408_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| check | text | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202408_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| classificacao_dos_paralisados | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |

*... e mais 10 campos*

**gefus_anteriores_202409_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202409_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| classificacao_dos_paralisados | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |

*... e mais 10 campos*

**gefus_anteriores_202410_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202410_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| dt_previsao_entrega | text | timestamp without time zone | ✅ |
| exec | text | text | ✅ |
| mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_28_qtd_entregues_ano_dt_referencia | bigint | bigint | ✅ |
| mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc | text | timestamp without time zone | ✅ |
| mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia | text | double precision | ✅ |
| mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |

*... e mais 17 campos*

**gefus_anteriores_202411_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 21 campos*

**gefus_anteriores_202411_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202411_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202412_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 21 campos*

**gefus_anteriores_202412_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202412_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_2024_07_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_2024_07_snh_pmcmv_dados_prioritarios_af_b_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_2024_08_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_2024_09_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_2024_09_snh_pmcmv_dados_prioritarios_af_b_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_2024_10_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |
| obra_nao_iniciada | text | text | ⚠️ |
| quantidade_de_uhs_contratadas_do_ano_de_referencia | bigint | text | ⚠️ |

*... e mais 19 campos*

**gefus_anteriores_202501_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202501_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202502_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202502_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202503_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202503_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202504_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202504_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202504_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202504_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202505_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202505_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202505_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202505_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202506_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202506_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202506_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202506_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202507_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202507_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202507_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202507_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202507_snh_pmcmv_dados_prioritarios_da_en**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| dt_ass_doc | timestamp without time zone | timestamp without time zone | ✅ |
| numero_de_unidades_entregues | bigint | bigint | ⚠️ |

**gefus_anteriores_202508_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202508_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202508_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202508_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | double precision | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202509_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202509_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202509_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202509_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | double precision | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202510_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202510_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202510_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202510_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | double precision | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202511_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202511_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202511_snh_pmcmv_dados_prioritarios_af_bb_0002**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202511_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202511_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | double precision | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202512_snh_pmcmv_dados_prioritarios_af_bb**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | text | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202512_snh_pmcmv_dados_prioritarios_af_bb_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | double precision | text | ⚠️ |
| cep_do_imovel | double precision | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | double precision | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_202512_snh_pmcmv_dados_prioritarios_af_ca**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| dt_entrega | text | timestamp without time zone | ✅ |
| qt_uh_entregues | bigint | bigint | ✅ |

**gefus_anteriores_202512_snh_pmcmv_dados_prioritarios_af_ca_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| bairro_do_imovel | text | text | ⚠️ |
| cep_do_imovel | bigint | text | ⚠️ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| complemento_do_endereco_do_imovel | text | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| logradouro_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |

*... e mais 25 campos*

**gefus_anteriores_2025_01_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 21 campos*

**gefus_anteriores_2025_02_snh_pmcmv_dados_prioritarios_af_b**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_contratacao | timestamp without time zone | timestamp without time zone | ⚠️ |
| data_de_movimento | timestamp without time zone | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | double precision | text | ✅ |
| latitude_do_imovel | double precision | text | ⚠️ |
| longitude_do_imovel | double precision | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 21 campos*

**gefus_anteriores_2025_02_snh_pmcmv_dados_prioritarios_af_b_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| agente_financeiro | text | text | ✅ |
| apf | bigint | text | ✅ |
| codigo_ibge_do_municipio | bigint | text | ⚠️ |
| data_da_previsao_da_entrega | text | timestamp without time zone | ⚠️ |
| data_de_contratacao | text | timestamp without time zone | ⚠️ |
| data_de_movimento | text | timestamp without time zone | ✅ |
| detalhamento_da_situacao_do_empreendimento | text | text | ⚠️ |
| detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano | text | text | ⚠️ |
| exec | text | text | ✅ |
| latitude_do_imovel | text | text | ⚠️ |
| longitude_do_imovel | text | text | ⚠️ |
| modalidade | text | text | ✅ |
| municipio | text | text | ⚠️ |
| nao_se_aplica | text | text | ⚠️ |
| nome_empreendimento | text | text | ✅ |

*... e mais 21 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empre_0048**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0002**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0003**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0004**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0005**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0006**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0007**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0008**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0009**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0010**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0011**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0012**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0013**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0014**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | double precision | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0015**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | double precision | text | ✅ |
| cod_agente_financeiro | double precision | text | ✅ |
| cod_municipio_ibge | double precision | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | double precision | text | ✅ |
| cod_tipo_contrato | double precision | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0016**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0017**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0018**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0019**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0020**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0021**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0022**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0023**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0024**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0025**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0026**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0027**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0028**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0029**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0030**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0031**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0032**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0033**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0034**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0035**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0036**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0037**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0038**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0039**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0040**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0041**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0042**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0043**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0044**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0045**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0046**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int040_ministeriocidades_far_caixa_empree_0047**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| cod_municipio_ibge | bigint | text | ✅ |
| cod_pendencia_obra | text | text | ✅ |
| cod_regime_execucao | text | text | ✅ |
| cod_situacao_contrato_pj | bigint | text | ✅ |
| cod_tipo_contrato | bigint | text | ✅ |
| desc_situacao_contrato | text | text | ✅ |
| dsc_tipologia | text | text | ✅ |
| dt_assinatura | text | timestamp without time zone | ✅ |
| dt_inicio_obra | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_termino_obra | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |

*... e mais 24 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0001**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | double precision | timestamp without time zone | ✅ |
| dt_ultima_entrega | double precision | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0002**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0003**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0004**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0005**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0006**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0007**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0008**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0009**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0010**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0011**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0012**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0013**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0014**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0015**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0016**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0017**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0018**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0019**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0020**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0021**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | bigint | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0022**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0023**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0024**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0025**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0026**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0027**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0028**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0029**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0030**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0031**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0032**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0033**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0034**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0035**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0036**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0037**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0038**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0039**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0040**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0041**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | text | text | ✅ |
| a_liberar_terreno | text | text | ✅ |
| a_liberar_ts | text | text | ✅ |
| cnpj_proponente | bigint | text | ✅ |
| cod_agente_financeiro | bigint | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | bigint | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

**gefus_anteriores_int054_ministeriocidades_far_bb_empreendi_0042**

| Campo | Tipo SFTP | Tipo Dump | Match |
|-------|-----------|-----------|-------|
| a_liberar_obra | double precision | text | ✅ |
| a_liberar_terreno | double precision | text | ✅ |
| a_liberar_ts | double precision | text | ✅ |
| cnpj_proponente | double precision | text | ✅ |
| cod_agente_financeiro | double precision | text | ✅ |
| dt_contratacao | text | timestamp without time zone | ✅ |
| dt_movimento | text | timestamp without time zone | ✅ |
| dt_primeira_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_entrega | text | timestamp without time zone | ✅ |
| dt_ultima_liberacao_recurso | text | timestamp without time zone | ✅ |
| no_empreendimento | text | text | ✅ |
| no_municipio | text | text | ✅ |
| nu_apf | double precision | text | ✅ |
| nu_empreendimento | double precision | text | ✅ |
| origem | text | text | ✅ |

*... e mais 12 campos*

## 3. Chaves de Cruzamento Candidatas

| Chave | Padrão | Tabelas SFTP | Tabelas Dump | Status |
|-------|--------|-------------|-------------|--------|
| nu_apf | `\bnu_apf\b` | 94 | 2 | ✅ validada duplamente (estrutural + conteúdo APF) |
| municipio | `\bmunicipio\b` | 89 | 7 | ⚠️ requer validação |

### Validação por Conteúdo (APF)

O cruzamento por conteúdo identificou **1282 pares** de alta confiança (sobreposição de APF ≥ 90%), distribuídos por **2 famílias** de tabelas. A chave `apf` (ou `nu_apf`) aparece em ambos os schemas com alta correspondência de valores, recebendo **validação dupla** — confirmação estrutural (coluna presente) e de conteúdo (valores coincidentes).

Isso torna `apf` a chave de cruzamento mais confiável para operações de JOIN entre os schemas SFTP e dados_historicos.

### Exemplos de Tabelas por Chave

- **nu_apf**: gefus_anteriores_202310_int054_ministeriocidades_far_bb_e; gefus_anteriores_202311_int054_ministeriocidades_far_bb_e; gefus_anteriores_202312_caixa_int040_ministeriocidades_fa
- **municipio**: _202406_snh_pmcmv_dados_prioritarios_af_caixa; _202407_snh_pmcmv_dados_prioritarios_af_caixa; _202409_snh_pmcmv_dados_prioritarios_af_caixa

## 4. Divergências

### Tabelas Exclusivas do SFTP

Total: **1735 tabelas** sem correspondência no dump histórico.

**gefus_anteriores_*** — 20 exemplos

- `_032020_validacoes_pmcmv_mar_2020` — Anos: [2020]
- `_042020_tab_validacao_arquivos_pf` — Anos: [2020]
- `_042020_tab_validacao_arquivos_pj` — Anos: [2020]
- `_202504_andamento_obra_af_caixa_v2` — Anos: [2025]
- `_ingest_log` — Anos: N/A
- `_ingest_minio_log` — Anos: N/A
- `analise_snh_032020_tab_validacao_arquivos_pf_tab_validaca` — Anos: [2020]
- `analise_snh_032020_tab_validacao_arquivos_pj_tab_validacao` — Anos: [2020]
- `analise_snh_032020_validacoes_pmcmv_mar_2020` — Anos: [2020]
- `analise_snh_042020_tab_validacao_arquivos_pf_042020_tab_va` — Anos: [2020]
- `analise_snh_042020_tab_validacao_arquivos_pj_042020_tab_va` — Anos: [2020]
- `base_andamento_obra_m20251218` — Anos: [2025]
- `base_pj_fgts_200106` — Anos: [2001]
- `base_pj_fgts_20200305` — Anos: [2020]
- `base_pj_fgts_20201105` — Anos: [2020]
- `base_pj_fgts_20201210` — Anos: [2020]
- `base_pj_fgts_20210109` — Anos: [2021]
- `base_pj_fgts_20210119` — Anos: [2021]
- `base_pj_fgts_20210209` — Anos: [2021]
- `base_pj_fgts_20210609` — Anos: [2021]

### Tabelas Exclusivas do Dump Histórico

Total: **356 tabelas** sem correspondência no SFTP.

**bb_*** — 20 exemplos

- `_001_2011_08_agosto_previs_o_de_conclus_o_e_entrega_far_modelo` — Anos: [2011]
- `_001_2012_01_janeiro_pmcmv_relatorio_executivo_31012012_base_bd` — Anos: [2012]
- `_001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_201` — Anos: [2012]
- `_001_2012_04_abril_2012_04_18_5c_base_contrata__o_pf_18042012` — Anos: [2012]
- `_001_2012_10_outubro_20121009_bases_relat_rio_executivo_0910201` — Anos: [2012]
- `_001_2012_10_outubro_20121009_bases_relat_rio_executivo_1610201` — Anos: [2012]
- `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas` — Anos: [2012]
- `_011_12_dezembro_pmcmv_relatorio_executivo_31122011_base___c_pi` — Anos: [2011]
- `_202406_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202407_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202408_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202409_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202411_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202412_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2024]
- `_202501_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]
- `_202503_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]
- `_202504_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]
- `_202505_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]
- `_202506_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]
- `_202507_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b` — Anos: [2025]

### Conexões por Conteúdo (APF)

O matching por conteúdo (sobreposição de APF) identificou **1248 pares** de alta confiança que **não foram detectados pelo matching estrutural**. Essas conexões representam tabelas com estruturas diferentes (nomes de coluna divergentes ou tipos distintos) mas que compartilham os mesmos contratos APF.


**snh_pmcmv_dados_prioritarios** — 1009 pares

| Tabela SFTP | Tabela Dump | Overlap APF | Overlap % |
|------------|-------------|-------------|-----------|
| _202406_snh_pmcmv_dados_prioritarios_af_caixa | historico_recente_202407_snh_pmcmv_dados_prioritar... | 199 | 99.5% |
| _202406_snh_pmcmv_dados_prioritarios_af_caixa | o_recente_202406_snh_pmcmv_dados_prioritarios_af_c... | 191 | 95.5% |
| _202406_snh_pmcmv_dados_prioritarios_af_caixa | o_recente_202407_snh_pmcmv_dados_prioritarios_af_c... | 182 | 91.0% |
| _202406_snh_pmcmv_dados_prioritarios_af_caixa | o_recente_202408_snh_pmcmv_dados_prioritarios_af_c... | 182 | 91.0% |
| _202406_snh_pmcmv_dados_prioritarios_af_caixa_entr... | historico_recente_202406_snh_pmcmv_dados_prioritar... | 191 | 95.5% |

*... e mais 1004 pares*

**snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_b** — 239 pares

| Tabela SFTP | Tabela Dump | Overlap APF | Overlap % |
|------------|-------------|-------------|-----------|
| _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | _202406_snh_pmcmv_dados_prioritarios_da_entrega_da... | 10 | 100.0% |
| _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | 10 | 100.0% |
| _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | _202408_snh_pmcmv_dados_prioritarios_da_entrega_da... | 10 | 100.0% |
| _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | _202409_snh_pmcmv_dados_prioritarios_da_entrega_da... | 10 | 100.0% |
| _202407_snh_pmcmv_dados_prioritarios_da_entrega_da... | _202411_snh_pmcmv_dados_prioritarios_da_entrega_da... | 10 | 100.0% |

*... e mais 234 pares*

### Consistência Temporal

Foram analisadas **1142 tabelas** quanto à consistência entre a data inferida do nome do arquivo e a data real dos dados (coluna ``data_de_movimento``).

| Status | Quantidade | Percentual |
|--------|-----------|------------|
| ✅ Ok | 397 | 34.8% |
| ⚠️ Divergente | 6 | 0.5% |
| ❓ Indeterminado | 739 | 64.7% |

#### Top 5 Discrepâncias

| Tabela | Data (Arquivo) | Data (Dados) | Diferença (dias) |
|--------|---------------|-------------|------------------|
| 202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af... | 2024-02-01 | 2025-02-28 | 393.0 |
| bb_2013_03_marco_entrada_bb_20130415.csv | 2013-03-01 | 2013-04-15 | 45.0 |
| bb_2013_10_outubro_entrada_bb_20131105.csv | 2013-10-01 | 2013-11-05 | 35.0 |
| bb_2013_11_novembro_entrada_bb_20131204.csv | 2013-11-01 | 2013-12-04 | 33.0 |
| bb_2014_02_fevereiro_entrada_bb_20140306.csv | 2014-02-01 | 2014-03-06 | 33.0 |

### Distribuição Temporal

| Ano | Tabelas SFTP | Tabelas Dump | Sobreposição |
|-----|-------------|-------------|--------------|
| 2001 | 1 | 0 |  |
| 2011 | 0 | 2 |  |
| 2012 | 0 | 6 |  |
| 2018 | 0 | 2 |  |
| 2020 | 11 | 0 |  |
| 2021 | 4 | 0 |  |
| 2023 | 3 | 0 |  |
| 2024 | 43 | 27 | ✅ |
| 2025 | 91 | 29 | ✅ |
| 2026 | 47 | 6 | ✅ |

Zona de sobreposição: 2024–2026 (3 anos com tabelas em ambos os schemas).

## 5. Recomendações de Uso Conjunto

> ⚠️ **Nota:** As recomendações abaixo são preliminares e baseiam-se apenas na análise estrutural. A validação de conteúdo (Fase 2) ainda não foi executada.

### Evidências

- **Correspondência por hash exato:** 1299 pares de tabelas com estrutura idêntica. Isso indica que parte das tabelas do SFTP é continuação temporal direta do dump histórico (mesma definição de colunas, tipos e constraints).

- **Correspondência por similaridade:** 211 pares adicionais foram identificados por stem canônico ou Jaccard de colunas, revelando relações semânticas entre tabelas com nomes diferentes.

- **Tabelas exclusivas:** 1735 tabelas exclusivas do SFTP e 356 tabelas exclusivas do dump não possuem correspondência no outro schema.

### Veredito

**O SFTP complementa e parcialmente substitui o dump histórico.**

As tabelas com hash exato idêntico indicam continuidade temporal — o SFTP carrega dados mais recentes com a mesma estrutura. Porém, a presença de tabelas exclusivas em ambos os schemas mostra que nenhum dos dois é completo isoladamente: o dump histórico contém dados de períodos anteriores não cobertos pelo SFTP, e o SFTP introduz tabelas e períodos que não existem no dump.

### Recomendações por Família de Dados

- **bb_***: 0 tabelas SFTP, 20 tabelas dump. Disponível apenas no dump histórico — usar como fonte primária para esta família.
- **gefus_anteriores_***: 20 tabelas SFTP, 0 tabelas dump. Disponível apenas no SFTP — usar como fonte primária para esta família.

### Exemplos de JOIN

Com base no cruzamento por conteúdo, a chave **`apf`** é a mais confiável para JOIN entre os schemas. Exemplo de consulta:

```sql
SELECT
    sftp.*,
    hist.municipio,
    hist.agente_financeiro,
    hist.data_de_movimento
FROM sftp.snh_pmcmv_dados_prioritarios_af_caixa sftp
INNER JOIN dados_historicos_formatados.historico_recente_snh_pmcmv_dados_prioritarios_af_caixa hist
    ON sftp.apf = hist.apf
WHERE sftp.apf IS NOT NULL
  AND hist.apf IS NOT NULL;
```

Para cruzar com o schema ``dados_historicos`` (não formatado), a sintaxe é similar, mas os nomes de tabela podem conter períodos que exigem escaping com aspas duplas:

```sql
SELECT *
FROM sftp."202406_snh_pmcmv_dados_prioritarios_af_caixa" sftp
INNER JOIN dados_historicos."024_10_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb" hist
    ON sftp.apf = hist.apf
WHERE sftp.apf IS NOT NULL
  AND hist.apf IS NOT NULL;
```

### Limitações Conhecidas

- A análise estrutural foi **complementada por validação de conteúdo** (cruzamento por APF). Os resultados de conteúdo estão refletidos nas seções 3 e 4.

- A nomenclatura de tabelas e colunas difere significativamente entre os dois schemas, o que pode levar a **falsos negativos** no matching (pares que existem mas não foram identificados).

- A **camada 3** (Jaccard de colunas) usa um limiar de 0,5, o que pode tanto incluir falsos positivos quanto excluir pares válidos com baixa similaridade nominal.

- O **cruzamento por conteúdo (APF)** só é aplicável a tabelas que possuem a coluna ``apf`` (ou ``nu_apf``). Tabelas sem essa coluna não foram validadas por conteúdo.

- A **consistência temporal** identificou **discrepâncias** em 6 tabelas. Recomenda-se verificar as datas antes de realizar JOINs baseados em período — a data do nome do arquivo pode não corresponder à data real dos dados.

- Tabelas sem colunas ou com estrutura atípica podem não ter sido corretamente classificadas.

