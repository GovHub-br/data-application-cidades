{#
    De-para dos dados históricos: família -> colunas da silver.

    Cada coluna da silver é {nome, origens, tipo}: `origens` são as colunas do cleansed que
    guardam o mesmo dado (em ordem de preferência; o layout de cada época tem uma delas) e
    `tipo` vem de `dhist_tipo()`. As equivalências entre nomes são as provadas no estudo de
    colunas (notebooks/dados_historicos/04_estudo_colunas.ipynb): cruzando duas épocas ou
    duas fontes pela chave, os valores coincidem linha a linha. Coluna que não tem
    equivalente fica com um nome no padrão abaixo. Toda coluna do cleansed da família tem de aparecer
    aqui ou em `descartar` (o teste dhist_de_para_cobertura confere).

    Nomes (spec 007; os testes dhist_prata_nomes_padrao e dhist_prata_apf_pelo_valor conferem):
    um conceito tem um nome só em todas as famílias. Prefixo pelo tipo: `dt_` (ou `mes_`)
    para data, `valor_`, `quantidade_`, `percentual_`, `codigo_`, `nome_` e `ic_`; os
    prefixos das fontes (`dat_`, `dte_`, `vlr_`, `qtd_`, `cod_`, `bln_`, `txt_`, `num_`, os
    húngaros da BEXT) não entram. Chaves e território: `apf` (e `apf_vinculado`),
    `codigo_empreendimento_bb`, `codigo_contrato_pf`, `uf` (sigla; por extenso, `nome_uf`),
    `municipio`, `codigo_ibge_municipio`, `cnpj_proponente`, `agente_financeiro`. Coluna
    toda mascarada na fonte (`***`) vai para `descartar`.

    Ressalvas que valem para o consumo (não mudam a coluna):
    - relatorio_executivo_caixa.quantidade_uh é MOVIMENTO com sinal (+N contratação, -N
      distrato); o estoque do APF é a soma (gold);
    - relatorio_executivo_caixa.valor_subsidio_ogu: de 2015-04 para 2015-05 só 38% dos
      valores do mesmo APF se repetem (cálculo ou conteúdo mudou; a confirmar);
    - snh_af_caixa: as colunas "em janeiro do ano de referência" mudam de conteúdo de
      2024-10 para 2024-11.

    Famílias do BB (spec 005, Etapa 3):
    - int054_far_bb: INT054 do FAR (BB), foto de 2018-08. É a ponte código BB → APF
      (prata_dhist_ponte_bb_apf). Ressalva: percentual_obra_realizado vem truncado em 0/1 da
      fonte (,00 / 1,00 já na analytics); para percentual de obra do BB, preferir a
      entrega_bb_pj (research R2).
    - entrada_bb: Entrada BB, 2012-10 a 2014-09. Ressalva: de 2013-05 para 2013-06 oito
      colunas de contrato e unidades mudam de conteúdo com o layout (notebook 04, seção 4).
      no_contrato_caixa e 20110906_000000 são sempre vazias.
    - entrada_bb_aux_far: Entrada BB auxiliar do FAR (arquivo único de 2013-12).
    - entrega_bb_pj: Entrega BB PJ, 2014-10 a 2019-04. dsc_endereco, nom_proponente,
      num_cep, nat_proponente e qtd_beneficiario_indicado chegam mascarados (***) da
      staging. dat_contratacao e dat_contratacao_tecnico_social são a mesma coluna renomeada
      (notebook 04, seção 3). vlr_total_liberado muda de conteúdo em 2015-10, 2015-12 e
      2016-07 (seção 4). dat_prevista_25/50/75 são sempre vazias.
    - snh_af_bb: Painel SNH dos empreendimentos do BB, 2024-06 a 2026-03; mesmos nomes do
      snh_af_caixa. uhs_* = uh_* (renomeação de 2025-02 para 2025-03, notebook 04, seção 3).
      nao_se_aplica e obra_nao_iniciada são colunas da fonte com valores de situação (64 e
      40 linhas).
    - snh_entregas_af_bb: Entregas por evento do painel SNH (BB), 2024-06 a 2026-03.
    - pnhr_bb: PNHR (rural, BB), 2014-10 a 2018-07. Renomeações provadas no notebook 04,
      seção 3. A escala do percentual varia por arquivo; o de 2015-09 mistura as duas e é
      resolvido pelo vizinho (2015-10) em prata_dhist_pnhr_bb (research R2). endereco é
      sempre vazia.

    Contratação PF (spec 005, Etapa 4; dado pessoal só mascarado):
    - bext: contrato de pessoa física (icodigo_empreendimento e cod_empreendimento são nomes
      enganosos, notebook 04 §2), 2012-05 a 2018-08. O layout de 2012 (218 linhas) usa os
      nomes do Relatório Cidades; as equivalências com o layout novo são as do notebook 04
      (§2 bext × Relatório Cidades e §3 bext 2012 → 2015).
      inst/pmcmv/origem/faixa/porte/produto/entregues do layout de 2012 ficam separados dos
      v*/c*/i* do novo por falta de prova linha a linha.
    - relatorio_cidades_caixa: Relatório Cidades (2016-02): contrato PF; cod_empreendimento
      é o contrato (notebook 04 §2). Mesmos nomes canônicos da bext.
    - relatorio_cidades_entregas_caixa: entregas do Relatório Cidades (2016-02) por contrato
      PF; o anomes daqui é outro mês, não o de referência do registro (notebook 04).
    - entrega_bb_pf_pf: contratos PF do BB (2014-10 a 2019-04); cod_contrato =
      icodigo_empreendimento da bext e cod_municipio = icodigo_municipio_ibge_sem_dv
      (notebook 04 §2).
    - entrega_bb_pj_pf: contratos PF dentro de empreendimento PJ do BB (2014-10 a 2019-04).
      nr_prpt é o código do empreendimento BB (medido em 16/11: dos 922 nr_prpt que existem
      na entrega_bb_pj, 911 têm o mesmo município), então o contrato chega ao APF pela
      ponte.
    - tab_contratos_pf_fgts_bb: contratos PF FGTS do BB, retrato de 2013-06. cod_operacao é
      o código do empreendimento BB (dos 151 que existem na entrega_bb_pj, 149 têm o mesmo
      município). qtd_parc_vencidas é sempre vazia.
    - tab_beneficiarios_fgts_bb: beneficiários dos contratos PF FGTS do BB, retrato de
      2013-06. cod_cpf vem em hash; nome, nascimento, endereço, titularidade e cor/raça vêm
      como *** da staging e passam como texto, sem transformar.

    Demais famílias (spec 005, Etapa 5):
    - base_pf_pj_caixa: bases PF/PJ da Caixa (2012-04 a 2012-08), mesmo layout e nomes do
      relatorio_executivo_caixa. data_prevista_termino_obra = cronograma_datatermino
      (notebook 04 §3). de_obra_executada muda de conteúdo de 2012-04 para 2012-08 (§4).
    - empreendimentos_contratados_caixa: empreendimentos contratados da Caixa (2012-04 e
      2012-05), nomes do relatorio_executivo_caixa. comercializados fica com o próprio nome
      (variante sem prova).
    - demanda_far_caixa: demanda do FAR na Caixa por APF (2011-08). ibge_codigo tem 12
      dígitos e fica como texto.
    - rural_caixa: contratação rural da Caixa por APF (2012-04).
    - tab_operacoes_bb: operações do BB, retrato de 2013-06. cod_operacao é o código do
      empreendimento BB (dos 407 que existem na entrega_bb_pj, 406 têm o mesmo município);
      cod_empreendimento é outro código (agrupa etapas) e fica com o nome. dte_apres_projeto
      vem como serial do Excel. dte_prev_contratacao é sempre vazia.
    - tab_empreendimentos_bb: empreendimentos do BB, retrato de 2013-06 (cod_empreendimento
      agrupa as etapas de tab_operacoes_bb).
    - tab_contratos_pj_bb: contratos PJ do BB, retrato de 2013-06; cod_operacao como em
      tab_operacoes_bb. dte_assin_contrat é o nome truncado (63 caracteres) de
      dte_assin_contrato.
    - tab_proponentes_bb: proponentes do BB, retrato de 2013-06. txt_ra é o nome truncado
      (63 caracteres), provavelmente txt_razao_social.
    - tab_andamento_obras_bb: andamento das obras do BB, retrato de 2013-06. sem_nome_10 a
      _17: o nome se perdeu no dump (cabeçalho cortado em 63 caracteres pelo PostgreSQL
      antigo, research R4); conteúdo aparente: datas (10, 11, 12, 16, 17), 0/100 (13, 14) e
      valor em R$ (15). sem_nome_05 a _09 são sempre vazias.
    - tab_unidades_concluidas_bb: unidades concluídas do BB, retrato de 2013-06.
      dte_primeira_en é o nome truncado de dte_primeira_entrega. sem_nome_05 a _07: nome
      perdido no dump; conteúdo aparente: data (05) e quantidades (06, 07). dte_legalizacao
      é sempre vazia.
    - tab_caracterizacoes_entornos_bb: caracterização do entorno dos empreendimentos do BB,
      retrato de 2013-06. sem_nome_05 a _16: nome perdido no dump; conteúdo aparente:
      indicadores T/F de infraestrutura.
    - contratacao_caixa_2010: contratação da Caixa por empreendimento (2010-06), sem APF.
    - pmcmv_parte2_caixa: empreendimentos enquadráveis da Caixa (2009-12), sem APF.
    - propostas_recebidas_caixa: propostas recebidas pela Caixa por empreendimento
      (2012-04), sem APF.
    - dados_bb_2011: 5 empreendimentos do Plano Piloto do BB (2011-02).
      faixa_de_renda_3_a_6_sm_ou_6_a_10_sm é sempre vazia.
    - relatorio_executivo_municipio_caixa: relatório executivo da Caixa agregado por
      município, faixa e produto (2011-11); conferência da gold.
    - contratacao_uf_caixa: contratação por UF, faixa e instituição (2017-11); conferência
      da gold.
    - bext_agregado_caixa: BEXT agregada por UF, faixa e produto (2012-04); conferência da
      contratação PF. Nomes do layout de 2012 da bext.
    - contratacao_pf_caixa: contratação PF da Caixa por município, produto e renda
      (2012-04); conferência da contratação PF.
    - contratacao_pf_total_caixa: contratação PF total da Caixa por município, produto e
      faixa (2010-09).
    - presidencia_caixa: relatório da presidência da Caixa por município: UH, valores, metas
      e subsídio por faixa de renda (2011-06 a 2011-08). nome é o nome do município.
    - desligados_caixa: desligamentos da Caixa por empreendimento (2010-09), sem APF.
    - previsao_entrega_far_caixa: previsão de conclusão e entrega do FAR por UF (2011-08);
      as colunas *_uh da previsão têm texto e ficam como texto.
    - relatorio_semanal_bb_uf: relatório semanal do BB ao MCid por UF (2011-01 a 2011-07):
      desligamentos do Plano Piloto, aprovações na Caixa e contratações; conferência.
    - relatorio_semanal_bb_publico: relatório semanal do BB ao MCid por UF e público-alvo
      (2011-01 a 2012-10); conferência. contratacao_total_quantidade_unidades é sempre
      vazia.
    - propostas_far_caixa: seleção do FAR por município (2013-02): déficit urbano 2010,
      diretriz do MCid, contratação na Caixa (MCMV2) e no BB e UH de propostas em análise;
      conferência e variável de demanda.
    - propostas_bb: financiamento PF do BB por UF e faixa (2013-02), layout do relatório
      semanal; conferência. Cinco colunas são sempre vazias.
#}
{% macro de_para_dhist() %}
    {{
        return(
            {
                "relatorio_executivo_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["cod_apf", "codapf"], "tipo": "texto"},
                        {
                            "nome": "id_registro_caixa",
                            "origens": ["iregistro", "idregistro"],
                            "tipo": "texto",
                        },
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_munic_ibge", "codmunicibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["vnome_empreendimento", "nomeempreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_proponente",
                            "origens": ["vnome_construtora", "construtora"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["inumero_cnpj", "cnpj"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["endereco_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["instituicao"],
                            "tipo": "texto",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "faixa_divisao", "origens": ["faixa_divisao"], "tipo": "texto"},
                        {
                            "nome": "faixa_percentual",
                            "origens": ["vfaixa_percentual", "faixa_perc"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "recortes_territoriais",
                            "origens": ["recortes_territoriais"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "forma_parcelamento_solo",
                            "origens": ["forma_parcelamento_solo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipologia_unidade",
                            "origens": ["tipologia_unidade"],
                            "tipo": "texto",
                        },
                        {"nome": "cmcmv", "origens": ["cmcmv"], "tipo": "texto"},
                        {"nome": "id_mcmv", "origens": ["id_mcmv"], "tipo": "texto"},
                        {"nome": "contratado", "origens": ["contratado"], "tipo": "texto"},
                        {
                            "nome": "proposta_em_negociacao",
                            "origens": ["proposta_em_negociacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["uh", "unidades"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["uh_concluidas", "unidades_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_em_obras",
                            "origens": ["uh_em_obras", "unidades_em_obras"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["iqde_unidades_entregues", "unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_comercializadas",
                            "origens": ["uh_comercializadas", "comercializadas"],
                            "tipo": "inteiro",
                        },
                        {"nome": "valor", "origens": ["valor"], "tipo": "valor"},
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": [
                                "valor_contrapartida_poder_publico",
                                "valor_da_contrapartida_do_poder_publico",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["valor_total_do_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["subsidio_fgts", "siaci_valorsubsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["subsidio_ogu", "siaci_valorsubsidio_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_total_liberado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_gestao_condominial",
                            "origens": ["valor_gestao_condominial"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "administracao_por_condominio",
                            "origens": ["administracao_por_condominio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "valor_trabalho_social",
                            "origens": ["valor_trabalho_social_tts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "trabalho_tecnico_social_tts",
                            "origens": ["trabalho_tecnico_social_tts"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_contratacao", "dataprevistasr"],
                            "tipo": "data",
                        },
                        {
                            "nome": "ano_contratacao",
                            "origens": ["ano_contratacao", "ano"],
                            "tipo": "ano",
                        },
                        {
                            "nome": "dt_termino_obra_cronograma",
                            "origens": ["data_termino", "cronograma_datatermino"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_termino_obra",
                            "origens": [
                                "data_prevista_termino_obras",
                                "data_prevista_termino_obra",
                            ],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_termino_obras",
                            "origens": ["data_termino_obras"],
                            "tipo": "data",
                        },
                        {
                            "nome": "mes_conclusao",
                            "origens": ["data_conclusao"],
                            "tipo": "ano_mes",
                        },
                        {"nome": "dt_vistoria", "origens": ["data_vistoria"], "tipo": "data"},
                        {
                            "nome": "dt_primeira_assinatura",
                            "origens": ["primeira_assinatura"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": [
                                "pct_obra_executada",
                                "obra_executada",
                                "de_obra_executada",
                            ],
                            "tipo": "percentual",
                        },
                        {
                            "nome": "situacao_obra",
                            "origens": ["situacao_de_obra"],
                            "tipo": "situacao",
                        },
                        {
                            "nome": "latitude",
                            "origens": ["latitude"],
                            "tipo": "latitude_br",
                            "par": "longitude",
                            "uf": "uf",
                        },
                        {
                            "nome": "longitude",
                            "origens": ["longitude"],
                            "tipo": "longitude_br",
                            "par": "latitude",
                        },
                    ],
                    "descartar": [],
                },
                "datas_entregas_pj_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["icodigo_apf"], "tipo": "texto"},
                        {"nome": "dt_entrega", "origens": ["data_entregas"], "tipo": "data"},
                    ],
                    "descartar": [],
                },
                "int040_far_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["nu_apf"], "tipo": "texto"},
                        {"nome": "dt_movimento", "origens": ["dt_movimento"], "tipo": "data"},
                        {"nome": "origem", "origens": ["origem"], "tipo": "texto"},
                        {
                            "nome": "codigo_agente_financeiro",
                            "origens": ["cod_agente_financeiro"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_municipio_ibge"],
                            "tipo": "ibge",
                        },
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["no_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_proponente",
                            "origens": ["razao_social_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cnpj_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipologia_unidade",
                            "origens": ["dsc_tipologia"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipo_comunidade_atendida",
                            "origens": ["dsc_tipo_comunidade_atendida"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "portaria_selecao",
                            "origens": ["portaria_selecao"],
                            "tipo": "texto",
                        },
                        {"nome": "tipo_aporte", "origens": ["tipo_aporte"], "tipo": "texto"},
                        {"nome": "apf_vinculado", "origens": ["vinculacoes"], "tipo": "texto"},
                        {
                            "nome": "codigo_tipo_contrato",
                            "origens": ["cod_tipo_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_tipo_contrato",
                            "origens": ["nome_tipo_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_contrato_pj",
                            "origens": ["cod_situacao_contrato_pj"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desc_situacao_contrato",
                            "origens": ["desc_situacao_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_regime_execucao",
                            "origens": ["cod_regime_execucao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_tipo_etapa",
                            "origens": ["cod_tipo_etapa"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_pendencia_obra",
                            "origens": ["cod_pendencia_obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_pendencia_entrega",
                            "origens": ["cod_pendencia_entrega"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_motivo_ociosidade",
                            "origens": ["cod_motivo_ociosidade"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "situacao_retomada",
                            "origens": ["situacao_retomada"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "situacao_obra",
                            "origens": ["situacao_obra_gefus"],
                            "tipo": "situacao",
                        },
                        {"nome": "dt_contratacao", "origens": ["dt_assinatura"], "tipo": "data"},
                        {"nome": "dt_inicio_obra", "origens": ["dt_inicio_obra"], "tipo": "data"},
                        {
                            "nome": "dt_termino_obra",
                            "origens": ["dt_termino_obra"],
                            "tipo": "data",
                        },
                        {"nome": "dt_legalizacao", "origens": ["dt_legalizacao"], "tipo": "data"},
                        {
                            "nome": "dt_primeira_entrega",
                            "origens": ["dt_primeira_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_ultima_entrega",
                            "origens": ["dt_ultima_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_ultima_liberacao_recurso",
                            "origens": ["dt_ultima_liberacao_recurso"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["percentual_obra_realizado"],
                            "tipo": "percentual",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["qt_unidade_financiadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["qt_unidades_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["qt_unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_ociosas",
                            "origens": ["qt_unidades_ociosas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_operacao",
                            "origens": ["vlr_operacao"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["vr_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["vr_liberado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_1",
                            "origens": ["vr_contrapartida_1"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "liberacao_obra",
                            "origens": ["liberacao_obra"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "liberacao_terreno",
                            "origens": ["liberacao_terreno"],
                            "tipo": "valor",
                        },
                        {"nome": "liberacao_ts", "origens": ["liberacao_ts"], "tipo": "valor"},
                        {
                            "nome": "liberacao_ts_adicional",
                            "origens": ["liberacao_ts_adicional"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "snh_af_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["apf"], "tipo": "texto"},
                        {
                            "nome": "dt_movimento",
                            "origens": ["data_de_movimento"],
                            "tipo": "data",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {"nome": "modalidade", "origens": ["modalidade"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_ibge_do_municipio"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "observacoes", "origens": ["observacoes"], "tipo": "texto"},
                        {
                            "nome": "situacao_obra",
                            "origens": ["situacao_do_empreendimento"],
                            "tipo": "situacao",
                        },
                        {
                            "nome": "detalhamento_situacao",
                            "origens": [
                                "detalhamento_da_situacao_do_empreendimento",
                                "classificacao_dos_paralisados",
                            ],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_de_contratacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_entrega",
                            "origens": ["data_da_previsao_da_entrega", "dt_previsao_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["exec"],
                            "tipo": "percentual",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["uh_contratadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["uh_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_vigentes",
                            "origens": ["uh_vigentes"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_previsao_entrega",
                            "origens": ["qt_uh_previsao_entrega"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_a_entregar",
                            "origens": ["unidades_habitacionais_a_serem_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_contratado",
                            "origens": ["valor_contratado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_desembolsado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_aporte_adicional",
                            "origens": ["valor_aporte_adicional"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "situacao_janeiro_ano_ref",
                            "origens": [
                                "situacao_do_empreendimento_em_janeiro_do_ano_de_referencia",
                                "mcmv_ogu_29_txt_situacao_empreendimento_janeiro_ano_dt_referenc",
                            ],
                            "tipo": "texto",
                        },
                        {
                            "nome": "detalhamento_situacao_janeiro_ano_ref",
                            "origens": [
                                "detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano",
                                "mcmv_ogu_31_txt_detalha_situacao_empreendimento_janeiro_ano_dt",
                            ],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_vigentes_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_vigentes_em_janeiro_do_ano_de_referencia",
                                "mcmv_ogu_27_qtd_vigentes_janeiro_ano_dt_referencia",
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_entregues_do_ano_de_referencia",
                                "mcmv_ogu_28_qtd_entregues_ano_dt_referencia",
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_desembolsado_ano_ref",
                            "origens": [
                                "valor_desembolsado_do_ano_de_referencia",
                                "mcmv_ogu_30_vlr_desembolsado_no_ano_dt_referencia",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "quantidade_uh_contratadas_ano_ref",
                            "origens": ["quantidade_de_uhs_contratadas_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_contratadas_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_contratadas_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_entregues_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_vigentes_ano_ref",
                            "origens": ["quantidade_de_uhs_vigentes_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas",
                            "origens": ["quantidade_de_uhs_distratadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas_ano_ref",
                            "origens": ["quantidade_de_uhs_distratadas_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_distratadas_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "logradouro_imovel",
                            "origens": ["logradouro_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "numero_imovel",
                            "origens": ["numero_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "complemento_imovel",
                            "origens": ["complemento_do_endereco_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "bairro_imovel",
                            "origens": ["bairro_do_imovel"],
                            "tipo": "texto",
                        },
                        {"nome": "cep_imovel", "origens": ["cep_do_imovel"], "tipo": "texto"},
                        {
                            "nome": "latitude_imovel",
                            "origens": ["latitude_do_imovel"],
                            "tipo": "latitude_br",
                            "par": "longitude_imovel",
                            "uf": "uf",
                        },
                        {
                            "nome": "longitude_imovel",
                            "origens": ["longitude_do_imovel"],
                            "tipo": "longitude_br",
                            "par": "latitude_imovel",
                        },
                    ],
                    "descartar": [],
                },
                "snh_entregas_af_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["apf"], "tipo": "texto"},
                        {
                            "nome": "dt_movimento",
                            "origens": ["data_de_movimento"],
                            "tipo": "data",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {"nome": "dt_entrega", "origens": ["dt_entrega"], "tipo": "data"},
                        {
                            "nome": "quantidade_uh_entregues_evento",
                            "origens": ["qt_uh_entregues"],
                            "tipo": "inteiro",
                        },
                        {"nome": "check", "origens": ["check"], "tipo": "texto"},
                    ],
                    "descartar": [],
                },
                "int054_far_bb": {
                    "colunas": [
                        {"nome": "apf", "origens": ["nu_apf"], "tipo": "texto"},
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["nu_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "dt_movimento", "origens": ["dt_movimento"], "tipo": "data"},
                        {"nome": "origem", "origens": ["origem"], "tipo": "texto"},
                        {
                            "nome": "codigo_agente_financeiro",
                            "origens": ["cod_agente_financeiro"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_municipio_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["no_municipio"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["sg_uf"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["no_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_proponente",
                            "origens": ["razao_social_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cnpj_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "situacao_obra",
                            "origens": ["situacao_obra"],
                            "tipo": "situacao",
                        },
                        {
                            "nome": "situacao_retomada",
                            "origens": ["situacao_retomada"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipologia_unidade",
                            "origens": ["dsc_tipologia"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipo_comunidade_atendida",
                            "origens": ["dsc_tipo_comunidade_atendida"],
                            "tipo": "texto",
                        },
                        {"nome": "tipo_aporte", "origens": ["tipo_aporte"], "tipo": "texto"},
                        {"nome": "apf_vinculado", "origens": ["vinculacoes"], "tipo": "texto"},
                        {
                            "nome": "portaria_selecao",
                            "origens": ["portaria_selecao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "co_tipo_objetivo_complemento",
                            "origens": ["co_tipo_objetivo_complemento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_tipo_contrato",
                            "origens": ["nome_tipo_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_contrato_pj",
                            "origens": ["cod_situacao_contrato_pj"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desc_situacao_contrato",
                            "origens": ["desc_situacao_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_regime_execucao",
                            "origens": ["cod_regime_execucao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_tipo_etapa",
                            "origens": ["cod_tipo_etapa"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_pendencia_obra",
                            "origens": ["cod_pendencia_obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_pendencia_entrega",
                            "origens": ["cod_pendencia_entrega"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_motivo_ociosidade",
                            "origens": ["cod_motivo_ociosidade"],
                            "tipo": "texto",
                        },
                        {"nome": "dt_contratacao", "origens": ["dt_contratacao"], "tipo": "data"},
                        {"nome": "dt_inicio_obra", "origens": ["dt_inicio_obra"], "tipo": "data"},
                        {
                            "nome": "dt_termino_obra",
                            "origens": ["dt_termino_obra"],
                            "tipo": "data",
                        },
                        {"nome": "dt_legalizacao", "origens": ["dt_legalizacao"], "tipo": "data"},
                        {
                            "nome": "dt_primeira_entrega",
                            "origens": ["dt_primeira_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_ultima_entrega",
                            "origens": ["dt_ultima_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_ultima_liberacao_recurso",
                            "origens": ["dt_ultima_liberacao_recurso"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["percentual_obra_realizado"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["qt_unidades_habitacionais"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["qt_unidades_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["qt_unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_ociosas",
                            "origens": ["qt_unidades_ociosas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["vr_emprestimo_far"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["vr_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["total_liberado_far"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_1",
                            "origens": ["vr_contrapartida_1"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "a_liberar_obra",
                            "origens": ["a_liberar_obra"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "a_liberar_terreno",
                            "origens": ["a_liberar_terreno"],
                            "tipo": "valor",
                        },
                        {"nome": "a_liberar_ts", "origens": ["a_liberar_ts"], "tipo": "valor"},
                        {
                            "nome": "liberacao_ts_adicional",
                            "origens": ["liberacao_ts_adicional"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "entrada_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["codigo_empreendimento_bb"],
                            "tipo": "texto",
                        },
                        {"nome": "fase_pmcmv", "origens": ["fase_do_pmcmv"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "nome_proponente", "origens": ["empresa"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_do_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "publico_alvo", "origens": ["publico_alvo"], "tipo": "texto"},
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["endereco_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "contrato_bb", "origens": ["contrato_bb"], "tipo": "texto"},
                        {
                            "nome": "contrato_caixa",
                            "origens": ["contrato_caixa"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_da_contratacao_bb"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_entrega",
                            "origens": ["entrega_do_empreendimento"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["percentual_de_obra"],
                            "tipo": "percentual_auto",
                        },
                        {"nome": "quantidade_uh", "origens": ["qde_unidades"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_financiadas_pf",
                            "origens": ["unid_fin_pf"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_disponiveis",
                            "origens": ["unid_disponiveis"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_global_de_venda_vgv",
                            "origens": ["valor_global_de_venda_vgv"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "total_financiamentos_pf",
                            "origens": ["total_financiamentos_pf"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "total_financ_a_producao_pj",
                            "origens": ["total_financ_a_producao_pj"],
                            "tipo": "valor",
                        },
                        {"nome": "desconto", "origens": ["desconto"], "tipo": "valor"},
                        {
                            "nome": "complemento_fgts",
                            "origens": ["complemento_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "complemento_ogu",
                            "origens": ["complemento_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "complemento_total",
                            "origens": ["complemento_total"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "equilibrio_fgts",
                            "origens": ["equilibrio_fgts"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "equilibrio_ogu",
                            "origens": ["equilibrio_ogu"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "equilibrio_total",
                            "origens": ["equilibrio_total"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "valor_fgts_pf_utilizado",
                            "origens": ["valor_fgts_pf_utilizado"],
                            "tipo": "valor_calculado",
                        },
                    ],
                    "descartar": ["no_contrato_caixa", "20110906_000000"],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "entrada_bb_aux_far": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["codigo_do_empreendimento", "numero_da_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "natureza_da_operacao",
                            "origens": ["natureza_da_operacao"],
                            "tipo": "texto",
                        },
                        {"nome": "subprograma", "origens": ["subprograma"], "tipo": "texto"},
                        {
                            "nome": "faixa_de_renda",
                            "origens": ["faixa_de_renda"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_apresentacao",
                            "origens": ["data_apresentacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_da_etapa_ou_modulo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["endereco_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_do_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "valor_operacao",
                            "origens": ["valor_da_operacao"],
                            "tipo": "valor_calculado",
                        },
                        {"nome": "quantidade_uh", "origens": ["no_unidades"], "tipo": "inteiro"},
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_assinatura"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["percentual_de_obra"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "liberacoes_acumuladas",
                            "origens": ["liberacoes_acumuladas"],
                            "tipo": "valor",
                        },
                        {"nome": "nome_proponente", "origens": ["construtora"], "tipo": "texto"},
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cnpj_construtora"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_adaptadas",
                            "origens": ["unidades_adaptadas"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "entrega_bb_pj": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_municipio"],
                            "tipo": "ibge",
                        },
                        {"nome": "codigo_faixa", "origens": ["num_faixa"], "tipo": "texto"},
                        {"nome": "modalidade", "origens": ["modalidade"], "tipo": "texto"},
                        {"nome": "codigo_programa", "origens": ["num_programa"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["dsc_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["dsc_endereco"],
                            "tipo": "texto",
                        },
                        {"nome": "cep", "origens": ["num_cep"], "tipo": "texto"},
                        {
                            "nome": "nome_proponente",
                            "origens": ["nom_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cod_cnpj_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nat_proponente",
                            "origens": ["nat_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_instituicao",
                            "origens": ["cnpj_instituicao"],
                            "tipo": "texto",
                        },
                        {"nome": "codigo_produto", "origens": ["idc_produto"], "tipo": "texto"},
                        {
                            "nome": "codigo_zoneamento_obra",
                            "origens": ["idc_zoneamento_obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "situacao_obra",
                            "origens": ["tx_est_obra_imra"],
                            "tipo": "situacao",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["prc_execucao_obra"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dat_contratacao", "dat_contratacao_tecnico_social"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_inicio_obra",
                            "origens": ["dat_prevista_inicio"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_termino_obra",
                            "origens": ["dat_prevista_termino"],
                            "tipo": "data",
                        },
                        {"nome": "dt_termino_obra", "origens": ["dat_conclusao"], "tipo": "data"},
                        {"nome": "dt_vistoria", "origens": ["dat_vistoria"], "tipo": "data"},
                        {"nome": "dt_entrega", "origens": ["dat_entregue"], "tipo": "data"},
                        {
                            "nome": "dt_atualizacao",
                            "origens": ["dat_atualizacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["qtd_unidade_habitacional"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["qtd_unidade_concluida"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["qtd_unidade_entregue"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_comercializadas",
                            "origens": ["qtd_comercializadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_adaptaveis",
                            "origens": ["qtd_unidade_adaptavel"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_kit_instalacao",
                            "origens": ["qtd_kit_instalacao"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_inadimplentes",
                            "origens": ["qtd_unidade_inadimplente"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["vlr_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": ["vlr_contrapartida"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["vlr_total_liberado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["vlr_total_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["vlr_subsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["vlr_subsidio_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "latitude",
                            "origens": ["vlr_latitude"],
                            "tipo": "latitude_br",
                            "par": "longitude",
                            "ibge": "codigo_ibge_municipio",
                        },
                        {
                            "nome": "longitude",
                            "origens": ["vlr_longitude"],
                            "tipo": "longitude_br",
                            "par": "latitude",
                        },
                    ],
                    "descartar": [
                        "dat_prevista_25",
                        "dat_prevista_50",
                        "dat_prevista_75",
                        "qtd_beneficiario_indicado",
                    ],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "snh_af_bb": {
                    "colunas": [
                        {"nome": "apf", "origens": ["apf"], "tipo": "texto"},
                        {
                            "nome": "dt_movimento",
                            "origens": ["data_de_movimento"],
                            "tipo": "data",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {"nome": "modalidade", "origens": ["modalidade"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_ibge_do_municipio"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "observacoes", "origens": ["observacoes"], "tipo": "texto"},
                        {
                            "nome": "situacao_obra",
                            "origens": ["situacao_do_empreendimento"],
                            "tipo": "situacao",
                        },
                        {
                            "nome": "detalhamento_situacao",
                            "origens": ["detalhamento_da_situacao_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "nao_se_aplica", "origens": ["nao_se_aplica"], "tipo": "texto"},
                        {
                            "nome": "obra_nao_iniciada",
                            "origens": ["obra_nao_iniciada"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_de_contratacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_entrega",
                            "origens": ["data_da_previsao_da_entrega"],
                            "tipo": "data",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["exec"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["uh_contratadas", "uhs_contratadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["uh_entregues", "uhs_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_vigentes",
                            "origens": ["uh_vigentes", "uhs_vigentes"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_a_entregar",
                            "origens": ["unidades_habitacionais_a_serem_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_contratado",
                            "origens": ["valor_contratado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_desembolsado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_aporte_adicional",
                            "origens": ["valor_aporte_adicional"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "situacao_janeiro_ano_ref",
                            "origens": [
                                "situacao_do_empreendimento_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "texto",
                        },
                        {
                            "nome": "detalhamento_situacao_janeiro_ano_ref",
                            "origens": [
                                "detalhamento_da_situacao_do_empreendimento_em_janeiro_do_ano"
                            ],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_vigentes_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_vigentes_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues_ano_ref",
                            "origens": ["quantidade_de_uhs_entregues_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_desembolsado_ano_ref",
                            "origens": ["valor_desembolsado_do_ano_de_referencia"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "quantidade_uh_contratadas_ano_ref",
                            "origens": ["quantidade_de_uhs_contratadas_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_contratadas_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_contratadas_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_entregues_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_vigentes_ano_ref",
                            "origens": ["quantidade_de_uhs_vigentes_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas",
                            "origens": ["quantidade_de_uhs_distratadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas_ano_ref",
                            "origens": ["quantidade_de_uhs_distratadas_do_ano_de_referencia"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_distratadas_janeiro_ano_ref",
                            "origens": [
                                "quantidade_de_uhs_distratadas_em_janeiro_do_ano_de_referencia"
                            ],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "logradouro_imovel",
                            "origens": ["logradouro_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "numero_imovel",
                            "origens": ["numero_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "complemento_imovel",
                            "origens": ["complemento_do_endereco_do_imovel"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "bairro_imovel",
                            "origens": ["bairro_do_imovel"],
                            "tipo": "texto",
                        },
                        {"nome": "cep_imovel", "origens": ["cep_do_imovel"], "tipo": "texto"},
                        {
                            "nome": "latitude_imovel",
                            "origens": ["latitude_do_imovel"],
                            "tipo": "latitude_br",
                            "par": "longitude_imovel",
                            "uf": "uf",
                        },
                        {
                            "nome": "longitude_imovel",
                            "origens": ["longitude_do_imovel"],
                            "tipo": "longitude_br",
                            "par": "latitude_imovel",
                        },
                    ],
                    "descartar": [],
                },
                "snh_entregas_af_bb": {
                    "colunas": [
                        {"nome": "apf", "origens": ["apf"], "tipo": "texto"},
                        {
                            "nome": "dt_movimento",
                            "origens": ["data_de_movimento"],
                            "tipo": "data",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {"nome": "dt_entrega", "origens": ["dt_ass_doc"], "tipo": "data"},
                        {
                            "nome": "quantidade_uh_entregues_evento",
                            "origens": ["numero_de_unidades_entregues"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "pnhr_bb": {
                    "colunas": [
                        {"nome": "apf", "origens": ["nr_prpt", "cod_eprd"], "tipo": "texto"},
                        {"nome": "natureza", "origens": ["natureza"], "tipo": "texto"},
                        {"nome": "subprograma", "origens": ["subprograma"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {
                            "nome": "agente_financeiro",
                            "origens": ["agente_financeiro"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_aclt_prpt_eprd",
                            "origens": ["dt_aclt_prpt_eprd"],
                            "tipo": "data_excel",
                        },
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nm_rsmd_eprd"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_entidade_organizadora",
                            "origens": ["nm_eo"],
                            "tipo": "texto",
                        },
                        {"nome": "cnpj_proponente", "origens": ["nr_cnpj"], "tipo": "texto"},
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["nm_mun"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["sg_uf", "uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cd_mun_ibge", "cod_mun_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "quantidade_uh", "origens": ["qt_und_eprd"], "tipo": "inteiro"},
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dt_ass_eft_ctr", "dt_ass"],
                            "tipo": "data",
                        },
                        {
                            "nome": "acompanhamento_obra",
                            "origens": ["acompanhmento_de_obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["pc_acm_obra_rlzd"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["vlr_emprestimo", "vlr_financ"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_custo_obra",
                            "origens": ["vlr_custo_obra", "vl_cst_obra"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_medio_uh",
                            "origens": ["vlr_medio_un"],
                            "tipo": "valor_calculado",
                        },
                    ],
                    "descartar": ["endereco"],
                },
                "bext": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf",
                            "origens": ["icodigo_empreendimento", "cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "uf", "origens": ["csigla_uf", "uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["icodigo_municipio_ibge_sem_dv", "codigo_ibge"],
                            "tipo": "ibge",
                        },
                        {
                            "nome": "municipio",
                            "origens": ["vnome_municipio", "nome_do_municipio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["mvalor_emprestimo", "valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["mvalor_investimento", "valor_do_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": [
                                "mvalor_contrapartida_poder_publico",
                                "contrapartida_poder_publico",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio",
                            "origens": ["mvalor_subsidio", "valor_subsidio"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_financiamento_pf",
                            "origens": ["mvalor_financiamento", "valor_financiamento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["mvalor_desembolso", "valor_desembolsado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "quantidade_uh",
                            "origens": ["iqde_uh", "uh"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_em_obras",
                            "origens": ["iqde_uh_em_obras", "uh_em_obras"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["iqde_uh_concluidas", "uh_concluidos"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "faixa_execucao_fisica",
                            "origens": ["vfaixa_perc_obra", "obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "mes_referencia_registro",
                            "origens": ["iano_mes", "anomes2"],
                            "tipo": "ano_mes",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["inst", "vinstituicao", "if_af"],
                            "tipo": "texto",
                        },
                        {"nome": "pmcmv", "origens": ["pmcmv"], "tipo": "texto"},
                        {"nome": "fase_mcmv", "origens": ["iprograma_mcmv"], "tipo": "texto"},
                        {
                            "nome": "origem",
                            "origens": ["origem", "csigla_origem"],
                            "tipo": "texto",
                        },
                        {"nome": "faixa", "origens": ["faixa", "cfaixa"], "tipo": "texto"},
                        {
                            "nome": "porte_municipio",
                            "origens": ["porte_de_municipio", "vporte_municipio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "produto",
                            "origens": ["produto", "vnome_produto"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["entregues", "iqde_uh_entregues"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "relatorio_cidades_caixa": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf",
                            "origens": ["cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "agente_financeiro", "origens": ["inst"], "tipo": "texto"},
                        {"nome": "pmcmv", "origens": ["pmcmv"], "tipo": "texto"},
                        {"nome": "origem", "origens": ["origem"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["nome_do_municipio"], "tipo": "texto"},
                        {
                            "nome": "porte_municipio",
                            "origens": ["porte_de_municipio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["valor_do_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": ["contrapartida_poder_publico"],
                            "tipo": "valor",
                        },
                        {"nome": "quantidade_uh", "origens": ["uh"], "tipo": "inteiro"},
                        {"nome": "faixa_execucao_fisica", "origens": ["obra"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_em_obras",
                            "origens": ["uh_em_obras"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["uh_concluidos"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "mes_referencia_registro",
                            "origens": ["anomes"],
                            "tipo": "ano_mes",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_desembolsado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio",
                            "origens": ["valor_subsidio"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_financiamento_pf",
                            "origens": ["valor_financiamento"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "relatorio_cidades_entregas_caixa": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf",
                            "origens": ["cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["entregues"],
                            "tipo": "inteiro",
                        },
                        {"nome": "mes_referencia", "origens": ["anomes"], "tipo": "ano_mes"},
                        {"nome": "dt_entrega", "origens": ["dt_entrega"], "tipo": "data"},
                    ],
                    "descartar": [],
                },
                "entrega_bb_pf_pf": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf",
                            "origens": ["cod_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_municipio"],
                            "tipo": "ibge",
                        },
                        {"nome": "codigo_programa", "origens": ["num_programa"], "tipo": "texto"},
                        {"nome": "codigo_faixa", "origens": ["num_faixa"], "tipo": "texto"},
                        {"nome": "fase_mcmv", "origens": ["fase_mcmv"], "tipo": "texto"},
                        {"nome": "quantidade_uh", "origens": ["qtd_uh"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["qtd_entregue"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["qtd_concluida"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_financiamento_pf",
                            "origens": ["vlr_financiamento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["vlr_subsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["vlr_subsidio_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_recurso_proprio",
                            "origens": ["vlr_recurso_proprio"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_fgts_sacado",
                            "origens": ["vlr_sacado_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dat_contratacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_atualizacao",
                            "origens": ["dat_atualizacao"],
                            "tipo": "data",
                        },
                    ],
                    "descartar": [],
                },
                "entrega_bb_pj_pf": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf_pj",
                            "origens": ["cod_contrato"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["nr_prpt"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_municipio"],
                            "tipo": "ibge",
                        },
                        {"nome": "codigo_programa", "origens": ["num_programa"], "tipo": "texto"},
                        {"nome": "codigo_faixa", "origens": ["num_faixa"], "tipo": "texto"},
                        {
                            "nome": "valor_financiamento_pf",
                            "origens": ["vlr_financiamento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_fgts_sacado",
                            "origens": ["vlr_sacado_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["vlr_subsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["vlr_subsidio_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dat_contratacao"],
                            "tipo": "data",
                        },
                        {
                            "nome": "valor_recurso_proprio",
                            "origens": ["vlr_recurso_proprio"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_operacao",
                            "origens": ["vlr_total_operacao"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "dt_atualizacao",
                            "origens": ["dat_atualizacao"],
                            "tipo": "data",
                        },
                    ],
                    "descartar": [],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_contratos_pf_fgts_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf_pj",
                            "origens": ["cod_contrato_alien_pf_fgts"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_mun_ibge"],
                            "tipo": "ibge",
                        },
                        {
                            "nome": "codigo_subprograma",
                            "origens": ["cod_subprograma"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_tipologia_uh",
                            "origens": ["cod_tipologia_und"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_grupo_modalidade_desconto_fgts",
                            "origens": ["cod_grupo_mod_desc_fgts"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_linha_aplicacao",
                            "origens": ["cod_linha_aplicacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_contrato_pf",
                            "origens": ["cod_sit_contrato_pf"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_agente_financeiro",
                            "origens": ["cod_cnpj_agente_finan"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_insercao_municipio",
                            "origens": ["cod_insercao_municipio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_faixa_renda",
                            "origens": ["cod_faixa_renda"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dte_assin_contrato"],
                            "tipo": "data",
                        },
                        {"nome": "dt_entrega", "origens": ["dte_entrega_und"], "tipo": "data"},
                        {"nome": "area_uh", "origens": ["num_area_und_hab"], "tipo": "valor"},
                        {
                            "nome": "valor_financiamento_pf",
                            "origens": ["vlr_financiamento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_complemento_fgts",
                            "origens": ["vlr_subsidio_comple_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_complemento_ogu",
                            "origens": ["vlr_subsidio_comple_ogu"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_equilibrio_fgts",
                            "origens": ["vlr_subsidio_equili_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_recurso_proprio_fgts",
                            "origens": ["vlr_recursos_proprios_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_recurso_proprio",
                            "origens": ["vlr_recursos_proprios"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_avaliacao_imovel",
                            "origens": ["vlr_avaliacao_imovel"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "ano_orcamento",
                            "origens": ["num_ano_orcamento"],
                            "tipo": "ano",
                        },
                        {
                            "nome": "prazo_financiamento_meses",
                            "origens": ["num_prazo_finan_meses"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_parcelas_pagas",
                            "origens": ["qtd_parc_pagas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "percentual_taxa_juros",
                            "origens": ["prc_taxa_juros"],
                            "tipo": "percentual",
                        },
                    ],
                    "descartar": ["qtd_parc_vencidas"],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_beneficiarios_fgts_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_contrato_pf_pj",
                            "origens": ["cod_contrato_alien_pf_fgts"],
                            "tipo": "texto",
                        },
                        {"nome": "cpf_mascarado", "origens": ["cod_cpf"], "tipo": "texto"},
                        {"nome": "codigo_sexo", "origens": ["cod_gen_sexo"], "tipo": "texto"},
                        {
                            "nome": "codigo_estado_civil",
                            "origens": ["cod_estado_civil"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_titularidade",
                            "origens": ["cod_titularidade"],
                            "tipo": "texto",
                        },
                        {"nome": "codigo_cadmut", "origens": ["num_cadmut"], "tipo": "texto"},
                        {
                            "nome": "valor_renda_familiar_mensal",
                            "origens": ["vlr_renda_fam_mensal"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [
                        "cod_cor_raca_beneficiario",
                        "dte_nascimento",
                        "txt_endereco_und_alien",
                        "txt_nome_benef",
                    ],
                },
                "base_pf_pj_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["codapf"], "tipo": "texto"},
                        {"nome": "id_registro_caixa", "origens": ["idregistro"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codmunicibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nomeempreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "nome_proponente", "origens": ["construtora"], "tipo": "texto"},
                        {"nome": "cnpj_proponente", "origens": ["cnpj"], "tipo": "texto"},
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["endereco_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {
                            "nome": "recortes_territoriais",
                            "origens": ["recortes_territoriais"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "forma_parcelamento_solo",
                            "origens": ["forma_parcelamento_solo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipologia_unidade",
                            "origens": ["tipologia_unidade"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "administracao_por_condominio",
                            "origens": ["administracao_por_condominio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "trabalho_tecnico_social_tts",
                            "origens": ["trabalho_tecnico_social_tts"],
                            "tipo": "texto",
                        },
                        {"nome": "quantidade_uh", "origens": ["unidades"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": ["valor_da_contrapartida_do_poder_publico"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["valor_total_do_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_total_liberado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["siaci_valorsubsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["siaci_valorsubsidio_ogu"],
                            "tipo": "valor",
                        },
                        {"nome": "dt_contratacao", "origens": ["dataprevistasr"], "tipo": "data"},
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["de_obra_executada"],
                            "tipo": "percentual",
                        },
                        {"nome": "faixa_divisao", "origens": ["faixa_divisao"], "tipo": "texto"},
                        {"nome": "faixa_percentual", "origens": ["faixa_perc"], "tipo": "texto"},
                        {"nome": "id_mcmv", "origens": ["id_mcmv"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["unidades_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_em_obras",
                            "origens": ["unidades_em_obras"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_comercializadas",
                            "origens": ["comercializadas"],
                            "tipo": "inteiro",
                        },
                        {"nome": "ano_contratacao", "origens": ["ano"], "tipo": "ano"},
                        {
                            "nome": "dt_termino_obra_cronograma",
                            "origens": ["cronograma_datatermino", "data_prevista_termino_obra"],
                            "tipo": "data",
                        },
                    ],
                    "descartar": [],
                },
                "empreendimentos_contratados_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["codapf"], "tipo": "texto"},
                        {"nome": "id_registro_caixa", "origens": ["idregistro"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codmunicibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nomeempreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "nome_proponente", "origens": ["construtora"], "tipo": "texto"},
                        {"nome": "cnpj_proponente", "origens": ["cnpj"], "tipo": "texto"},
                        {
                            "nome": "endereco_empreendimento",
                            "origens": ["endereco_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {
                            "nome": "recortes_territoriais",
                            "origens": ["recortes_territoriais"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "forma_parcelamento_solo",
                            "origens": ["forma_parcelamento_solo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "tipologia_unidade",
                            "origens": ["tipologia_unidade"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "administracao_por_condominio",
                            "origens": ["administracao_por_condominio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "trabalho_tecnico_social_tts",
                            "origens": ["trabalho_tecnico_social_tts"],
                            "tipo": "texto",
                        },
                        {"nome": "quantidade_uh", "origens": ["unidades"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_contrapartida_poder_publico",
                            "origens": ["valor_da_contrapartida_do_poder_publico"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["valor_total_do_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_desembolsado",
                            "origens": ["valor_total_liberado"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_fgts",
                            "origens": ["siaci_valorsubsidio_fgts"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_subsidio_ogu",
                            "origens": ["siaci_valorsubsidio_ogu"],
                            "tipo": "valor",
                        },
                        {"nome": "dt_contratacao", "origens": ["dataprevistasr"], "tipo": "data"},
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["de_obra_executada"],
                            "tipo": "percentual",
                        },
                        {
                            "nome": "dt_termino_obra_cronograma",
                            "origens": ["data_prevista_termino_obra"],
                            "tipo": "data",
                        },
                        {
                            "nome": "quantidade_uh_comercializadas",
                            "origens": ["comercializados"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "demanda_far_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["codapf"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "id_mcmv", "origens": ["id_mcmv"], "tipo": "texto"},
                        {"nome": "quantidade_uh", "origens": ["unidades"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["unidades_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "percentual_execucao_fisica",
                            "origens": ["cronograma_perc_real"],
                            "tipo": "percentual_auto",
                        },
                        {
                            "nome": "dt_termino_obra_cronograma",
                            "origens": ["cronograma_datatermino"],
                            "tipo": "data",
                        },
                        {"nome": "tipologia", "origens": ["tipologia"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_fonte",
                            "origens": ["ibge_codigo"],
                            "tipo": "texto",
                        },
                        {"nome": "re", "origens": ["re"], "tipo": "texto"},
                        {"nome": "contratado", "origens": ["contratado"], "tipo": "texto"},
                        {
                            "nome": "quantidade_desligados",
                            "origens": ["desligados"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_a_desligar_enquadradas",
                            "origens": ["unidades_a_desligar_enq"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["data_assinatura"],
                            "tipo": "data",
                        },
                        {
                            "nome": "dt_previsao_termino_obra",
                            "origens": ["data_termino_prev"],
                            "tipo": "data",
                        },
                        {"nome": "mes_referencia", "origens": ["anomes"], "tipo": "ano_mes"},
                    ],
                    "descartar": [],
                },
                "rural_caixa": {
                    "colunas": [
                        {"nome": "apf", "origens": ["codapf"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "contratado", "origens": ["contratado"], "tipo": "texto"},
                        {"nome": "status", "origens": ["status"], "tipo": "texto"},
                    ],
                    "descartar": [],
                },
                "tab_operacoes_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cod_cnpj_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["cod_mun_ibge"],
                            "tipo": "ibge",
                        },
                        {
                            "nome": "latitude",
                            "origens": ["num_latitude"],
                            "tipo": "latitude_br",
                            "par": "longitude",
                            "ibge": "codigo_ibge_municipio",
                        },
                        {
                            "nome": "longitude",
                            "origens": ["num_longitude"],
                            "tipo": "longitude_br",
                            "par": "latitude",
                        },
                        {
                            "nome": "codigo_empreendimento_agrupador_bb",
                            "origens": ["cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_natureza_operacao",
                            "origens": ["cod_nat_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_operacao",
                            "origens": ["cod_sit_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_motivo_rejeicao",
                            "origens": ["cod_motivo_rejeicao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_subprograma",
                            "origens": ["cod_subprograma"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_linha_credito",
                            "origens": ["cod_linha_credito"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_faixa_renda",
                            "origens": ["cod_faixa_renda"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_tipo_etapa",
                            "origens": ["cod_tipo_etapa"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_insercao_municipio",
                            "origens": ["cod_insercao_municipio"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_comunidade_atendida",
                            "origens": ["cod_comun_atendida"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "cnpj_agente_financeiro",
                            "origens": ["cod_cnpj_agente_finan"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_fase_mcmv",
                            "origens": ["cod_fase_pmcmv"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_apresentacao_projeto",
                            "origens": ["dte_apres_projeto"],
                            "tipo": "data_excel",
                        },
                        {
                            "nome": "dt_conclusao_analise",
                            "origens": ["dte_conclu_analise"],
                            "tipo": "data",
                        },
                        {"nome": "nome_etapa", "origens": ["txt_nome_etapa"], "tipo": "texto"},
                        {"nome": "endereco_etapa", "origens": ["txt_end_etapa"], "tipo": "texto"},
                        {"nome": "cep_etapa", "origens": ["num_cep_etapa"], "tipo": "texto"},
                        {
                            "nome": "area_total_etapa",
                            "origens": ["num_area_total_etapa"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_investimento",
                            "origens": ["vlr_total_investimento"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_operacao",
                            "origens": ["vlr_operacao"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "valor_recurso_proprio_tomador",
                            "origens": ["vlr_rec_prop_tomador"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "quantidade_uh_adaptadas",
                            "origens": ["qtd_und_adaptadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_enquadraveis",
                            "origens": ["qtd_und_enquadraveis"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_adaptaveis",
                            "origens": ["qtd_und_adaptaveis"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": ["dte_prev_contratacao"],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_empreendimentos_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_agrupador_bb",
                            "origens": ["cod_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["txt_empreendimento"],
                            "tipo": "texto",
                        },
                    ],
                    "descartar": [],
                },
                "tab_contratos_pj_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_contrato_pj",
                            "origens": ["cod_contrato_pj"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_contrato_pj",
                            "origens": ["cod_sit_contrato_pj"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao",
                            "origens": ["dte_assin_contrat"],
                            "tipo": "data",
                        },
                    ],
                    "descartar": [],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_proponentes_bb": {
                    "colunas": [
                        {
                            "nome": "cnpj_proponente",
                            "origens": ["cod_cnpj_proponente"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_porte_construtor",
                            "origens": ["cod_porte_construtor"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_natureza_proponente",
                            "origens": ["cod_nat_proponente"],
                            "tipo": "texto",
                        },
                        {"nome": "nome_proponente", "origens": ["txt_ra"], "tipo": "texto"},
                    ],
                    "descartar": [],
                },
                "tab_andamento_obras_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_situacao_obra",
                            "origens": ["cod_sit_obra"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_regime_construcao",
                            "origens": ["cod_regime_construcao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_pendencia_obra",
                            "origens": ["cod_pendencia_obra"],
                            "tipo": "texto",
                        },
                        {"nome": "sem_nome_10", "origens": ["sem_nome_10"], "tipo": "texto"},
                        {"nome": "sem_nome_11", "origens": ["sem_nome_11"], "tipo": "texto"},
                        {"nome": "sem_nome_12", "origens": ["sem_nome_12"], "tipo": "texto"},
                        {"nome": "sem_nome_13", "origens": ["sem_nome_13"], "tipo": "texto"},
                        {"nome": "sem_nome_14", "origens": ["sem_nome_14"], "tipo": "texto"},
                        {"nome": "sem_nome_15", "origens": ["sem_nome_15"], "tipo": "texto"},
                        {"nome": "sem_nome_16", "origens": ["sem_nome_16"], "tipo": "texto"},
                        {"nome": "sem_nome_17", "origens": ["sem_nome_17"], "tipo": "texto"},
                    ],
                    "descartar": [
                        "sem_nome_05",
                        "sem_nome_06",
                        "sem_nome_07",
                        "sem_nome_08",
                        "sem_nome_09",
                    ],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_unidades_concluidas_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_motivo_ociosidade",
                            "origens": ["cod_motivo_ociosidade"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_primeira_entrega",
                            "origens": ["dte_primeira_en"],
                            "tipo": "data",
                        },
                        {"nome": "sem_nome_05", "origens": ["sem_nome_05"], "tipo": "texto"},
                        {"nome": "sem_nome_06", "origens": ["sem_nome_06"], "tipo": "texto"},
                        {"nome": "sem_nome_07", "origens": ["sem_nome_07"], "tipo": "texto"},
                    ],
                    "descartar": ["dte_legalizacao"],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "tab_caracterizacoes_entornos_bb": {
                    "colunas": [
                        {
                            "nome": "codigo_empreendimento_bb",
                            "origens": ["cod_operacao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "ic_empreendimento_his",
                            "origens": ["bln_empreendimento_his"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "ic_rede_abastecimento_agua",
                            "origens": ["bln_rede_abast_agua"],
                            "tipo": "texto",
                        },
                        {"nome": "ic_coleta_lixo", "origens": ["bln_coleta"], "tipo": "texto"},
                        {"nome": "sem_nome_05", "origens": ["sem_nome_05"], "tipo": "texto"},
                        {"nome": "sem_nome_06", "origens": ["sem_nome_06"], "tipo": "texto"},
                        {"nome": "sem_nome_07", "origens": ["sem_nome_07"], "tipo": "texto"},
                        {"nome": "sem_nome_08", "origens": ["sem_nome_08"], "tipo": "texto"},
                        {"nome": "sem_nome_09", "origens": ["sem_nome_09"], "tipo": "texto"},
                        {"nome": "sem_nome_10", "origens": ["sem_nome_10"], "tipo": "texto"},
                        {"nome": "sem_nome_11", "origens": ["sem_nome_11"], "tipo": "texto"},
                        {"nome": "sem_nome_12", "origens": ["sem_nome_12"], "tipo": "texto"},
                        {"nome": "sem_nome_13", "origens": ["sem_nome_13"], "tipo": "texto"},
                        {"nome": "sem_nome_14", "origens": ["sem_nome_14"], "tipo": "texto"},
                        {"nome": "sem_nome_15", "origens": ["sem_nome_15"], "tipo": "texto"},
                        {"nome": "sem_nome_16", "origens": ["sem_nome_16"], "tipo": "texto"},
                    ],
                    "descartar": [],
                    "ponte_apf": "codigo_empreendimento_bb",
                },
                "contratacao_caixa_2010": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_enquadraveis",
                            "origens": ["uh_enquadraveis"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_uh_enquadraveis",
                            "origens": ["valor_das_uh_enquadraveis"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "pmcmv_parte2_caixa": {
                    "colunas": [
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_empreendimento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_ibge"],
                            "tipo": "ibge",
                        },
                        {
                            "nome": "municipio",
                            "origens": ["nome_municipio", "municipio"],
                            "tipo": "texto",
                        },
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh",
                            "origens": ["quant_unidades_total"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_unidades_total",
                            "origens": ["valor_unidades_total"],
                            "tipo": "valor",
                        },
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {"nome": "nome_uf", "origens": ["estado"], "tipo": "texto"},
                        {"nome": "recurso", "origens": ["recurso"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_enquadraveis",
                            "origens": ["qtd_enquadraveis"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "valor_uh_enquadraveis",
                            "origens": ["vlr_enquadraveis"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "propostas_recebidas_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nomeempreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "quantidade_uh", "origens": ["unidades"], "tipo": "inteiro"},
                        {"nome": "valor", "origens": ["valor"], "tipo": "valor"},
                        {"nome": "contratado", "origens": ["contratado"], "tipo": "texto"},
                        {
                            "nome": "proposta_em_negociacao",
                            "origens": ["proposta_em_negociacao"],
                            "tipo": "texto",
                        },
                    ],
                    "descartar": [],
                },
                "dados_bb_2011": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "codigo_ibge_municipio",
                            "origens": ["codigo_do_munic_ibge"],
                            "tipo": "ibge",
                        },
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nome_do_empreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {
                            "nome": "tipo_de_financiamento",
                            "origens": ["tipo_de_financiamento"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "empreendimentos_contiguos_isolados",
                            "origens": ["empreendimentos_contiguos_isolados"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_financiadas",
                            "origens": ["unidades_financiadas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "montante_financiamentos_pf_no_bb",
                            "origens": ["montante_financiamentos_pf_no_bb"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "previsao_entrega",
                            "origens": ["data_prevista_para_entrega"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "dt_contratacao_convenio",
                            "origens": ["data_de_contratacao_convenio"],
                            "tipo": "data",
                        },
                    ],
                    "descartar": ["faixa_de_renda_3_a_6_sm_ou_6_a_10_sm"],
                },
                "relatorio_executivo_municipio_caixa": {
                    "colunas": [
                        {"nome": "codigo_ibge_municipio", "origens": ["codibge"], "tipo": "ibge"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "quantidade_uh", "origens": ["uh"], "tipo": "inteiro"},
                        {"nome": "valor", "origens": ["valor"], "tipo": "valor"},
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["uh_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["uh_entregues"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "contratacao_uf_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "valor", "origens": ["valor"], "tipo": "valor"},
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["uh_concluidas"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["uh_entregues"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "agente_financeiro",
                            "origens": ["instituicao"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_contratadas",
                            "origens": ["uh_contratadas"],
                            "tipo": "inteiro",
                        },
                        {"nome": "pmcmv", "origens": ["pmcmv"], "tipo": "texto"},
                    ],
                    "descartar": [],
                },
                "bext_agregado_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {
                            "nome": "valor_emprestimo",
                            "origens": ["valor_do_emprestimo"],
                            "tipo": "valor",
                        },
                        {"nome": "quantidade_uh", "origens": ["uh"], "tipo": "inteiro"},
                        {"nome": "faixa_execucao_fisica", "origens": ["obra"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_em_obras",
                            "origens": ["uh_em_obras"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["uh_concluidos"],
                            "tipo": "inteiro",
                        },
                        {"nome": "agente_financeiro", "origens": ["inst"], "tipo": "texto"},
                        {"nome": "mes_referencia", "origens": ["anomes"], "tipo": "ano_mes"},
                        {"nome": "pmcmv", "origens": ["pmcmv"], "tipo": "texto"},
                        {"nome": "origem", "origens": ["origem"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["entregues"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "contratacao_pf_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "codigo_ibge_municipio", "origens": ["codibge"], "tipo": "ibge"},
                        {"nome": "municipio", "origens": ["nomemunicipio"], "tipo": "texto"},
                        {"nome": "quantidade_uh", "origens": ["uh"], "tipo": "inteiro"},
                        {"nome": "valor", "origens": ["valor"], "tipo": "valor"},
                        {"nome": "ano_contratacao", "origens": ["anocontratacao"], "tipo": "ano"},
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {
                            "nome": "siaci_nome_do_produto_pf",
                            "origens": ["siaci_nome_do_produto_pf"],
                            "tipo": "texto",
                        },
                        {"nome": "renda_sm", "origens": ["renda_sm"], "tipo": "texto"},
                        {"nome": "sub_fgts", "origens": ["sub_fgts"], "tipo": "valor_calculado"},
                        {"nome": "sub_ogu", "origens": ["sub_ogu"], "tipo": "valor_calculado"},
                        {
                            "nome": "valor_recurso_proprio",
                            "origens": ["valor_do_recurso_proprio"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "valor_operacao",
                            "origens": ["valor_total_da_operacao"],
                            "tipo": "valor_calculado",
                        },
                        {"nome": "nome_uf", "origens": ["estado"], "tipo": "texto"},
                        {"nome": "e_rm", "origens": ["e_rm"], "tipo": "texto"},
                        {"nome": "e_capital", "origens": ["e_capital"], "tipo": "texto"},
                        {
                            "nome": "nome_faixa_pop_1",
                            "origens": ["nome_faixa_pop_1"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["unidadeentregue"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "contratacao_pf_total_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {
                            "nome": "nome_do_produto_pf",
                            "origens": ["nome_do_produto_pf"],
                            "tipo": "texto",
                        },
                        {"nome": "quantidade_uh", "origens": ["qtd_unidades"], "tipo": "inteiro"},
                        {
                            "nome": "valor_financiamento",
                            "origens": ["valor_financiamento"],
                            "tipo": "valor_calculado",
                        },
                        {"nome": "sub_fgts", "origens": ["sub_fgts"], "tipo": "valor_calculado"},
                        {"nome": "sub_ogu", "origens": ["sub_ogu"], "tipo": "valor_calculado"},
                        {
                            "nome": "valor_total",
                            "origens": ["valor_total"],
                            "tipo": "valor_calculado",
                        },
                    ],
                    "descartar": [],
                },
                "presidencia_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "codigo_ibge_municipio", "origens": ["codibge"], "tipo": "ibge"},
                        {"nome": "regiao", "origens": ["regiao"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["nome"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh_0a3sm",
                            "origens": ["uh_0a3sm"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_3a6sm",
                            "origens": ["uh_3a6sm"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_6a10sm",
                            "origens": ["uh_6a10sm"],
                            "tipo": "inteiro",
                        },
                        {"nome": "quantidade_uh", "origens": ["uh_total"], "tipo": "inteiro"},
                        {"nome": "valor_0a3sm", "origens": ["valor_0a3sm"], "tipo": "valor"},
                        {"nome": "valor_3a6sm", "origens": ["valor_3a6sm"], "tipo": "valor"},
                        {"nome": "valor_6a10sm", "origens": ["valor_6a10sm"], "tipo": "valor"},
                        {
                            "nome": "valor_total",
                            "origens": ["valor_total"],
                            "tipo": "valor_calculado",
                        },
                        {"nome": "meta_0a3sm", "origens": ["meta_0a3sm"], "tipo": "inteiro"},
                        {"nome": "meta_3a6sm", "origens": ["meta_3a6sm"], "tipo": "inteiro"},
                        {"nome": "meta_6a10sm", "origens": ["meta_6a10sm"], "tipo": "inteiro"},
                        {"nome": "meta_total", "origens": ["meta_total"], "tipo": "inteiro"},
                        {
                            "nome": "quantidade_uh_faixa1",
                            "origens": ["uh_faixa1"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_faixa2",
                            "origens": ["uh_faixa2"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_faixa3",
                            "origens": ["uh_faixa3"],
                            "tipo": "inteiro",
                        },
                        {"nome": "valor_faixa1", "origens": ["valor_faixa1"], "tipo": "valor"},
                        {"nome": "valor_faixa2", "origens": ["valor_faixa2"], "tipo": "valor"},
                        {"nome": "valor_faixa3", "origens": ["valor_faixa3"], "tipo": "valor"},
                        {
                            "nome": "subsidio_faixa1",
                            "origens": ["subsidio_faixa1"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "subsidio_faixa2",
                            "origens": ["subsidio_faixa2"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "subsidio_faixa3",
                            "origens": ["subsidio_faixa3"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "subsidio_total",
                            "origens": ["subsidio_total"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [],
                },
                "desligados_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {
                            "nome": "nome_empreendimento",
                            "origens": ["nomeempreendimento"],
                            "tipo": "texto",
                        },
                        {"nome": "produto", "origens": ["produto"], "tipo": "texto"},
                        {"nome": "faixa", "origens": ["faixa"], "tipo": "texto"},
                        {
                            "nome": "quantidade_uh",
                            "origens": ["soma_de_unidades"],
                            "tipo": "inteiro",
                        },
                        {"nome": "valor", "origens": ["soma_de_valor"], "tipo": "valor"},
                        {
                            "nome": "quantidade_desligados",
                            "origens": ["soma_de_desligados"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "previsao_entrega_far_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["unidade_da_federacao"], "tipo": "texto"},
                        {
                            "nome": "quantidade_empreendimentos_contratados",
                            "origens": ["contratados_empr"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_contratadas",
                            "origens": ["contratados_uh"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_empreendimentos_concluidos",
                            "origens": ["concluidos_empr"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_concluidas",
                            "origens": ["concluidos_uh"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_empreendimentos_entregues",
                            "origens": ["entregues_empr"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "quantidade_uh_entregues",
                            "origens": ["entregues_uh"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "previsao_de_conclusao_ate_dez_2011_empr",
                            "origens": ["previsao_de_conclusao_ate_dez_2011_empr"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "previsao_de_conclusao_ate_dez_2011_uh",
                            "origens": ["previsao_de_conclusao_ate_dez_2011_uh"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "previsao_de_entrega_ate_dez_2011_empr",
                            "origens": ["previsao_de_entrega_ate_dez_2011_empr"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "previsao_de_entrega_ate_dez_2011_uh",
                            "origens": ["previsao_de_entrega_ate_dez_2011_uh"],
                            "tipo": "texto",
                        },
                    ],
                    "descartar": [],
                },
                "relatorio_semanal_bb_uf": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {
                            "nome": "desligamento_plano_piloto_quantidade_unidades",
                            "origens": ["desligamento_plano_piloto_quantidade_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_total_financ_pf_contratados",
                            "origens": [
                                "desligamento_plano_piloto_total_de_financ_pf_contratados_desligamento"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_meta_r",
                            "origens": ["desligamento_plano_piloto_meta_r"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_pct",
                            "origens": ["desligamento_plano_piloto_pct"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_meta_r",
                            "origens": ["aprovado_cef_meta_r"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_quantidade_unidades",
                            "origens": ["contratacao_total_quantidade_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_valor",
                            "origens": ["contratacao_total_valor"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_unid_financ_pf",
                            "origens": ["desligamento_plano_piloto_unid_financ_pf"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_unid_disponiveis",
                            "origens": ["desligamento_plano_piloto_unid_disponiveis"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_total_financ_pf",
                            "origens": ["desligamento_plano_piloto_total_financ_pf"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_quantidade_unidades",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_quantidade_unidades",
                                "aprovado_cef_quantidade_unidades",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_unid_financ_pf",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_unid_financ_pf"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_unid_disponiveis",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_unid_disponiveis"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_pf",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_pf"
                            ],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_total_financ_producao_pj",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_a_producao_pj",
                                "aprovado_cef_valor_total",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_meta_r",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_meta_r",
                                "aprovado_cef_financ_a_producao_pj_e_repasse_meta_pj_r",
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_pct_meta_r",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_pct_meta_r",
                                "aprovado_cef_pct_meta_r",
                            ],
                            "tipo": "valor_calculado",
                        },
                    ],
                    "descartar": [],
                },
                "relatorio_semanal_bb_publico": {
                    "colunas": [
                        {
                            "nome": "uf",
                            "origens": ["desligamento_plano_piloto_uf", "uf"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desligamento_plano_piloto_publico_alvo",
                            "origens": ["desligamento_plano_piloto_publico_alvo", "publico_alvo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desligamento_plano_piloto_unidades_cadastradas",
                            "origens": ["desligamento_plano_piloto_unidades_cadastradas"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_unid_financ_pf",
                            "origens": ["desligamento_plano_piloto_unid_financ_pf"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_total_financ_pf",
                            "origens": ["desligamento_plano_piloto_total_financ_pf"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_unid_financ_pf",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_unid_financ_pf"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_unid_disponiveis",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_unid_disponiveis"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_pf",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_pf"
                            ],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_total_financ_producao_pj",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_a_producao_pj"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_pf_no_unidades",
                            "origens": ["contratacao_total_pf_no_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_pf_valor_r",
                            "origens": ["contratacao_total_pf_valor_r"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "desligamento_quantidade_unidades",
                            "origens": ["desligamento_quantidade_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_valor_total",
                            "origens": ["desligamento_valor_total"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_quantidade_unidades",
                            "origens": ["aprovado_cef_quantidade_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_valor_total",
                            "origens": ["aprovado_cef_valor_total"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_meta",
                            "origens": ["contratacao_total_meta"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_pct",
                            "origens": ["contratacao_total_pct"],
                            "tipo": "percentual",
                        },
                        {
                            "nome": "contratacao_total_unidades",
                            "origens": ["contratacao_total_unidades"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_valor_r",
                            "origens": ["contratacao_total_valor_r"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_no_de_unidades",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_no_de_unidades"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "contratacao_total_no_unidades",
                            "origens": ["contratacao_total_no_unidades"],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": ["contratacao_total_quantidade_unidades"],
                },
                "propostas_far_caixa": {
                    "colunas": [
                        {"nome": "uf", "origens": ["uf"], "tipo": "texto"},
                        {"nome": "municipio", "origens": ["municipio"], "tipo": "texto"},
                        {"nome": "sr", "origens": ["sr"], "tipo": "texto"},
                        {"nome": "rm", "origens": ["rm"], "tipo": "texto"},
                        {
                            "nome": "diretriz_mcidades",
                            "origens": ["diretriz_mcidades"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "deficit_urbano_2010",
                            "origens": ["deficit_urbano_2010"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "contratacao_caixa_mcmv2",
                            "origens": ["contratacao_caixa_mcmv2"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "contratacao_bb",
                            "origens": ["contratacao_bb"],
                            "tipo": "inteiro",
                        },
                        {
                            "nome": "propostas_em_analise",
                            "origens": ["propostas_em_analise"],
                            "tipo": "inteiro",
                        },
                    ],
                    "descartar": [],
                },
                "propostas_bb": {
                    "colunas": [
                        {
                            "nome": "uf",
                            "origens": ["desligamento_plano_piloto_uf"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desligamento_plano_piloto_publico_alvo",
                            "origens": ["desligamento_plano_piloto_publico_alvo"],
                            "tipo": "texto",
                        },
                        {
                            "nome": "desligamento_plano_piloto_unid_financ_pf",
                            "origens": ["desligamento_plano_piloto_unid_financ_pf"],
                            "tipo": "valor",
                        },
                        {
                            "nome": "desligamento_plano_piloto_total_financ_pf",
                            "origens": ["desligamento_plano_piloto_total_financ_pf"],
                            "tipo": "valor_calculado",
                        },
                        {
                            "nome": "aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas"
                            ],
                            "tipo": "valor",
                        },
                        {
                            "nome": "aprovado_cef_total_financ_producao_pj",
                            "origens": [
                                "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_a_producao_pj"
                            ],
                            "tipo": "valor",
                        },
                    ],
                    "descartar": [
                        "desligamento_plano_piloto_unidades_cadastradas",
                        "aprovado_cef_financ_a_producao_pj_e_repasse_unid_financ_pf",
                        "aprovado_cef_financ_a_producao_pj_e_repasse_unid_disponiveis",
                        "aprovado_cef_financ_a_producao_pj_e_repasse_total_financ_pf",
                        "contratacao_total_pf_no_unidades",
                        "contratacao_total_pf_valor_r",
                    ],
                },
            }
        )
    }}
{% endmacro %}

{#
    Famílias do cleansed sem silver, com o motivo (estudo de 16/11/2026, research R4 da
    spec 005): nada sobre os empreendimentos subsidiados ou financiados, nem um recorte
    territorial útil para a gold. O teste dhist_familias_cobertura cobra que toda família
    de familias_dhist() esteja aqui ou no de-para, nunca nos dois.
#}
{% macro descartes_dhist() %}
    {{
        return(
            {
                "tab_arquivos_dados_bb": "metadado do envio do arquivo (1 linha)",
                "validacao_caixa": "cópia: as 3.105 linhas casam 100% com a base_pf_pj_caixa de 18/04/2012",
                "balanco_min_planejamento_bb": "total nacional por faixa e estágio de obra (2 linhas), sem empreendimento nem território",
                "propostas_contratadas_caixa_2009": "total nacional por faixa (3 linhas), sem empreendimento nem território",
                "propostas_recebidas_caixa_2009": "total nacional por faixa (3 linhas), sem empreendimento nem território",
                "municipios_contratados_uf_caixa": "contagem de municípios atendidos por UF, que a gold deriva sozinha",
                "municipios_contratados_regiao_caixa": "contagem de municípios atendidos por região, que a gold deriva sozinha",
            }
        )
    }}
{% endmacro %}
