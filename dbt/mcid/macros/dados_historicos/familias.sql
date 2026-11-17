{#
    As famílias do cleansed dos dados históricos (uma pasta cada em
    `tests/cleansed/dados_historicos/<familia>/`). Quem decide a família de cada arquivo é
    o mapa `familias.csv` da dados_historicos_dag, no MinIO; aqui ficam só os nomes, para
    a bronze ter um modelo por família e os testes percorrerem todas.
#}
{% macro familias_dhist() %}
    {{
        return(
            [
                "balanco_min_planejamento_bb",
                "base_pf_pj_caixa",
                "bext",
                "bext_agregado_caixa",
                "contratacao_caixa_2010",
                "contratacao_pf_caixa",
                "contratacao_pf_total_caixa",
                "contratacao_uf_caixa",
                "dados_bb_2011",
                "datas_entregas_pj_caixa",
                "demanda_far_caixa",
                "desligados_caixa",
                "empreendimentos_contratados_caixa",
                "entrada_bb",
                "entrada_bb_aux_far",
                "entrega_bb_pf_pf",
                "entrega_bb_pj",
                "entrega_bb_pj_pf",
                "int040_far_caixa",
                "int054_far_bb",
                "municipios_contratados_regiao_caixa",
                "municipios_contratados_uf_caixa",
                "pmcmv_parte2_caixa",
                "pnhr_bb",
                "presidencia_caixa",
                "previsao_entrega_far_caixa",
                "propostas_bb",
                "propostas_contratadas_caixa_2009",
                "propostas_far_caixa",
                "propostas_recebidas_caixa",
                "propostas_recebidas_caixa_2009",
                "relatorio_cidades_caixa",
                "relatorio_cidades_entregas_caixa",
                "relatorio_executivo_caixa",
                "relatorio_executivo_municipio_caixa",
                "relatorio_semanal_bb_publico",
                "relatorio_semanal_bb_uf",
                "rural_caixa",
                "snh_af_bb",
                "snh_af_caixa",
                "snh_entregas_af_bb",
                "snh_entregas_af_caixa",
                "tab_andamento_obras_bb",
                "tab_arquivos_dados_bb",
                "tab_beneficiarios_fgts_bb",
                "tab_caracterizacoes_entornos_bb",
                "tab_contratos_pf_fgts_bb",
                "tab_contratos_pj_bb",
                "tab_empreendimentos_bb",
                "tab_operacoes_bb",
                "tab_proponentes_bb",
                "tab_unidades_concluidas_bb",
                "validacao_caixa",
            ]
        )
    }}
{% endmacro %}
