{#
    MAPA DE FAMILIAS do eixo historico (D5 da change
    pipeline-bronze-historica-destino-trocavel).

    Cada bronze de serie historica materializa UMA tabela por familia de
    origem; a uniao entre familias passou para a camada silver, com projecao
    explicita de colunas por braco. Este arquivo e a unica fonte de verdade
    sobre quais familias existem, qual glob da staging alimenta cada uma e
    qual modelo dbt a materializa — os 13 modelos sao cascas finas que
    chamam o corpo correspondente com o nome da familia.

    Colunas de auditoria comuns a todas as 13 (tarefa 3.6):
        source_file    nome do arquivo parquet de origem (coluna `filename`)
        dt_referencia  mes-snapshot da linha
        dt_ingest      instante da materializacao
        hash_linha     md5 da identidade da linha na origem
#}

{#- Serie executiva historica (pre-2024), 4 familias. -#}
{% macro familias_serie_executiva() %}
    {{ return([
        {
            'nome': 'bases_relatorio_executivo',
            'modelo': 'bronze_mcmv_historico_serie_bases_relatorio_executivo',
            'glob': 'dados_historicos/*bases_relat*rio_executivo*.parquet',
            'grao': 'empreendimento',
        },
        {
            'nome': 'min_cidades',
            'modelo': 'bronze_mcmv_historico_serie_min_cidades',
            'glob': 'dados_historicos/*min_cidades*.parquet',
            'grao': 'empreendimento/contrato (BB)',
        },
        {
            'nome': 'entrada_bb',
            'modelo': 'bronze_mcmv_historico_serie_entrada_bb',
            'glob': 'dados_historicos/*entrada_bb*.parquet',
            'grao': 'empreendimento (BB)',
        },
        {
            'nome': 'bext',
            'modelo': 'bronze_mcmv_historico_serie_bext',
            'glob': 'dados_historicos/*bext*.parquet',
            'grao': 'contrato PF (CAIXA)',
        },
    ]) }}
{% endmacro %}

{#- Interfaces de empreendimento do SFTP/GEFUS, 5 familias. -#}
{% macro familias_gefus() %}
    {{ return([
        {
            'nome': 'INT040',
            'modelo': 'bronze_mcmv_historico_empreendimento_int040',
            'fonte_interface': 'INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT040_*.parquet',
            'frente': 'FAR CAIXA',
        },
        {
            'nome': 'INT054',
            'modelo': 'bronze_mcmv_historico_empreendimento_int054',
            'fonte_interface': 'INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT054_*.parquet',
            'frente': 'FAR BB',
        },
        {
            'nome': 'INT057',
            'modelo': 'bronze_mcmv_historico_empreendimento_int057',
            'fonte_interface': 'INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT057_*.parquet',
            'frente': 'PNHR / Rural BB',
        },
        {
            'nome': 'INT059',
            'modelo': 'bronze_mcmv_historico_empreendimento_int059',
            'fonte_interface': 'INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT059_*.parquet',
            'frente': 'FDS / Entidades',
        },
        {
            'nome': 'INT065',
            'modelo': 'bronze_mcmv_historico_empreendimento_int065',
            'fonte_interface': 'INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT065_*.parquet',
            'frente': 'PNHR / Rural CAIXA',
        },
    ]) }}
{% endmacro %}

{#- Snapshots mensais SNH de dados prioritarios por empreendimento, 2 agentes.
    Os globs de CAIXA tambem casam os fluxos de ENTREGA (`*_af_caixa_entregas`);
    o filtro `not like '%entrega%'` no corpo os exclui, como antes. -#}
{% macro familias_snh_empreendimento() %}
    {{ return([
        {
            'nome': 'BB',
            'modelo': 'bronze_mcmv_historico_empreendimento_snh_bb',
            'glob': 'dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_bb*.parquet',
        },
        {
            'nome': 'CAIXA',
            'modelo': 'bronze_mcmv_historico_empreendimento_snh_caixa',
            'glob': 'dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_caixa*.parquet',
        },
    ]) }}
{% endmacro %}

{#- Fluxos de ENTREGA por evento da SNH, 2 agentes. Note que o nome do
    arquivo põe `entrega` ANTES do agente no BB e DEPOIS no CAIXA. -#}
{% macro familias_snh_entregas() %}
    {{ return([
        {
            'nome': 'BB',
            'modelo': 'bronze_reloginho_snh_entregas_evento_bb',
            'glob': 'dados_historicos/*snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb.parquet',
        },
        {
            'nome': 'CAIXA',
            'modelo': 'bronze_reloginho_snh_entregas_evento_caixa',
            'glob': 'dados_historicos/*snh_pmcmv_dados_prioritarios_af_caixa_entregas.parquet',
        },
    ]) }}
{% endmacro %}

{#- Busca uma familia pelo nome dentro de um dos mapas acima. -#}
{% macro familia(mapa, nome) %}
    {%- for f in mapa -%}
        {%- if f.nome == nome -%}{{ return(f) }}{%- endif -%}
    {%- endfor -%}
    {{ exceptions.raise_compiler_error(
        "familia '" ~ nome ~ "' nao existe no mapa (macros/historico/familias.sql)"
    ) }}
{% endmacro %}
