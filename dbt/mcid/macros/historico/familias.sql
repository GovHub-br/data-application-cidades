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
            'modelo': 'bronze_dhist_serie_bases_relatorio_executivo',
            'glob': 'dados_historicos/*bases_relat*rio_executivo*.parquet',
            'grao': 'empreendimento',
        },
        {
            'nome': 'min_cidades',
            'modelo': 'bronze_dhist_serie_min_cidades',
            'glob': 'dados_historicos/*min_cidades*.parquet',
            'grao': 'empreendimento/contrato (BB)',
        },
        {
            'nome': 'entrada_bb',
            'modelo': 'bronze_dhist_serie_entrada_bb',
            'glob': 'dados_historicos/*entrada_bb*.parquet',
            'grao': 'empreendimento (BB)',
        },
        {
            'nome': 'bext',
            'modelo': 'bronze_dhist_serie_bext',
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
            'modelo': 'bronze_sftp_empreendimento_int040',
            'fonte_interface': 'INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT040_*.parquet',
            'frente': 'FAR CAIXA',
        },
        {
            'nome': 'INT054',
            'modelo': 'bronze_sftp_empreendimento_int054',
            'fonte_interface': 'INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT054_*.parquet',
            'frente': 'FAR BB',
        },
        {
            'nome': 'INT057',
            'modelo': 'bronze_sftp_empreendimento_int057',
            'fonte_interface': 'INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT057_*.parquet',
            'frente': 'PNHR / Rural BB',
        },
        {
            'nome': 'INT059',
            'modelo': 'bronze_sftp_empreendimento_int059',
            'fonte_interface': 'INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS',
            'glob': 'sftp/fabrica/GEFUS/**/INT059_*.parquet',
            'frente': 'FDS / Entidades',
        },
        {
            'nome': 'INT065',
            'modelo': 'bronze_sftp_empreendimento_int065',
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
            'modelo': 'bronze_dhist_empreendimento_snh_bb',
            'glob': 'dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_bb*.parquet',
        },
        {
            'nome': 'CAIXA',
            'modelo': 'bronze_dhist_empreendimento_snh_caixa',
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
            'modelo': 'bronze_dhist_snh_entregas_evento_bb',
            'glob': 'dados_historicos/*snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb.parquet',
        },
        {
            'nome': 'CAIXA',
            'modelo': 'bronze_dhist_snh_entregas_evento_caixa',
            'glob': 'dados_historicos/*snh_pmcmv_dados_prioritarios_af_caixa_entregas.parquet',
        },
    ]) }}
{% endmacro %}

{#- Evolucao de obra por empreendimento (MONIT_MOV_OBRA), 3 frentes.
    Fonte: os snapshots datados `MONIT_MOV_OBRA_<FRENTE>_MENSAL_YYYYMM` sob
    `staging/sharepoint/Novo MCMV - */` (glob recursivo `**`). A frente e
    selecionada pela substring NO NOME DO ARQUIVO, nao pela pasta -- os arquivos
    FDS/RURAL de 202602+ estao fisicamente sob `Novo MCMV - FAR/` (misfiled).
    `_LAYOUT_` (dicionario de campos, `_YYYYMMDD`), `_SEMANAL_` e `_DIARIO_`
    ficam de fora -- o filtro `_MENSAL_` no proprio glob ja os exclui; o corpo
    reforca com `not ilike '%_LAYOUT_%'`. O flat
    `staging/sharepoint/novo_mcmv_<frente>_obra_mensal.parquet` (snapshot
    corrente sobrescrito, sem historico) NAO e usado. Janela real: 202512+.
    Change enriquecer-quantidades-uh-e-sinais-obra-historico (D4). -#}
{% macro familias_obra_mensal() %}
    {{ return([
        {
            'nome': 'OBRA_FAR',
            'modelo': 'bronze_shpt_obra_mensal_far',
            'frente': 'FAR',
            'glob': 'sharepoint/Novo MCMV - */**/*MONIT_MOV_OBRA_FAR_MENSAL_*.parquet',
        },
        {
            'nome': 'OBRA_FDS',
            'modelo': 'bronze_shpt_obra_mensal_fds',
            'frente': 'Entidades',
            'glob': 'sharepoint/Novo MCMV - */**/*MONIT_MOV_OBRA_FDS_MENSAL_*.parquet',
        },
        {
            'nome': 'OBRA_RURAL',
            'modelo': 'bronze_shpt_obra_mensal_rural',
            'frente': 'Rural',
            'glob': 'sharepoint/Novo MCMV - */**/*MONIT_MOV_OBRA_RURAL_MENSAL_*.parquet',
        },
    ]) }}
{% endmacro %}

{#- Frentes novas do GEFUS (change frentes-restantes-mcmv-historico), 3
    familias. Mesma pasta das interfaces INT0xx, mas o nome do arquivo usa
    `_YYYY_MM_DD.parquet` (com underscore), nao `_YYYYMMDD.parquet` -- por
    isso nao reaproveitam bronze_gefus/familias_gefus() e tem corpo proprio
    (bronze_frente_gefus_semanal em corpos_bronze.sql). -#}
{% macro familias_frentes_gefus() %}
    {{ return([
        {
            'nome': 'PMCMV_FAIXA3_MCID',
            'modelo': 'bronze_sftp_classe_media_faixa3',
            'glob': 'sftp/fabrica/GEFUS/**/PMCMV_FAIXA3_MCID_*.parquet',
            'frente': 'Classe Média',
        },
        {
            'nome': 'PMCMV_CIDADES_MCID',
            'modelo': 'bronze_sftp_mcmv_cidades',
            'glob': 'sftp/fabrica/GEFUS/**/PMCMV_CIDADES_MCID_*.parquet',
            'frente': 'MCMV Cidades',
        },
        {
            'nome': 'PMCMV_REFORMAS_MCID',
            'modelo': 'bronze_sftp_reforma_casa_brasil',
            'glob': 'sftp/fabrica/GEFUS/**/PMCMV_REFORMAS_MCID_*.parquet',
            'frente': 'Reforma Casa Brasil',
        },
    ]) }}
{% endmacro %}

{#- Frentes GEAVO de Pró-Moradia (Canal FGTS via CAIXA/GEAVO), 3 familias.
    Pasta `sftp/caixa.geavo/GEAVO/` (staging), pacote semanal
    `MC<aaaammdd>__MCidades_AO_<n>__<tabela>.parquet` -- a data do snapshot
    fica no PREFIXO do nome do arquivo, diferente das demais familias deste
    mapa (`_YYYY_MM_DD`/`_YYYYMMDD` sufixo) -- corpo proprio
    (bronze_geavo_semanal em corpos_bronze.sql), D1 da change
    enriquecer-pro-moradia-execucao-desembolso-historico. -#}
{% macro familias_geavo_pro_moradia() %}
    {{ return([
        {
            'nome': 'tab_desembolsos_fgts',
            'modelo': 'bronze_sftp_pro_moradia_desembolsos',
            'glob': 'sftp/caixa.geavo/GEAVO/MC*__MCidades_AO_2__tab_desembolsos_fgts.parquet',
            'frente': 'Pró-Moradia',
        },
        {
            'nome': 'tab_execucoes_obras',
            'modelo': 'bronze_sftp_pro_moradia_execucoes_obra',
            'glob': 'sftp/caixa.geavo/GEAVO/MC*__MCidades_AO_2__tab_execucoes_obras.parquet',
            'frente': 'Pró-Moradia',
        },
        {
            'nome': 'operacoes_paralisadas_fgts_setorpublico',
            'modelo': 'bronze_sftp_pro_moradia_paralisacoes',
            'glob': 'sftp/caixa.geavo/GEAVO/MC*__MCidades_AO_2__operacoes_paralisadas_fgts_setorpublico.parquet',
            'frente': 'Pró-Moradia',
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
