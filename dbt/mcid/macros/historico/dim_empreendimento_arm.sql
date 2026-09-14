{#
    Braço da dim_empreendimento_historico (change colunas-orfas-bronze-historico).

    Projeta o contrato de atributos ESTÁVEIS do empreendimento a partir de
    qualquer bronze de empreendimento (INT040/054/057/059/065, snh_bb/caixa),
    resolvendo cada campo por lista de aliases via coalesce_present — que
    introspecciona a bronze materializada e compila a coluna ausente como NULL do
    tipo certo (uma família não tem a maioria destas colunas).

    Grão de saída do braço: 1 linha por linha da bronze (apf + dt_referencia +
    atributos). A ded9up para 1 linha/apf e depois 1 linha/(frente,codigo) é
    feita no modelo. (dedup, não "ded9up".)
#}
{% macro dim_empreendimento_arm(rel) %}
        select
            coalesce(
                nullif(nullif(trim(({{ coalesce_present(rel, ['nu_apf']) }})::text), ''), 'NULL'),
                nullif(nullif(trim(({{ coalesce_present(rel, ['nu_contrato_empreendimento', 'nu_contrato_emprendimento']) }})::text), ''), 'NULL'),
                nullif(trim(({{ coalesce_present(rel, ['apf']) }})::text), '')
            ) as apf,
            date_trunc('month', dt_referencia)::date as snap_date,
            nullif(trim(({{ coalesce_present(rel, ['no_entidade_organizadora']) }})::text), '')
            as no_entidade_organizadora,
            nullif(trim(({{ coalesce_present(rel, ['nu_cnpj_entidade']) }})::text), '')
            as nu_cnpj_entidade,
            nullif(nullif(trim(({{ coalesce_present(rel, ['dsc_tipologia']) }})::text), ''), 'NULL')
            as dsc_tipologia,
            nullif(nullif(trim(({{ coalesce_present(rel, ['tipo_de_unidade_do_empreendimento']) }})::text), ''), 'NULL')
            as tipo_de_unidade_do_empreendimento,
            nullif(nullif(trim(({{ coalesce_present(rel, ['regime_construcao']) }})::text), ''), 'NULL')
            as regime_construcao,
            nullif(nullif(trim(({{ coalesce_present(rel, ['cod_regime_execucao']) }})::text), ''), 'NULL')
            as cod_regime_execucao,
            nullif(nullif(trim(({{ coalesce_present(rel, ['modalidade_requalificacao']) }})::text), ''), 'NULL')
            as modalidade_requalificacao,
            -- coordenadas: só o SNH tem decimal (`-22,9161`); sentinela '0' -> NULL.
            nullif(
                try_cast(replace(nullif(trim(({{ coalesce_present(rel, ['latitude_do_imovel']) }})::text), ''), ',', '.') as double),
                0
            ) as latitude,
            nullif(
                try_cast(replace(nullif(trim(({{ coalesce_present(rel, ['longitude_do_imovel']) }})::text), ''), ',', '.') as double),
                0
            ) as longitude,
            nullif(trim(({{ coalesce_present(rel, ['bairro_do_imovel']) }})::text), '')
            as bairro,
            nullif(trim(({{ coalesce_present(rel, ['nu_cep', 'co_cep', 'cep_do_imovel']) }})::text), '')
            as cep,
            nullif(trim(({{ coalesce_present(rel, ['no_logradouro', 'logradouro', 'logradouro_do_imovel']) }})::text), '')
            as logradouro,
            nullif(trim(({{ coalesce_present(rel, ['nu_apf_vinculacao']) }})::text), '')
            as nu_apf_vinculacao,
            nullif(nullif(trim(({{ coalesce_present(rel, ['portaria_selecao']) }})::text), ''), 'NULL')
            as portaria_selecao
        from {{ rel }}
        where nullif(trim(
            {{ coalesce_present(rel, ['nu_apf', 'nu_contrato_empreendimento', 'nu_contrato_emprendimento', 'apf']) }}::text
        ), '') is not null
{% endmacro %}
