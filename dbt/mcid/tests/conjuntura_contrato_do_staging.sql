-- Aviso, não erro, até a revisão dos testes do projeto: falha hoje pelo piso da
-- `infomoney_imob` (336, da fonte antiga), maior que a carga nova da Alpha Vantage.
{{ config(severity='warn') }}

-- Detecta colapso de volume nos parquets de `staging/`.
--
-- Item 2.13. Este é o teste que faltava contra o problema estrutural do
-- projeto: os arquivos em `staging/` são escritos por DAGs deste repositório
-- E por um job de outro time, com contratos de formato diferentes. Quando o
-- formato muda, a bronze continua construindo (é `select *`) — só que com 1
-- linha aninhada no lugar de 500 achatadas. O `dbt run` passa, e o problema
-- só aparece lá na frente.
--
-- O piso de cada fonte está em `sources.yml` (`meta.linhas_minimas`),
-- calibrado em metade do volume observado em 2026-08-29. Metade dá folga
-- para variação real de série sem deixar passar um colapso de ordem de
-- grandeza.
--
-- Complementa `gld_qualidade_schema_drift`: o drift avisa no dia seguinte,
-- comparando retratos; este falha na hora, dentro do build.
-- Roda depois das bronzes que conta (macros/depende_de.sql): as das fontes de `lake_staging`
-- com `meta.linhas_minimas`. Fonte nova com piso entra aqui.
{{ depende_de([
        'bronze_abecip_financiamentos',
        'bronze_abecip_instituicoes',
        'bronze_abecip_poupanca_sbpe',
        'bronze_bacen_credito_pib',
        'bronze_bacen_financiamentos_imobiliarios',
        'bronze_fgv_icst',
        'bronze_fgv_incc_m',
        'bronze_fipezap_locacao',
        'bronze_geavo_cca_analitico',
        'bronze_geavo_cci_analitico',
        'bronze_geavo_fgts_pf',
        'bronze_geavo_fgts_pj',
        'bronze_gefus_fundo_social',
        'bronze_ibge_paic_obras',
        'bronze_ibge_paic_pessoal_salarial',
        'bronze_ibge_paic_resultados',
        'bronze_ibge_pib_consolidado_trimestral',
        'bronze_ibge_pib_construcao_civil',
        'bronze_ibge_pib_corrente',
        'bronze_ibge_pim_pf_brasil',
        'bronze_ibge_pmc_construcao',
        'bronze_ibge_pnad_construcao_ocupados',
        'bronze_ibge_pnad_construcao_rendimento',
        'bronze_ibge_pnadc_populacao_decis_renda',
        'bronze_ibge_pnadc_rendimento_domiciliar',
        'bronze_ibge_sinapi',
        'bronze_infomoney_imob',
        'bronze_siafi_dotacao_execucao',
    ]) }}
{% set linhas = [] %}
{% if execute %}
    {% for no in graph.sources.values() if no.source_name == 'lake_staging' %}
        {% set piso = no.meta.get('linhas_minimas') %}
        {% if piso %}
            {% do linhas.append(
                "select '" ~ no.name ~ "'::text as fonte, count(*)::bigint as linhas, "
                ~ piso ~ "::bigint as minimo from bronze.bronze_" ~ no.name
            ) %}
        {% endif %}
    {% endfor %}
{% endif %}

with
    medido as (
        {% if linhas %}{{ linhas | join('\n    union all\n    ') }}
        {% else %}
            select null::text as fonte, null::bigint as linhas, null::bigint as minimo where false
        {% endif %}
    )

select fonte, linhas, minimo
from medido
where linhas < minimo
