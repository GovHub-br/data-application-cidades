{{ config(materialized='table') }}

with empreendimentos as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_tab_empreendimentos') }}
),
construtor as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_tab_empreendimentos_construtor') }}
),
sftp_pj as (
    select * from {{ source('fgts', 'fgts_sftp_empreendimentos_base_pj') }}
),
gps as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_tab_empreendimentos_gps') }}
),
municipios as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_municipios') }}
)

select
    e.cod_empreendimento::varchar,
    coalesce(e.txt_nome_empreendimento, 'Não Informado')::varchar as nome_empreendimento,
    e.txt_objeto::varchar as objeto,
    e.txt_localidade::varchar as localidade,
    e.txt_logradouro::varchar as logradouro,
    
    -- Enriquecimento construtor / SFTP PJ
    coalesce(c.cgc, sp.cgc, 'Não Informado')::varchar as cnpj_construtora,
    coalesce(c.entidade, sp.rz_social, 'Não Informado')::varchar as nome_construtora,
    
    -- Localização
    e.cod_municipio::varchar,
    coalesce(m.municipio, 'Não Informado')::varchar as municipio_nome,
    coalesce(m.uf, 'ND')::varchar as municipio_uf,
    
    -- Coordenadas GPS (mantendo varchar por conta de caracteres como °, ', '')
    coalesce(nullif(sp.latitude, 'None'), max(case when g.tipo_da_caracteristica = 'GPS LATITUDE GRAU' then g.texto end))::varchar as gps_lat,
    coalesce(nullif(sp.longitude, 'None'), max(case when g.tipo_da_caracteristica = 'GPS LONGITUDE GRAU' then g.texto end))::varchar as gps_long,
    
    -- Valores (usando macros de parse padrão - macro retorna inteiro)
    coalesce({{ parse_int('e.qtd_unidades_financiadas') }}, 0) as quantidade_uh
    
from empreendimentos e
left join municipios m on e.cod_municipio = m.codigo
left join construtor c on e.cod_empreendimento = c.cod_empreendimento
left join gps g on e.cod_empreendimento = g.empreendimento_codigo
left join sftp_pj sp on right(sp.nu_apf, 7) = e.cod_empreendimento
group by
    e.cod_empreendimento, e.txt_nome_empreendimento, e.txt_objeto, e.txt_localidade, e.txt_logradouro,
    c.cgc, c.entidade, sp.cgc, sp.rz_social, e.cod_municipio, m.municipio, m.uf, sp.latitude, sp.longitude, e.qtd_unidades_financiadas
