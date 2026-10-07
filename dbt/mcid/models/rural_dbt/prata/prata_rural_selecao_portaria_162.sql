{{ config(materialized="table") }}

-- Prata: Empreendimentos do Rural selecionados pela Portaria MCidades nº 162/2018.
-- Fonte: bronze_sftp_portaria_162_2018_rural. Grão: empreendimento selecionado.
-- `apf` é o código do empreendimento normalizado como APF, para casar com o PNHR/SNH.

select
    {{ target.schema }}.normalize_apf(codigo_do_empreendimento::text) as apf,
    nullif(trim(codigo_do_empreendimento::text), '') as codigo_empreendimento,
    nullif(regexp_replace(cnpj_entidade::text, '[^0-9]', '', 'g'), '') as entidade_organizadora_cnpj,
    nullif(trim(uf::text), '') as uf,
    upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '')) as municipio,
    initcap(nullif(trim(regiao::text), '')) as regiao,
    {{ parse_int('qtde_de_uh::text') }} as quantidade_uh_selecionadas,
    date '2018-02-27' as dt_portaria,
    'Portaria MCidades nº 162/2018' as ato_selecao,
    regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
from {{ ref("bronze_sftp_portaria_162_2018_rural") }}
where nullif(trim(codigo_do_empreendimento::text), '') is not null
