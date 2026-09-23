{{ config(materialized="table") }}

-- Prata: Propostas apresentadas ao Novo MCMV FNHIS Sub-50, com o resultado da seleção
-- Fonte: bronze_shpt_fnhis_sub50_propostas (planilha de todas as propostas, portaria MCID
-- nº 673/2024). Grão: uma proposta (`numero_proposta`, único na origem).
--
-- O município vem como "Nome/UF" num campo só; o IBGE vem com 7 dígitos. `cod_ibge_6` existe
-- porque o SNH grava o código SEM o dígito verificador — é a chave para cruzar com o contrato.
--
-- `resultado_selecao` agrupa as nove situações da origem em cinco. As propostas "de risco"
-- selecionadas contam como selecionadas; "Município já contemplado (RISCO)" não.

with
    proposta as (
        select
            nullif(trim(numero_da_proposta::text), '') as numero_proposta,
            nullif(trim(cod_ibge_munic_beneficiado::text), '') as cod_ibge,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '') as municipio_uf_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(proponente::text)), '') as proponente,
            {{ parse_int("total_de_uh::text") }} as quantidade_uh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_da_proposta::text)), '') as situacao_proposta,
            nullif(trim({{ target.schema }}.corrigir_mojibake(justificativa_nao_enquadramento::text)), '') as justificativa_nao_enquadramento,
            nullif(trim(_source_file::text), '') as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_fnhis_sub50_propostas") }}
    )

select
    numero_proposta,
    cod_ibge,
    left(cod_ibge, 6) as cod_ibge_6,
    substring(municipio_uf_origem from '^(.*)/[A-Z]{2}$') as municipio,
    substring(municipio_uf_origem from '/([A-Z]{2})$') as uf,
    case
        when proponente ~* 'municipal' then 'Município'
        when proponente ~* 'estadual|distrito federal' then 'Estado/DF'
        else proponente
    end as proponente_esfera,
    quantidade_uh,
    situacao_proposta,
    case
        when situacao_proposta ~* 'selecionada' then 'Selecionada'
        when situacao_proposta ~* 'j. contemplado' then 'Município já contemplado'
        when situacao_proposta ~* 'n.o enquadrada' then 'Não enquadrada'
        when situacao_proposta ~* 'cota insuficiente' then 'Cota insuficiente da UF'
        else 'Outra'
    end as resultado_selecao,
    situacao_proposta ~* 'selecionada' as ic_selecionada,
    situacao_proposta ~* 'risco' as ic_proposta_de_risco,
    justificativa_nao_enquadramento,
    arquivo_de_origem,
    criado_em
from proposta
