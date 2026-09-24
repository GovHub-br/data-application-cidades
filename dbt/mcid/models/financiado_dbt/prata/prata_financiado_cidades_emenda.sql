{{ config(materialized="table") }}

-- Prata: MCMV Cidades — contratos PF vinculados a emenda parlamentar.
-- Fonte: bronze_sftp_mcmv_cidades_emendas
-- Grão: uma linha por `numero_contrato` (contrato PF).
--
-- MCMV Cidades NÃO é uma linha de crédito à parte: toda linha do arquivo carrega
-- `linhadecredito` = "Apoio à Produção - PF - PMCMV" ou "- Parcerias". É uma camada de
-- aporte complementar montada em cima da linha financiada, e é por isso que este
-- modelo vive no mesmo pacote em vez de num `financiado_cidades_dbt` separado.
--
-- Datas aqui chegam em ISO (`2022-12-19 00:00:00`), e não no formato americano do
-- resto do pacote — o arquivo é gerado por outra rotina.
select
    trim(numerodocontrato) as numero_contrato,
    nullif(trim(operacao), '') as cod_operacao,
    nullif(trim(numerooperacao), '') as cod_operacao_alternativo,
    nullif(trim(emenda), '') as emenda,
    nullif(trim(linhadecredito), '') as linha_credito,
    nullif(trim(programa), '') as programa,
    nullif(trim(subprograma), '') as subprograma,
    nullif(trim(faixaprograma), '') as faixa_programa,
    nullif(trim(caracteristica), '') as cod_caracteristica,
    nullif(trim(modalidade), '') as modalidade,
    nullif(trim(tomadornome), '') as tomador_nome,
    nullif(trim(tomadorcodigo), '') as cod_tomador,
    nullif(trim(municipiocodigo), '') as cod_municipio_caixa,
    nullif(trim(municipionome), '') as municipio,
    nullif(trim(ufsigla), '') as uf,
    nullif(trim(regiaonome), '') as regiao,

    {{ parse_numeric("vlrdofinanciamentobruto") }} as vr_financiamento_bruto,
    {{ parse_numeric("vlrdofinanciamentoliquido") }} as vr_financiamento_liquido,
    {{ parse_numeric("vlrdorecursodofgts") }} as vr_recurso_fgts,
    {{ parse_numeric("vlrdodesconto") }} as vr_desconto,
    {{ parse_numeric("vlrdodescontoogu") }} as vr_desconto_ogu,

    -- Recurso próprio do MUTUÁRIO. Não é contrapartida do ente federado, e somar os
    -- dois seria dizer que a prefeitura pôs o que a família pôs.
    {{ parse_numeric("vlrcapitalproprio") }} as vr_capital_proprio,

    {{ parse_numeric("vlrcontrapartidaparceria") }} as vr_contrapartida_parceria,
    {{ parse_int("prazoemmeses") }} as prazo_meses,
    {{ parse_int("anodoorcamento") }} as ano_orcamento,
    nullif(trim(anomesdoorcamento), '') as competencia_orcamento,
    {{ parse_data_iso("datadacontratacao") }} as dt_contratacao,

    coalesce({{ parse_numeric("vlrcontrapartidaparceria") }}, 0) > 0
    as ic_tem_contrapartida_financeira,

    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_sftp_mcmv_cidades_emendas") }}
