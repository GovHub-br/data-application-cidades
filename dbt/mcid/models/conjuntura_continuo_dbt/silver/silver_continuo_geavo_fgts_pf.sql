{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: FGTS-PF por faixa MCMV e por condição de
-- uso (novo/usado), via sistema GEAVO da Caixa. Páginas 4 e 5.
-- Ver docs/conjuntura-fontes-dbt.md (local, fora do repositório).
--
-- Diferente do FGTS-PJ, aqui não é fluxo de caixa (desembolso) — é
-- contagem/valor de unidades na assinatura do contrato, então usa
-- Base_PF_FGTS direto (não precisa da tabela de desembolso). Granularidade
-- mensal, igual o gold antigo (manual) que está sendo substituído.
--
-- Categorias: `faixa` vem G1/G2/G3 no arquivo — mapeado pra "Faixa 1/2/3"
-- (leitura mais óbvia do campo, ainda não 100% confirmada contra uma
-- tabela de domínio oficial). "Classe Média" vem de `tp_orcamento`
-- (faixa fica em branco pra esse programa). Todo o resto com faixa em
-- branco é financiamento comum fora do MCMV (ex.: Carta de Crédito
-- Individual "normal", ~2,3 milhões de contratos) — **excluído** daqui
-- porque não é o recorte "por faixa MCMV" que a Página 4 pede; incluir
-- inflaria a categoria "fora MCMV" pra um tamanho sem sentido comparado
-- às faixas.
--
-- ATENÇÃO: o nome do arquivo muda todo mês (Base_PF_FGTS_{data}.parquet).
-- Aponta pro mais recente conhecido em 2026-08-25 (07/07/2026) — precisa
-- atualizar o caminho manualmente até existir uma DAG que copie sempre o
-- arquivo mais novo pra um caminho fixo.

with base as (
    select
        (r['dt_assinatura']::text)::date as dt_assinatura,
        case
            when r['faixa']::text = 'G1' then 'Faixa 1'
            when r['faixa']::text = 'G2' then 'Faixa 2'
            when r['faixa']::text = 'G3' then 'Faixa 3'
            when r['tp_orcamento']::text ilike '%classe media%' then 'Classe Média'
        end as faixa,
        case lower(r['tpimovel']::text)
            when 'novo' then 'Novo'
            when 'usado' then 'Usado'
            else r['tpimovel']::text
        end as tipo_imovel,
        replace(r['vlr_emprestimo']::text, ',', '.')::numeric as valor
    from read_parquet('s3://data-lake-mcid/staging/sftp/caixa.geavo/GEAVO/Base_PF_FGTS_20260707.parquet') as r
)

select
    extract(year from dt_assinatura)::int  as ano,
    extract(month from dt_assinatura)::int as mes,
    faixa,
    tipo_imovel,
    count(*)                as qtd_unidades,
    sum(valor)               as valor_emprestimo,
    current_timestamp        as dt_silver
from base
where faixa is not null
group by 1, 2, 3, 4
