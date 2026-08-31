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
-- Categorias: `faixa` vem G1/G2/G3 no arquivo — mapeado pra "Faixa 1/2/3".
-- CONFIRMADO em 2026-08-27 (empírico, não achamos tabela de domínio
-- oficial no MinIO/Postgres pra G1/G2/G3 especificamente): batendo
-- `vlr_renda_familiar_comprovada` (também no arquivo, mas não usada aqui)
-- por valor de `faixa`, G1/G2/G3 têm faixas de renda limpas e batendo com
-- o que se espera do MCMV — G1 até R$2.400, G2 R$2.000-4.400, G3
-- R$4.000-8.000. O arquivo também tem valores antigos de `faixa`
-- ('1_5'/'2'/'3', sem "G") — são nomenclatura anterior, usada só até
-- 2020-08-25 (a virada pro sistema "G" é exata, 2020-08-26); como esse
-- model só processa dt_assinatura recente, esses códigos nunca aparecem
-- aqui e não precisam de tratamento. "Classe Média" vem de `tp_orcamento`
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
        dt_assinatura::text::date as dt_assinatura,
        case
            when faixa::text = 'G1' then 'Faixa 1'
            when faixa::text = 'G2' then 'Faixa 2'
            when faixa::text = 'G3' then 'Faixa 3'
            when tp_orcamento::text ilike '%classe media%' then 'Classe Média'
        end as faixa,
        case lower(tpimovel::text)
            when 'novo' then 'Novo'
            when 'usado' then 'Usado'
            else tpimovel::text
        end as tipo_imovel,
        replace(vlr_emprestimo::text, ',', '.')::numeric as valor
    from {{ ref('bronze_continuo_geavo_fgts_pf') }}
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
