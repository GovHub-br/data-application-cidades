{{ config(materialized="table") }}

-- GOLD — serie historica mensal do MCMV (pre-2024) para analise preditiva.
--
-- Agrega a silver_mcmv_historico_serie_executiva por
-- (dt_referencia, fonte_familia, nivel_agregacao, regiao_sigla, uf,
-- linha_ogu_fgts): UH contratadas/entregues/concluidas, valores e subsidios.
-- Niveis via GROUPING SETS: 'nacional', 'regiao' e 'uf'. regiao_sigla /
-- regiao_nome (macrorregiao IBGE) vem da silver e sao adicionais ao contrato
-- (colunas ao final; nomes/tipos anteriores preservados).
--
-- IMPORTANTE — natureza_serie PROPAGADA da silver (nao mais 'estoque' fixo):
-- 'estoque' (carteira ACUMULADA no mes-snapshot) p/ bases_relatorio_executivo /
-- min_cidades / bext; 'fluxo' (entrada de novos empreendimentos no mes) p/
-- entrada_bb. Nas linhas de estoque NAO somar entre meses (dupla contagem do
-- acumulado). NUNCA somar entre fonte_familia de grao diferente (grao_familia:
-- 'contrato' p/ bext e min_cidades, 'empreendimento' p/ as demais).
-- bases_relatorio_executivo e min_cidades se sobrepoem no tempo (2014-2016).
-- Para montar UMA serie continua,
-- filtrar por prioridade_familia (menor = preferencial), escolhendo por
-- (dt_referencia, uf) a familia de menor prioridade com dado. A consolidacao
-- fica a cargo do consumidor / de um mart posterior. Guardas: testes
-- soma_nao_cruza_familia e cobertura_classificacao_ogu_fgts.
--
-- VALORES NOMINAIS (R$ da data do fato, 2012-2018) — comparacao plurianual
-- exige deflator externo. Ver docs/glossario-valores-financeiros.md.
--
-- BREAKING (change vocabulario-e-qualidade-financeira-historica): renome
-- valor_investimento/financiamento/liberado -> *_acumulado; colunas novas
-- natureza_serie, grao_familia, valor_vgv, valor_contrapartidas, subsidio_total.
-- valor_emprestimo -> valor_financiamento e valor_contrapartida ->
-- valor_contrapartidas (alinhado ao vocabulario das fichas atuais).
-- Valores monetarios em numeric(15,2) na silver (parse_hist_numeric).
--
-- Alimenta: backtest do relogio, tendencia/sazonalidade/drift, e (via
-- linha_ogu_fgts) a substituicao futura do seed anual do piloto #118.
--
-- Destino conforme o target: `staging_duckdb` materializa no arquivo DuckDB
-- local (modo A, dev), `prod_duckdb` no Postgres atachado (modo C); a
-- publicação a partir do arquivo local é o modo B (./publicar-historico.sh).
-- O corpo é o mesmo nos três — ver models/mcmv_historico_dbt/README.md.
with

    base as (
        select
            dt_referencia,
            year(dt_referencia) as ano,
            month(dt_referencia) as mes,
            fonte_familia,
            case
                fonte_familia
                when 'bases_relatorio_executivo'
                then 1
                when 'min_cidades'
                then 2
                when 'entrada_bb'
                then 3
                when 'bext'
                then 4
                else 9
            end as prioridade_familia,
            coalesce(uf, 'ND') as uf,
            -- regiao_* ja resolvidas na silver (join ao seed dominio_regiao_uf);
            -- 'ND' onde a silver nao tem uf (min_cidades / bext antigos).
            coalesce(regiao_sigla, 'ND') as regiao_sigla,
            coalesce(regiao_nome, 'ND') as regiao_nome,
            coalesce(linha_ogu_fgts, 'Nao classificada') as linha_ogu_fgts,
            -- grao_familia (change vocabulario-e-qualidade-financeira-historica,
            -- D3): 'contrato' (bext) x 'empreendimento' (demais). Guarda contra
            -- somar UH/valor entre graos diferentes — ver o teste
            -- soma_nao_cruza_familia.
            grao_familia,
            -- natureza_serie por familia (change
            -- auditar-grao-serie-executiva-historica): propagada da silver.
            natureza_serie,
            chave_natural,
            uh_contratadas,
            uh_entregues,
            uh_concluidas,
            uh_em_obras,
            uh_comercializadas,
            valor_investimento,
            valor_financiamento,
            valor_vgv,
            valor_contrapartidas,
            valor_liberado,
            subsidio_fgts,
            subsidio_ogu,
            subsidio_total
        from {{ ref("silver_mcmv_historico_serie_executiva") }}
        where dt_referencia is not null
    ),

    agg as (
        select
            dt_referencia,
            ano,
            mes,
            fonte_familia,
            prioridade_familia,
            case
                when grouping(uf) = 0
                then 'uf'
                when grouping(regiao_sigla) = 0
                then 'regiao'
                else 'nacional'
            end as nivel_agregacao,
            case when grouping(uf) = 0 then uf else 'BR' end as uf,
            linha_ogu_fgts,
            count(distinct chave_natural) as n_registros,
            -- cast p/ bigint: sum(bigint) -> HUGEINT (int128) no DuckDB, sem
            -- tipo no Postgres (modo C). Total nacional ~1,5 M cabe em bigint.
            -- Change: verificar-tipagem-silver-gold-historico.
            cast(sum(uh_contratadas) as bigint) as uh_contratadas,
            cast(sum(uh_entregues) as bigint) as uh_entregues,
            cast(sum(uh_concluidas) as bigint) as uh_concluidas,
            cast(sum(uh_em_obras) as bigint) as uh_em_obras,
            -- BREAKING (change vocabulario-e-qualidade-financeira-historica, D1):
            -- valores de estoque ganham o sufixo _acumulado e o desembolso passa
            -- ao nome canonico. Mapa nome-antigo -> nome-novo no schema.yml e em
            -- docs/glossario-valores-financeiros.md.
            sum(valor_investimento) as valor_investimento_acumulado,
            sum(valor_financiamento) as valor_financiamento_acumulado,
            sum(valor_liberado) as valor_desembolsado_acumulado,
            sum(subsidio_fgts) as subsidio_fgts,
            sum(subsidio_ogu) as subsidio_ogu,
            -- colunas novas ao final — contrato de colunas anterior preservado
            -- (exceto os renomes _acumulado acima).
            -- natureza_serie propagada da silver (change
            -- auditar-grao-serie-executiva-historica): 'estoque' p/
            -- bases_relatorio_executivo / min_cidades / bext, 'fluxo' p/
            -- entrada_bb. Uniforme dentro de cada (dt_referencia, fonte_familia).
            max(natureza_serie) as natureza_serie,
            max(grao_familia) as grao_familia,
            sum(valor_vgv) as valor_vgv,
            sum(valor_contrapartidas) as valor_contrapartidas,
            sum(subsidio_total) as subsidio_total,
            case
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                when grouping(uf) = 0
                then max(regiao_sigla)
                else 'BR'
            end as regiao_sigla,
            case
                when grouping(regiao_sigla) = 0 or grouping(uf) = 0
                then max(regiao_nome)
            end as regiao_nome,
            -- uh_comercializadas ao FIM do contrato (change
            -- auditar-grao-serie-executiva-historica, D5): so exposto apos a
            -- correcao de grao da silver. `uh_em_obras` ja estava no contrato
            -- (posicao preservada acima). natureza_serie / "nao somar entre
            -- meses" valem igual as demais colunas de UH.
            cast(sum(uh_comercializadas) as bigint) as uh_comercializadas
        from base
        group by
            grouping sets (
                (
                    dt_referencia,
                    ano,
                    mes,
                    fonte_familia,
                    prioridade_familia,
                    linha_ogu_fgts
                ),
                (
                    dt_referencia,
                    ano,
                    mes,
                    fonte_familia,
                    prioridade_familia,
                    linha_ogu_fgts,
                    regiao_sigla
                ),
                (
                    dt_referencia,
                    ano,
                    mes,
                    fonte_familia,
                    prioridade_familia,
                    linha_ogu_fgts,
                    uf
                )
            )
    )

select *
from agg
order by dt_referencia, fonte_familia, nivel_agregacao, regiao_sigla, uf, linha_ogu_fgts
