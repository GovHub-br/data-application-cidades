{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: TODOS os dados manuais TRIMESTRAIS do boletim,
-- da planilha oficial (boletim.xlsx, aba "Dados Trimestrais"), carregada em
-- conjuntura.bnz_manual_dados_trimestrais. Cobre PIB %, CBIC, balanço das empresas,
-- ocupados PNAD, INCC trimestral, ticket médio e financiamentos habitacionais.
-- A camada gold seleciona as séries por seção do boletim.

-- `unnamed_115` e `unnamed_116` são colunas residuais da importação da
-- planilha: 100% vazias e fora da convenção de nomes. Duas checagens
-- independentes as apontaram (perfil de completude e teste de padronização),
-- então ficam de fora aqui em vez de vazarem para o gold.
-- A dimensão temporal é derivada aqui, uma vez, e não em cada gold: a origem
-- traz `ano` como `double precision` e `trimestre` como o texto '2T', o que
-- obrigava cada consumidor a reparar tempo por conta própria. Conferido em
-- 2026-08-30: as 19 linhas casam com '^[1-4]T[0-9]{4}$'.
select
    {{ dimensao_temporal_do_periodo('periodo') }},
    {{ colunas_exceto('manual_conjuntura', 'dados_trimestrais',
                      ['unnamed_115', 'unnamed_116',
                       'ano', 'trimestre', 'periodo']) }}
from conjuntura.bnz_manual_dados_trimestrais
where periodo is not null
