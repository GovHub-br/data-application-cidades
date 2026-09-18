{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: TODOS os dados manuais TRIMESTRAIS do boletim,
-- da planilha oficial (boletim.xlsx, aba "Dados Trimestrais"), carregada em
-- manual_conjuntura.dados_trimestrais. Cobre PIB %, CBIC, balanço das empresas,
-- ocupados PNAD, INCC trimestral, ticket médio e financiamentos habitacionais.
-- A camada gold seleciona as séries por seção do boletim.
select *
from manual_conjuntura.dados_trimestrais
where periodo is not null
