{{ config(materialized="table") }}

-- Bronze: Termos de compromisso do Novo MCMV FNHIS Sub-50 no TransfereGov: uma linha por proposta
-- selecionada, com situação da contratação e do instrumento, valores de repasse, contrapartida e
-- empenho. Retrato mensal; a tabela guarda o mais recente.
-- Fonte: SHPT — sharepoint/Novo MCMV - FNHIS Sub 50/ (painel mensal extraído do TransfereGov)
--
-- Exceção ao padrão das outras bronzes, e não por gosto: algumas remessas do painel chegaram
-- com as colunas de ANEXO do e-mail que as trouxe (`odata_type_*`, `id_aamk...`, `size_*`), várias
-- com o MESMO nome. Ler o glob com `union_by_name` e projetar colunas dele primeiro fez o
-- `create table` recusar nome repetido; depois, com a projeção explícita, derrubou a conexão com
-- o servidor duas vezes seguidas.
--
-- Por isso o arquivo vencedor é resolvido ANTES, na compilação (a mesma consulta de
-- `arquivo_mais_recente`, que só lista nomes de arquivo e roda limpa), e a tabela lê só ele, sem
-- glob e sem união. O layout é o do arquivo corrente; o teste `sem_drift_de_colunas` avisa se
-- ele mudar.
{% set padrao = "s3://data-lake-mcid/staging/**/FNHIS SUB 50 Painel_TG_*.parquet" %}
{%- set arquivo = "s3://data-lake-mcid/__parse__" -%}
{%- if execute -%}
    {%- set resultado = run_query("select " ~ arquivo_mais_recente(padrao) ~ " as arquivo") -%}
    {%- set arquivo = resultado.columns[0].values()[0] -%}
    {%- if not arquivo -%}
        {{ exceptions.raise_compiler_error("nenhum arquivo casou com " ~ padrao) }}
    {%- endif -%}
{%- endif %}

select *
from read_parquet('{{ arquivo }}', filename => true) as r
