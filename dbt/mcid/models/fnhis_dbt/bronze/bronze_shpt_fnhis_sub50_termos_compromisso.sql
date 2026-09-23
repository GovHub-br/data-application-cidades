{{ config(materialized="table") }}

-- Bronze: Termos de compromisso do Novo MCMV FNHIS Sub-50 no TransfereGov: uma linha por proposta
-- selecionada, com situação da contratação e do instrumento, valores de repasse, contrapartida e
-- empenho. Retrato mensal; a tabela guarda o mais recente.
-- Fonte: SHPT — sharepoint/Novo MCMV - FNHIS Sub 50/ (painel mensal extraído do TransfereGov)
--
-- Exceção ao `select *` das outras bronzes, e não por gosto: algumas remessas do painel chegaram
-- com as colunas de ANEXO do e-mail que as trouxe (`odata_type_*`, `id_aamk...`, `size_*`), e o
-- `union_by_name` do glob as trazia para a tabela. Duas delas têm o mesmo nome depois que o
-- Postgres normaliza, e o `create table` falhava com "column specified more than once". A lista
-- abaixo é o layout do painel; o teste `sem_drift_de_colunas` continua avisando se ele mudar.
{% set padrao = "s3://data-lake-mcid/staging/**/FNHIS SUB 50 Painel_TG_*.parquet" %}

select
    cast(r['data_consulta'] as varchar) as data_consulta,
    cast(r['codigo_programa'] as varchar) as codigo_programa,
    cast(r['no_proposta'] as varchar) as no_proposta,
    cast(r['sit_contratacao'] as varchar) as sit_contratacao,
    cast(r['situacao_proposta'] as varchar) as situacao_proposta,
    cast(r['modalidade'] as varchar) as modalidade,
    cast(r['vl_repasse_proposta'] as varchar) as vl_repasse_proposta,
    cast(r['vl_empenhado_pre_convenio'] as varchar) as vl_empenhado_pre_convenio,
    cast(r['valor_de_repasse'] as varchar) as valor_de_repasse,
    cast(r['valor_de_contrapartida'] as varchar) as valor_de_contrapartida,
    cast(r['valor_empenhado_acumulado'] as varchar) as valor_empenhado_acumulado,
    cast(r['objeto'] as varchar) as objeto,
    cast(r['uf'] as varchar) as uf,
    cast(r['municipio'] as varchar) as municipio,
    cast(r['regiao'] as varchar) as regiao,
    cast(r['nome_proponente'] as varchar) as nome_proponente,
    cast(r['situacao_instrumento'] as varchar) as situacao_instrumento,
    cast(r['data_assinatura'] as varchar) as data_assinatura,
    cast(r['cnpj'] as varchar) as cnpj,
    cast(r['no_reservado_pac'] as varchar) as no_reservado_pac,
    cast(r['_source_file'] as varchar) as _source_file,
    cast(r['_ingested_at'] as varchar) as _ingested_at,
    cast(r['_source_hash'] as varchar) as _source_hash,
    cast(r['filename'] as varchar) as filename
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
