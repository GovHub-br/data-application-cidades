{{ config(materialized="table") }}

-- BRONZE — série histórica mensal de empreendimentos MCMV a partir das
-- interfaces do SFTP (GEFUS), cópia fiel.
--
-- Empilha, SEM regra de negócio e SEM deduplicação, os snapshots mensais das
-- cinco interfaces de empreendimento lidas de staging/sftp/fabrica/GEFUS/ via
-- MinIO/DuckDB:
--
-- INT040  FAR CAIXA          nu_apf
-- INT054  FAR BB             nu_apf
-- INT059  FDS / Entidades    nu_apf
-- INT057  PNHR / Rural BB    nu_contrato_empreendimento
-- INT065  PNHR / Rural CAIXA nu_apf
--
-- Responsabilidade desta camada (ver models/docs/arquitetura-medalhao-mcid.md):
-- * uma linha por linha de origem (sem dedup);
-- * colunas da fonte preservadas como vieram (union_by_name entre as 5
-- interfaces — a tabela fica larga e esparsa, esperado para bronze);
-- * sem tipagem (a coerção fica na silver);
-- * dt_referencia derivada do NOME DO ARQUIVO (mais confiável que dt_movimento,
-- ver docs/entregas/issue-130-pendencias-encoding-canonicalizacao-sftp-minio.md);
-- * auditoria: source_file, fonte_interface, dt_ingest, hash_linha.
--
-- Arquivos de reentrega (sufixo != _YYYYMMDD) e de VALIDACAO são excluídos para
-- não duplicar APF × mês — mesma regra da silver anterior. A separação por
-- frente é responsabilidade da silver (o discriminador é a interface).
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
{% set interfaces = [
    ('INT040', 'INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS'),
    ('INT054', 'INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS'),
    ('INT059', 'INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS'),
    ('INT057', 'INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS'),
    ('INT065', 'INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS'),
] %}

with

    {% for code, fonte in interfaces %}
        {{ code | lower }} as (
            select
                *,
                '{{ fonte }}' as fonte_interface,
                filename as source_file,
                strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date
                as dt_referencia
            from
                {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/' ~ code ~ '_*.parquet') }}
            where
                regexp_matches(filename, '_\d{8}\.parquet$')
                and filename not ilike '%validacao%'
        ){{ "," if not loop.last }}
    {% endfor %},
    unido as (
        {% for code, fonte in interfaces %}
            select * from {{ code | lower }} {{ "union all by name" if not loop.last }}
        {% endfor %}
    )

select
    *,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|',
            source_file,
            cast(row_number() over (partition by source_file) as varchar)
        )
    ) as hash_linha
from unido
