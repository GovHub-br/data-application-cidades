{{ config(materialized="table") }}

-- SILVER do reloginho MCMV (grupo A) — série mensal SNH tratada e deduplicada.
--
-- Lê as BRONZES SNH POR AGENTE (bronze_dhist_empreendimento_snh_bb e
-- _snh_caixa) — desde a change pipeline-bronze-historica-destino-trocavel (D5)
-- não existe mais bronze unificada, e a união é feita aqui, com projeção
-- explícita e idêntica por braço. A view intermediária
-- bronze_reloginho_snh_serie_mensal, que era um passthrough sobre a bronze
-- única, deixou de existir junto com ela. Aplica:
-- * tipagem  — texto -> bigint (UH) / date (datas). As contagens de UH usam
-- coalesce_present_parsed(['uh_contratadas','uhs_contratadas'], 'parse_hist_bigint')
-- porque a SNH BB muda o NOME da coluna por safra: `uhs_*` nos snapshots
-- 2024-06/07/10/11 e 2025-01, `uh_*` a partir de 2025-03. A CAIXA usa sempre
-- `uh_*` e não tem `uhs_*` — daí coalesce_present (introspecção por família) e
-- não um coalesce cru, que quebraria o braço CAIXA. Sem isso ~7,7 k linhas BB
-- (37% do agente) viravam NULL e o ouro_reloginho_indicadores do BB lia 0 UH
-- nesses 5 meses. Mesmo tratamento do prata_dhist_snh_arm (silver por frente).
-- * normalização de domínio — agente_financeiro em maiúsculas (BB/CAIXA) e
-- frente_mcmv canônica a partir de `modalidade` (FAR / Entidades / Rural —
-- resolve o RURAL vs Rural entre CAIXA e BB, decisão #6 do
-- issue-130-resumo-final);
-- * deduplicação — as tabelas `historico_recente_*` trazem cada APF 2x no
-- mesmo snapshot (#130 Fase 3). row_number() por (agente, apf,
-- dt_referencia) mantendo rn = 1 — MESMA chave validada na reconciliação
-- contra a referência #66 (diff 0,000%). Reentregas do mesmo mês
-- (sufixos vsNN / correcao) são desempatadas por prioridade_reentrega.
--
-- Grão de saída: uma linha por (agente_financeiro, apf, dt_referencia), com a
-- frente_mcmv da linha sobrevivente. Alimenta as golds indicadores_reloginho
-- (total por agente) e indicadores_reloginho_frente (quebra por frente).
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`.
{% set snh_familias = familias_snh_empreendimento() %}

with

    tipado as (
        {% for f in snh_familias %}
        select
            -- Usa a coluna da fonte (não o nome do arquivo) para não recuperar
            -- linhas que a versão anterior do gold descartava — preserva a
            -- reconciliação exata contra a referência #66. agente_arquivo fica
            -- disponível na bronze para auditoria.
            upper(nullif(trim(agente_financeiro::text), '')) as agente_financeiro,
            case
                upper(nullif(trim(modalidade::text), ''))
                when 'FAR'
                then 'FAR'
                when 'ENTIDADES'
                then 'Entidades'
                when 'FDS'
                then 'Entidades'
                when 'FDS / ENTIDADES'
                then 'Entidades'
                when 'RURAL'
                then 'Rural'
                when 'PNHR'
                then 'Rural'
                else nullif(trim(modalidade::text), '')
            end as frente_mcmv,
            nullif(trim(apf::text), '') as apf,
            dt_referencia,
            try_cast(
                nullif(trim(data_de_movimento::text), '') as date
            ) as data_de_movimento,
            -- Nome da coluna varia por safra na SNH BB (`uhs_*` até 2025-01,
            -- `uh_*` a partir de 2025-03); CAIXA só tem `uh_*`. coalesce_present
            -- resolve por família em tempo de compilação.
            {{ coalesce_present_parsed(
                ref(f.modelo), ["uh_contratadas", "uhs_contratadas"], "parse_hist_bigint", "bigint"
            ) }} as uh_contratadas,
            {{ coalesce_present_parsed(
                ref(f.modelo), ["uh_entregues", "uhs_entregues"], "parse_hist_bigint", "bigint"
            ) }} as uh_entregues,
            {{ coalesce_present_parsed(
                ref(f.modelo), ["uh_vigentes", "uhs_vigentes"], "parse_hist_bigint", "bigint"
            ) }} as uh_vigentes,
            -- distrato de UH (change enriquecer-quantidades-uh-e-sinais-obra-historico):
            -- só a SNH reporta; 0 é informação, distinto de NULL. Ambos os
            -- agentes trazem a coluna `quantidade_de_uhs_distratadas`.
            try_cast(
                nullif(trim(quantidade_de_uhs_distratadas::text), '') as bigint
            ) as quantidade_uh_distratadas,
            upper(nullif(trim(uf::text), '')) as uf,
            -- strip_float_text: no agente CAIXA ~20% dos códigos IBGE chegam
            -- como "355030.0" (int->float->str a montante). Ver
            -- docs/varredura-sufixo-float-texto.md (change testes-data-quality-dbt).
            {{ strip_float_text('codigo_ibge_do_municipio') }} as codigo_ibge_municipio,
            nullif(trim(municipio::text), '') as municipio,
            nullif(trim(situacao_do_empreendimento::text), '') as status_operacional,
            try_cast(
                nullif(trim(data_de_contratacao::text), '') as date
            ) as dt_contratacao,
            prioridade_reentrega,
            source_file,
            hash_linha
        from {{ ref(f.modelo) }}
        where nullif(trim(apf::text), '') is not null
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by agente_financeiro, apf, dt_referencia
                order by
                    prioridade_reentrega desc, data_de_movimento nulls last, source_file
            ) as rn
        from tipado
        where dt_referencia is not null and agente_financeiro is not null
    ),

    -- situacao_canonica + regiao_* por left join aos seeds de referência
    -- (D1/D3 da change serie-historica-situacao-obra-regiao), depois da dedup —
    -- status_operacional cru e o grão (agente, apf, dt_referencia) intactos.
    -- Os golds do reloginho projetam colunas explícitas e NÃO consomem estas —
    -- saída dos golds inalterada.
    enriquecido_dominio as (
        select
            d.*,
            case
                when nullif(trim(d.status_operacional), '') is null
                then null
                when lower(trim(d.status_operacional)) in ('null', 'nan')
                then null
                when ds.situacao_canonica is not null
                then ds.situacao_canonica
                else 'nao_mapeada'
            end as situacao_canonica,
            dr.regiao_sigla,
            dr.regiao_nome
        from dedup d
        left join {{ ref('dominio_status') }} ds
            on lower(trim(d.status_operacional)) = lower(trim(ds.valor_bruto))
        left join {{ ref('dominio_regiao_uf') }} dr
            on upper(trim(d.uf)) = upper(trim(dr.uf))
    )

select
    agente_financeiro,
    frente_mcmv,
    apf,
    dt_referencia,
    uh_contratadas,
    uh_entregues,
    uh_vigentes,
    quantidade_uh_distratadas,
    uf,
    codigo_ibge_municipio,
    municipio,
    status_operacional,
    dt_contratacao,
    data_de_movimento,
    source_file,
    hash_linha,
    situacao_canonica,
    regiao_sigla,
    regiao_nome
from enriquecido_dominio
where rn = 1
