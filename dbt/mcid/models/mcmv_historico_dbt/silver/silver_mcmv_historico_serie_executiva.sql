{{ config(materialized="table") }}

-- SILVER — serie executiva historica do MCMV (pre-2024), contrato comum.
--
-- Le as 4 BRONZES POR FAMILIA (D5 da change
-- pipeline-bronze-historica-destino-trocavel) e as une aqui, com projecao
-- explicita por braco: um laco Jinja sobre o mapa de familias monta um CTE por
-- familia com a MESMA lista de colunas, e o `union all` empilha os quatro. E
-- aqui — nao mais na bronze — que o mapa de colunas (doc
-- issue-130-proposta-bronze-series-historicas.md) e aplicado, via
-- coalesce_present(), que so referencia as colunas existentes de fato NAQUELA
-- familia (cada uma tem 2-3 geracoes de schema).
--
-- Nao ha `union all by name` nem `select * exclude`: todo o SQL desta camada e
-- Postgres-valido, o que mantem aberta a opcao de roda-la com dbt-postgres.
--
-- ORDEM DE BUILD: coalesce_present() introspecciona a relacao no banco em tempo
-- de COMPILE — as 4 bronzes precisam existir quando esta silver e compilada.
-- Ver models/mcmv_historico_dbt/README.md.
--
-- Grao de saida: 1 registro de origem (empreendimento ou contrato), tipado e
-- deduplicado por (fonte_familia, chave_natural, dt_referencia) mantendo o
-- snapshot mais recente do mes. dt_referencia = mes-snapshot.
--
-- `linha_ogu_fgts` classifica o registro para a serie do piloto #118:
-- OGU/Subsidiado quando o subsidio OGU domina; FGTS/Financiado quando o FGTS
-- domina; os valores brutos ficam expostos para o gold somar os dois.
{% set familias = familias_serie_executiva() %}

with

{% for f in familias %}
    {%- set b = ref(f.modelo) -%}
    familia_{{ f.nome }} as (
        select
            'Minha Casa Minha Vida' as programa,
            '{{ f.nome }}' as fonte_familia,
            source_file,
            dt_referencia,
            report_date_parsed,
            hash_linha,

            case
                when
                    lower(source_file) like 'caixa%' or lower(source_file) like '%_caixa%'
                then 'CAIXA'
                when lower(source_file) like 'bb%' or lower(source_file) like '%_bb%'
                then 'Banco do Brasil'
            end as agente_financeiro,

            case when lower(source_file) like '%pnhr%' then 'Rural' end as frente_hint,

            {{ coalesce_present(b, [
            'cod_apf','codapf','cod_empreendimento','icodigo_empreendimento',
            'codigo_empreendimento_bb','cod_contrato','nr_prpt','contrato_bb','contrato_caixa'
        ], 'varchar') }}
            as chave_natural,

            {{ coalesce_present(b, ['uf','csigla_uf'], 'varchar') }} as uf_raw,
            {{ coalesce_present(b, [
            'cod_munic_ibge','codmunicibge','cod_municipio','codigo_do_ibge',
            'icodigo_municipio_ibge_sem_dv'
        ], 'varchar') }} as codigo_ibge_raw,
            {{ coalesce_present(b, ['municipio','vnome_municipio'], 'varchar') }}
            as municipio_raw,
            {{ coalesce_present(b, ['faixa','cfaixa','num_faixa','faixa_divisao'], 'varchar') }}
            as faixa_raw,
            {{ coalesce_present(b, [
            'produto','vnome_produto','iprograma_mcmv','num_programa','fase_mcmv','fase_do_pmcmv'
        ], 'varchar') }}
            as produto_raw,
            {{ coalesce_present(b, [
            'vnome_empreendimento','nomeempreendimento','dsc_empreendimento','empreendimento'
        ], 'varchar') }}
            as nome_empreendimento,
            {{ coalesce_present(b, ['vnome_construtora','construtora','nom_proponente','empresa'], 'varchar') }}
            as responsavel_nome,
            {{ coalesce_present(b, ['inumero_cnpj','cnpj','cod_cnpj_proponente'], 'varchar') }}
            as responsavel_id,

            {{ coalesce_present(b, [
            'uh','unidades','iqde_uh','qtd_unidade_habitacional','qtd_uh','qde_unidades'
        ], 'varchar') }} as uh_contratadas_raw,
            {{ coalesce_present(b, [
            'iqde_unidades_entregues','unidades_entregues','iqde_uh_entregues',
            'qtd_unidade_entregue','qtd_entregue','entregues'
        ], 'varchar') }} as uh_entregues_raw,
            {{ coalesce_present(b, [
            'uh_concluidas','unidades_concluidas','iqde_uh_concluidas',
            'qtd_unidade_concluida','qtd_concluida','uh_concluidos'
        ], 'varchar') }} as uh_concluidas_raw,
            {{ coalesce_present(b, ['uh_em_obras','unidades_em_obras','iqde_uh_em_obras'], 'varchar') }}
            as uh_em_obras_raw,
            {{ coalesce_present(b, ['uh_comercializadas','comercializadas','qtd_comercializadas'], 'varchar') }}
            as uh_comercializadas_raw,

            {{ coalesce_present(b, [
            'valor_total_do_investimento','mvalor_investimento','vlr_total_operacao','vlr_total_investimento'
        ], 'varchar') }}
            as valor_investimento_raw,
            -- valor_financiamento = operacao de credito de fato (nome alinhado
            -- ao vocabulario das fichas atuais: valor_financiamento_fds / valor_far).
            -- VGV saiu daqui (change vocabulario-e-qualidade-financeira-historica,
            -- D2): a familia entrada_bb so tem valor_global_de_venda_vgv e caia
            -- 100% como emprestimo. Agora vai para valor_vgv_raw abaixo.
            {{ coalesce_present(b, [
            'valor_do_emprestimo','mvalor_emprestimo','vlr_emprestimo','vlr_financiamento',
            'mvalor_financiamento','total_financiamentos_pf'
        ], 'varchar') }}
            as valor_financiamento_raw,
            {{ coalesce_present(b, ['valor_global_de_venda_vgv'], 'varchar') }}
            as valor_vgv_raw,
            -- contrapartidas (poder publico / estado / municipio / entidade) —
            -- antes descartada. bext.mvalor_contrapartida_poder_publico e 100%
            -- preenchida (D2). Nome no plural = vocabulario das fichas atuais.
            {{ coalesce_present(b, [
            'mvalor_contrapartida_poder_publico','valor_contrapartida_poder_publico',
            'valor_da_contrapartida_do_poder_publico','vlr_contrapartida'
        ], 'varchar') }}
            as valor_contrapartidas_raw,
            {{ coalesce_present(b, ['valor_total_liberado','mvalor_desembolso'], 'varchar') }}
            as valor_liberado_raw,
            {{ coalesce_present(b, [
            'subsidio_fgts','siaci_valorsubsidio_fgts','vlr_subsidio_fgts','complemento_fgts'
        ], 'varchar') }}
            as subsidio_fgts_raw,
            {{ coalesce_present(b, [
            'subsidio_ogu','siaci_valorsubsidio_ogu','vlr_subsidio_ogu','complemento_ogu'
        ], 'varchar') }} as subsidio_ogu_raw,
            {{ coalesce_present(b, ['mvalor_subsidio'], 'varchar') }} as subsidio_total_raw,

            {{ coalesce_present(b, [
            'obra_executada','de_obra_executada','prc_execucao_obra','percentual_de_obra',
            'vfaixa_perc_obra','obra'
        ], 'varchar') }} as pct_execucao_fisica_raw,

            {{ coalesce_present(b, ['data_contratacao','dat_contratacao','data_da_contratacao_bb'], 'varchar') }}
            as dt_contratacao_raw,
            {{ coalesce_present(b, ['data_conclusao','dat_entregue','entrega_do_empreendimento'], 'varchar') }}
            as dt_entrega_raw,
            {{ coalesce_present(b, [
            'data_prevista_termino_obra','data_prevista_termino_obras','dat_prevista_termino',
            'cronograma_datatermino','dataprevistasr'
        ], 'varchar') }}
            as dt_previsao_termino_raw
        from {{ b }}
    ),
{% endfor %}

    mapeado as (
        {% for f in familias %}
        select
            programa,
            fonte_familia,
            source_file,
            dt_referencia,
            report_date_parsed,
            hash_linha,
            agente_financeiro,
            frente_hint,
            chave_natural,
            uf_raw,
            codigo_ibge_raw,
            municipio_raw,
            faixa_raw,
            produto_raw,
            nome_empreendimento,
            responsavel_nome,
            responsavel_id,
            uh_contratadas_raw,
            uh_entregues_raw,
            uh_concluidas_raw,
            uh_em_obras_raw,
            uh_comercializadas_raw,
            valor_investimento_raw,
            valor_financiamento_raw,
            valor_vgv_raw,
            valor_contrapartidas_raw,
            valor_liberado_raw,
            subsidio_fgts_raw,
            subsidio_ogu_raw,
            subsidio_total_raw,
            pct_execucao_fisica_raw,
            dt_contratacao_raw,
            dt_entrega_raw,
            dt_previsao_termino_raw
        from familia_{{ f.nome }}
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    tipado as (
        select
            programa,
            fonte_familia,
            coalesce(frente_hint, 'Nao classificada') as frente_mcmv,
            agente_financeiro,
            -- strip_float_text: os identificadores desta série chegam da fonte
            -- como texto com sufixo ".0" (int->float->str a montante). Sem a
            -- limpeza, `chave_natural`/`responsavel_id` não casam em join e o
            -- `regexp_replace(\D)` do código IBGE transformaria "353470.0" em
            -- "3534700". Ver docs/varredura-sufixo-float-texto.md.
            {{ strip_float_text('chave_natural') }} as chave_natural,
            upper(nullif(trim(cast(uf_raw as varchar)), '')) as uf,
            regexp_replace(
                {{ strip_float_text('codigo_ibge_raw') }}, '\D', '', 'g'
            ) as codigo_ibge_municipio,
            nullif(trim(cast(municipio_raw as varchar)), '') as municipio,
            lower(nullif(trim(cast(faixa_raw as varchar)), '')) as faixa,
            nullif(trim(cast(produto_raw as varchar)), '') as produto,
            nullif(trim(cast(nome_empreendimento as varchar)), '') as nome_empreendimento,
            nullif(trim(cast(responsavel_nome as varchar)), '') as responsavel_nome,
            {{ strip_float_text('responsavel_id') }} as responsavel_id,

            {{ parse_hist_bigint('uh_contratadas_raw') }} as uh_contratadas,
            {{ parse_hist_bigint('uh_entregues_raw') }} as uh_entregues,
            {{ parse_hist_bigint('uh_concluidas_raw') }} as uh_concluidas,
            {{ parse_hist_bigint('uh_em_obras_raw') }} as uh_em_obras,
            {{ parse_hist_bigint('uh_comercializadas_raw') }} as uh_comercializadas,

            -- Valores monetarios em numeric(15,2) (parse_hist_numeric), alinhado
            -- ao tipo das fichas atuais (parse_hist_numeric / parse_financial_value).
            -- NULL preservado p/ ausencia (nao 0.00) — ver glossario §. Change:
            -- vocabulario-e-qualidade-financeira-historica.
            {{ parse_hist_numeric('valor_investimento_raw') }} as valor_investimento,
            {{ parse_hist_numeric('valor_financiamento_raw') }} as valor_financiamento,
            {{ parse_hist_numeric('valor_vgv_raw') }} as valor_vgv,
            {{ parse_hist_numeric('valor_contrapartidas_raw') }} as valor_contrapartidas,
            {{ parse_hist_numeric('valor_liberado_raw') }} as valor_liberado,
            {{ parse_hist_numeric('subsidio_fgts_raw') }} as subsidio_fgts,
            {{ parse_hist_numeric('subsidio_ogu_raw') }} as subsidio_ogu,
            {{ parse_hist_numeric('subsidio_total_raw') }} as subsidio_total,
            {{ parse_hist_double('pct_execucao_fisica_raw') }}
            as percentual_execucao_fisica,

            {{ parse_hist_date('dt_contratacao_raw') }} as dt_contratacao,
            {{ parse_hist_date('dt_entrega_raw') }} as dt_entrega,
            {{ parse_hist_date('dt_previsao_termino_raw') }} as dt_previsao_termino,

            dt_referencia,
            report_date_parsed,
            source_file,
            hash_linha
        from mapeado
    ),

    classificado as (
        select
            *,
            case
                when
                    coalesce(subsidio_ogu, 0) > 0
                    and coalesce(subsidio_ogu, 0) >= coalesce(subsidio_fgts, 0)
                then 'OGU/Subsidiado'
                when coalesce(subsidio_fgts, 0) > 0
                then 'FGTS/Financiado'
            end as linha_ogu_fgts,

            -- grao_familia (change vocabulario-e-qualidade-financeira-historica,
            -- D3): bext e extrato de contrato PF individual; as demais familias
            -- sao por empreendimento. Impede o consumidor de somar UH/valor
            -- entre graos diferentes no gold_serie_mensal.
            case fonte_familia when 'bext' then 'contrato' else 'empreendimento' end
            as grao_familia,

            -- Fallback de dedup por conteudo de negocio quando chave_natural e
            -- nulo. Substitui hash_linha (que inclui row_number() da bronze e
            -- por isso nunca colide, nem entre linhas identicas) por um hash
            -- das proprias colunas ja tipadas aqui, com marcador NULL-safe
            -- (concat_ws ignora NULL silenciosamente e colidiria NULL com
            -- string vazia sem isso).
            md5(
                concat_ws(
                    '|',
                    coalesce(uf, '␀NULL␀'),
                    coalesce(codigo_ibge_municipio, '␀NULL␀'),
                    coalesce(municipio, '␀NULL␀'),
                    coalesce(faixa, '␀NULL␀'),
                    coalesce(produto, '␀NULL␀'),
                    coalesce(nome_empreendimento, '␀NULL␀'),
                    coalesce(responsavel_nome, '␀NULL␀'),
                    coalesce(responsavel_id, '␀NULL␀'),
                    coalesce(cast(uh_contratadas as varchar), '␀NULL␀'),
                    coalesce(cast(uh_entregues as varchar), '␀NULL␀'),
                    coalesce(cast(uh_concluidas as varchar), '␀NULL␀'),
                    coalesce(cast(uh_em_obras as varchar), '␀NULL␀'),
                    coalesce(cast(uh_comercializadas as varchar), '␀NULL␀'),
                    coalesce(cast(valor_investimento as varchar), '␀NULL␀'),
                    coalesce(cast(valor_financiamento as varchar), '␀NULL␀'),
                    coalesce(cast(valor_vgv as varchar), '␀NULL␀'),
                    coalesce(cast(valor_contrapartidas as varchar), '␀NULL␀'),
                    coalesce(cast(valor_liberado as varchar), '␀NULL␀'),
                    coalesce(cast(subsidio_fgts as varchar), '␀NULL␀'),
                    coalesce(cast(subsidio_ogu as varchar), '␀NULL␀'),
                    coalesce(cast(subsidio_total as varchar), '␀NULL␀'),
                    coalesce(cast(percentual_execucao_fisica as varchar), '␀NULL␀'),
                    coalesce(cast(dt_contratacao as varchar), '␀NULL␀'),
                    coalesce(cast(dt_entrega as varchar), '␀NULL␀'),
                    coalesce(cast(dt_previsao_termino as varchar), '␀NULL␀')
                )
            ) as conteudo_hash
        from tipado
    ),

    -- quarentena (change vocabulario-e-qualidade-financeira-historica, D6):
    -- registros comprovadamente invalidos (valor negativo persistente, R$/UH
    -- extremo), varridos e versionados no seed. Anti-join AQUI, depois do
    -- strip_float_text em `tipado` — chave_natural ja esta limpa, casa com o
    -- seed. Ver seeds/data_quality/README.md.
    --
    -- Para RE-VARRER o seed contra uma silver limpa (sem circularidade):
    --   dbt build --select silver_mcmv_historico_serie_executiva \
    --     --vars 'quarentena_bypass: true' --target staging_duckdb
    -- depois regenerar o CSV e reconstruir sem a var.
    quarentena as (
        select distinct
            cast(fonte_familia as varchar) as fonte_familia,
            cast(chave_natural as varchar) as chave_natural
        from {{ ref('quarentena_valores_financeiros') }}
    ),

    util as (
        -- descarta linhas sem grao util: os relatorios agregados antigos de
        -- min_cidades (2011-2013) nao trazem chave nem metrica por empreendimento.
        select c.*
        from classificado c
        where
            c.dt_referencia is not null
            and (
                c.chave_natural is not null
                or c.uh_contratadas is not null
                or c.uh_entregues is not null
                or c.valor_investimento is not null
                or c.valor_financiamento is not null
            )
            {% if not var('quarentena_bypass', false) %}
            and not exists (
                select 1
                from quarentena q
                where
                    q.fonte_familia = c.fonte_familia
                    and q.chave_natural = c.chave_natural
            )
            {% endif %}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by
                    fonte_familia, coalesce(chave_natural, conteudo_hash), dt_referencia
                order by
                    report_date_parsed desc nulls last,
                    source_file desc,
                    -- desempate deterministico + preferencia por linha SEM valor
                    -- negativo quando a mesma chave/mes tem as duas versoes (a
                    -- fonte reenvia o snapshot com sinal corrigido). Change:
                    -- vocabulario-e-qualidade-financeira-historica (D6/8.3).
                    (
                        case
                            when
                                coalesce(valor_investimento, 0) < 0
                                or coalesce(valor_financiamento, 0) < 0
                                or coalesce(valor_vgv, 0) < 0
                                or coalesce(valor_contrapartidas, 0) < 0
                                or coalesce(valor_liberado, 0) < 0
                                or coalesce(subsidio_fgts, 0) < 0
                                or coalesce(subsidio_ogu, 0) < 0
                                or coalesce(subsidio_total, 0) < 0
                            then 1
                            else 0
                        end
                    ) asc,
                    hash_linha
            ) as rn
        from util
    ),

    -- situacao_derivada (D2 da change serie-historica-situacao-obra-regiao):
    -- esta fonte NAO tem status_operacional; a situacao e derivada SO de
    -- quantidade, com dominio e nome PROPRIOS — nunca confundir com a
    -- situacao_canonica reportada das silvers por frente. regiao_* por join ao
    -- seed dominio_regiao_uf sobre uf (min_cidades/bext antigos sem uf -> nula).
    enriquecido_dominio as (
        select
            d.*,
            case
                when
                    d.dt_contratacao is not null
                    and coalesce(d.uh_entregues, 0) = 0
                then 'contratada'
                when
                    d.uh_entregues > 0
                    and d.uh_contratadas is not null
                    and d.uh_entregues < d.uh_contratadas
                then 'em_entrega'
                when d.uh_contratadas > 0 and d.uh_entregues >= d.uh_contratadas
                then 'concluida'
                else 'nao_mapeada'
            end as situacao_derivada,
            dr.regiao_sigla,
            dr.regiao_nome
        from dedup d
        left join {{ ref('dominio_regiao_uf') }} dr
            on upper(trim(d.uf)) = upper(trim(dr.uf))
    )

-- Lista explicita no lugar do `select * exclude (rn, conteudo_hash)` que estava
-- aqui: `exclude` e sintaxe exclusiva do DuckDB (tarefa 4.2).
select
    programa,
    fonte_familia,
    frente_mcmv,
    agente_financeiro,
    chave_natural,
    uf,
    codigo_ibge_municipio,
    municipio,
    faixa,
    produto,
    nome_empreendimento,
    responsavel_nome,
    responsavel_id,
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
    subsidio_total,
    percentual_execucao_fisica,
    dt_contratacao,
    dt_entrega,
    dt_previsao_termino,
    dt_referencia,
    report_date_parsed,
    source_file,
    hash_linha,
    linha_ogu_fgts,
    grao_familia,
    situacao_derivada,
    regiao_sigla,
    regiao_nome
from enriquecido_dominio
where rn = 1
