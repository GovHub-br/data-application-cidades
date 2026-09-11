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
-- Grao de saida: 1 linha por (fonte_familia, chave_natural, dt_referencia) —
-- o EMPREENDIMENTO (bases_relatorio_executivo, entrada_bb) ou o CONTRATO PF
-- (bext, min_cidades) inteiro no mes, agregado (SUM) a partir das linhas de
-- movimento da fonte. Ver a secao "DEDUP EM DUAS ETAPAS" abaixo e o doc de
-- auditoria docs/auditar-grao-serie-executiva-bases-relat-exec.md. Antes esta
-- silver escolhia UMA linha de movimento arbitraria por (chave, mes) via
-- row_number()/rn=1 (change auditar-grao-serie-executiva-historica).
--
-- `natureza_serie` por familia (auditoria D4): estoque (carteira restatada mes
-- a mes) para bases_relatorio_executivo / min_cidades / bext; fluxo (entrada de
-- novos empreendimentos) para entrada_bb. O gold propaga (nao mais 'estoque'
-- fixo).
--
-- `linha_ogu_fgts` classifica o registro AGREGADO para a serie do piloto #118:
-- OGU/Subsidiado quando o subsidio OGU somado domina; FGTS/Financiado quando o
-- FGTS somado domina; os valores brutos ficam expostos para o gold somar os dois.
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

            -- grao_familia por familia (change
            -- vocabulario-e-qualidade-financeira-historica D3, refinado pela
            -- auditoria auditar-grao-serie-executiva-bases-relat-exec.md):
            -- bext E min_cidades sao extrato de contrato PF individual (1 linha
            -- por contrato/mes, ~2-5 UH/linha); bases_relatorio_executivo e
            -- entrada_bb sao por empreendimento. Impede o consumidor de somar
            -- UH/valor entre graos diferentes no ouro_historico_serie_mensal.
            case
                when fonte_familia in ('bext', 'min_cidades') then 'contrato'
                else 'empreendimento'
            end as grao_familia,

            -- natureza_serie por familia (D4, auditoria task 1.2/1.7): antes
            -- hard-coded 'estoque' no ouro_historico_serie_mensal. bases_relatorio_executivo
            -- / min_cidades / bext sao ESTOQUE (carteira restatada mes a mes;
            -- valor por contrato constante entre meses); entrada_bb e FLUXO
            -- (entrada de novos empreendimentos na carteira BB).
            case
                when fonte_familia = 'entrada_bb' then 'fluxo'
                else 'estoque'
            end as natureza_serie,

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
    --   dbt build --select prata_historico_serie_executiva \
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

    -- ── DEDUP EM DUAS ETAPAS + AGREGACAO AO GRAO DE CONSUMO ──────────────────
    -- change auditar-grao-serie-executiva-historica; auditoria em
    -- models/mcmv_historico_dbt/docs/auditar-grao-serie-executiva-bases-relat-exec.md
    --
    -- A fonte NAO e snapshot por empreendimento: bases_relatorio_executivo e
    -- bext sao razoes de MOVIMENTO de unidades (coluna uh assinada — +N
    -- contratacao, -N distrato) restatados a cada arquivo mensal. O
    -- row_number()/rn=1 antigo mantinha UMA linha de movimento arbitraria por
    -- (chave, mes): faixa, subsidio e linha_ogu_fgts da linha sobrevivente eram
    -- sorteados (24% das linhas com uh_comercializadas > uh_entregues).
    --
    --  1. reenvio_rank  — entre source_file do mesmo mes/chave/faixa/municipio,
    --     mantem so o mais recente (report_date_parsed, depois source_file:
    --     `_v2` > base). ANTES de qualquer soma.
    --  2. conteudo_rank — colapsa reingestao byte-a-byte da mesma linha de
    --     negocio (o extrator a montante emite "295.0" e "295"): 87% dos grupos
    --     multi-linha por APF sao isso, nao movimento. SUM cego dobraria (~9%).
    --  3. agregado      — SUM ao grao de consumo (chave_natural, dt_referencia):
    --     os pares +N/-N genuinos netam (distrato -> 0); a linha passa a
    --     representar o empreendimento/contrato inteiro. linha_ogu_fgts e
    --     situacao_derivada sao RECALCULADOS sobre os valores agregados.
    -- `qualify` e sintaxe exclusiva do DuckDB — o rank vai em CTE + `where`.
    reenvio_rank as (
        select
            *,
            dense_rank() over (
                partition by
                    fonte_familia,
                    coalesce(chave_natural, conteudo_hash),
                    coalesce(faixa, ''),
                    coalesce(codigo_ibge_municipio, ''),
                    dt_referencia
                order by report_date_parsed desc nulls last, source_file desc
            ) as rk_reenvio
        from util
    ),

    conteudo_rank as (
        select
            *,
            row_number() over (
                partition by
                    fonte_familia,
                    coalesce(chave_natural, ''),
                    conteudo_hash,
                    dt_referencia
                order by hash_linha
            ) as rk_conteudo
        from reenvio_rank
        where rk_reenvio = 1
    ),

    agregado as (
        select
            programa,
            fonte_familia,
            frente_mcmv,
            grao_familia,
            natureza_serie,
            dt_referencia,

            max(chave_natural) as chave_natural,
            max(agente_financeiro) as agente_financeiro,
            max(uf) as uf,
            max(codigo_ibge_municipio) as codigo_ibge_municipio,
            max(municipio) as municipio,
            max(faixa) as faixa,
            max(produto) as produto,
            max(nome_empreendimento) as nome_empreendimento,
            max(responsavel_nome) as responsavel_nome,
            max(responsavel_id) as responsavel_id,

            -- cast p/ bigint: sum(bigint) devolve HUGEINT (int128) no DuckDB,
            -- sem tipo equivalente no Postgres (modo C). O grão pré-2019 tem
            -- max ~12 k UH/linha; o total nacional agregado no ouro_historico_serie_mensal
            -- ~1,5 M — cabe em bigint com folga.
            -- Change: verificar-tipagem-silver-gold-historico.
            cast(sum(uh_contratadas) as bigint) as uh_contratadas,
            cast(sum(uh_entregues) as bigint) as uh_entregues,
            cast(sum(uh_concluidas) as bigint) as uh_concluidas,
            cast(sum(uh_em_obras) as bigint) as uh_em_obras,
            cast(sum(uh_comercializadas) as bigint) as uh_comercializadas,

            sum(valor_investimento) as valor_investimento,
            sum(valor_financiamento) as valor_financiamento,
            sum(valor_vgv) as valor_vgv,
            sum(valor_contrapartidas) as valor_contrapartidas,
            sum(valor_liberado) as valor_liberado,
            sum(subsidio_fgts) as subsidio_fgts,
            sum(subsidio_ogu) as subsidio_ogu,
            sum(subsidio_total) as subsidio_total,

            max(percentual_execucao_fisica) as percentual_execucao_fisica,
            max(dt_contratacao) as dt_contratacao,
            max(dt_entrega) as dt_entrega,
            max(dt_previsao_termino) as dt_previsao_termino,
            max(report_date_parsed) as report_date_parsed,
            max(source_file) as source_file,
            max(hash_linha) as hash_linha
        from conteudo_rank
        where rk_conteudo = 1
        group by
            programa,
            fonte_familia,
            frente_mcmv,
            grao_familia,
            natureza_serie,
            coalesce(chave_natural, conteudo_hash),
            dt_referencia
    ),

    classificado_agregado as (
        select
            *,
            case
                when
                    coalesce(subsidio_ogu, 0) > 0
                    and coalesce(subsidio_ogu, 0) >= coalesce(subsidio_fgts, 0)
                then 'OGU/Subsidiado'
                when coalesce(subsidio_fgts, 0) > 0
                then 'FGTS/Financiado'
            end as linha_ogu_fgts
        from agregado
    ),

    -- situacao_derivada (D2 da change serie-historica-situacao-obra-regiao):
    -- esta fonte NAO tem status_operacional; a situacao e derivada SO de
    -- quantidade (agora agregada), com dominio e nome PROPRIOS — nunca confundir
    -- com a situacao_canonica das silvers por frente. regiao_* por join ao seed
    -- dominio_regiao_uf sobre uf (min_cidades/bext antigos sem uf -> nula).
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
        from classificado_agregado d
        left join {{ ref('dominio_regiao_uf') }} dr
            on upper(trim(d.uf)) = upper(trim(dr.uf))
    )

-- Lista explicita no lugar do `select * exclude` (sintaxe exclusiva do DuckDB).
-- 1 linha por (fonte_familia, chave_natural/conteudo, dt_referencia).
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
    natureza_serie,
    situacao_derivada,
    regiao_sigla,
    regiao_nome
from enriquecido_dominio
