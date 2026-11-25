{#
    Corpos do produto Trabalho Social (models/trabalho_social_dbt).

    Regra de camadas do projeto: a prata só lê bronze e a ouro só lê prata. Quando uma prata precisa
    do resultado de outra, o SQL entra aqui como macro e é embutido (como em corpos_rural.sql),
    em vez de ref() de prata para prata. Cada modelo prata_ts_* é só {{ corpo_<modelo>() }}.
#}

{% macro corpo_prata_ts_acompanhamento() %}
-- Prata: acompanhamento do Trabalho Social por contrato na remessa de jul/2026, nas três linhas
-- (FAR, FDS/Entidades e Rural/PNHR) e nos dois agentes financeiros (CAIXA e BB), num formato único.
-- Grão: linha × agente financeiro × contrato. Chave: id_ts.
--
-- Fontes (bronzes de jul/2026, cópia fiel das planilhas):
--   FAR CAIXA   bronze_shpt_mcmv_far_ts_caixa_202607
--   FAR BB      bronze_ts_trabalho-social-2026-07__ts_af_bb_far_07_26
--   FDS CAIXA   bronze_shpt_mcmv_fds_ts_caixa_202607
--   RURAL CAIXA bronze_shpt_mcmv_rural_ts_caixa_202607
--   RURAL BB    bronze_shpt_mcmv_rural_ts_caixa_bb_202607 (apesar do nome, é a aba do BB)
--
-- Portaria de TS: aqui fica só a INFORMADA na planilha (portaria_ts_informada) e a que estava em
-- vigor na data de contratação (portaria_ts_vigente_contratacao, seed ts_portarias). A portaria
-- final, com o histórico das remessas, está em prata_ts_portaria_contrato.
--
-- Diferenças de layout tratadas aqui:
--   * BB grava % de execução do TS e % de obra entre 0 e 1; a CAIXA, entre 0 e 100. Tudo sai 0-100.
--   * BB grava a portaria só com o número ("21"); a CAIXA, "21-2014" (macro portaria_ts_normalizada).
--   * A planilha pode repetir o contrato: fica a linha de maior valor global de TS e
--     qt_linhas_origem conta as repetições.

with
    caixa_far as (
        select
            'FAR' as linha, 'CAIXA' as agente_financeiro,
            contrato, null::text as cim_registro_af, recurso, municipio, uf, nome_empreendimento, uh,
            tipologia, fase_mcmv, data_da_contratacao as dt_contratacao_txt,
            vr_global_ts, vr_desembolsado as vr_desembolsado_ts,
            vr_a_desembolsar_nao_concluidos as vr_a_desembolsar_ts, vr_nao_desembolsado_concluidos,
            execucao_ts, obra, portaria_adotada as portaria, instrumento_de_planejamento,
            natureza_execucao, data_entrega as dt_entrega_txt, situacao_ts,
            motivo_situacao_ts_atrasado_paralisado as motivo, null::text as dt_primeiro_relatorio_txt,
            dt_ultimo_avt as dt_ultimo_relatorio_txt, dt_avf as dt_avf_txt,
            _source_file as arquivo_de_origem
        from {{ ref("bronze_shpt_mcmv_far_ts_caixa_202607") }}
    ),

    caixa_fds as (
        select
            'FDS', 'CAIXA',
            contrato, null::text, recursos, municipio, uf, nome_empreendimento, uh,
            tipologia, fase_mcmv, data_da_contratacao,
            vr_global_ts, vr_desembolsado, vr_a_desembolsar_nao_concluidos, vr_nao_desembolsado_concluidos,
            execucao_ts, obra, portaria_adotada, instrumento_de_planejamento,
            -- o Postgres corta o nome da coluna em 63 caracteres
            natureza_execucao, dt_entrega_data_da_assinatura_do_primeiro_contrato_de_pessoa_fi, situacao_ts,
            motivo_situacao_ts_atrasado_paralisado, null::text,
            dt_ultimo_avt, dt_avf,
            _source_file
        from {{ ref("bronze_shpt_mcmv_fds_ts_caixa_202607") }}
    ),

    caixa_rural as (
        select
            'RURAL', 'CAIXA',
            contrato, null::text, recurso, municipio, uf, nome_empreendimento, uh,
            tipologia, fase_mcmv, data_da_contratacao,
            vr_global_ts, vr_desembolsado, vr_a_desembolsar_nao_concluidos, vr_nao_desembolsado_concluidos,
            execucao_ts, obra, portaria_adotada, instrumento_de_planejamento,
            natureza_execucao, dt_entrega, situacao_ts,
            motivo_situacao_ts_atrasado_paralisado, null::text,
            dt_ultimo_avt, dt_avf,
            _source_file
        from {{ ref("bronze_shpt_mcmv_rural_ts_caixa_202607") }}
    ),

    bb_far as (
        select
            'FAR', 'BB',
            contratoregistroao, cimregistroaf, recurso, municipio, uf, nome_empreendimento, uh,
            tipologia, fase_mcmv, data_contratacao_empreendimento,
            vr_total_ts, vr_desembolsado_ts, vr_a_desembolsar_ts, null::text,
            execucao_ts, obra, portaria_ts_utilizada, instrumento_de_planejamento,
            forma_natureza_de_execucao_direta_indireta_mista_pelo_af, dt_entrega, situacao_ts,
            motivo_situacao_ts_atrasado_paralisado, data_primeiro_relatorio,
            data_ultimo_relatorio, null::text,
            _source_file
        from {{ ref("bronze_ts_trabalho-social-2026-07__ts_af_bb_far_07_26") }}
    ),

    bb_rural as (
        select
            'RURAL', 'BB',
            contratoregistroao, null::text, recurso, municipio, uf, nome_empreendimento, uh,
            tipologia, fase_mcmv, data_contratacao_empreendimento,
            vr_total_ts, vr_desembolsado_ts, vr_a_desembolsar_ts, null::text,
            execucao_ts, obra, portaria_ts_utilizada, instrumento_de_planejamento,
            forma_natureza_de_execucao_direta_indireta_mista_pelo_af,
            data_da_assinatura_do_primeiro_contrato_de_pessoa_fisica, situacao_ts,
            motivo_situacao_ts_atrasado_paralisado, data_primeiro_relatorio,
            data_ultimo_relatorio, null::text,
            _source_file
        from {{ ref("bronze_shpt_mcmv_rural_ts_caixa_bb_202607") }}
    ),

    todas as (
        select * from caixa_far
        union all select * from caixa_fds
        union all select * from caixa_rural
        union all select * from bb_far
        union all select * from bb_rural
    ),

    tipada as (
        select
            linha,
            agente_financeiro,
            {{ ts_texto("contrato") }} as contrato,
            {{ ts_texto("cim_registro_af") }} as cim_registro_af,
            {{ ts_texto("recurso") }} as recurso,
            {{ ts_texto("nome_empreendimento") }} as empreendimento_nome,
            upper({{ ts_texto("municipio") }}) as municipio,
            upper({{ ts_texto("uf") }}) as uf,
            round({{ ts_numero("uh") }})::integer as qt_uh,
            {{ ts_texto("tipologia") }} as tipologia,
            {{ ts_texto("fase_mcmv") }} as fase_mcmv,
            {{ ts_data("dt_contratacao_txt") }} as dt_contratacao,
            {{ ts_numero("vr_global_ts", "numeric(15, 2)") }} as vr_global_ts,
            {{ ts_numero("vr_desembolsado_ts", "numeric(15, 2)") }} as vr_desembolsado_ts,
            {{ ts_numero("vr_a_desembolsar_ts", "numeric(15, 2)") }} as vr_a_desembolsar_ts,
            {{ ts_numero("vr_nao_desembolsado_concluidos", "numeric(15, 2)") }} as vr_nao_desembolsado_concluidos,
            {{ ts_numero("execucao_ts") }} as pc_execucao_ts_origem,
            {{ ts_numero("obra") }} as pc_obra_origem,
            {{ ts_texto("portaria") }} as portaria_ts_origem,
            {{ portaria_ts_normalizada("portaria") }} as portaria_ts_informada,
            upper({{ ts_texto("instrumento_de_planejamento") }}) as instrumento_planejamento,
            upper({{ ts_texto("natureza_execucao") }}) as natureza_execucao,
            {{ ts_data("dt_entrega_txt") }} as dt_entrega,
            upper({{ ts_texto("situacao_ts") }}) as situacao_ts_origem,
            {{ ts_texto("motivo") }} as motivo_situacao_ts,
            {{ ts_data("dt_primeiro_relatorio_txt") }} as dt_primeiro_relatorio,
            {{ ts_data("dt_ultimo_relatorio_txt") }} as dt_ultimo_relatorio,
            {{ ts_data("dt_avf_txt") }} as dt_avf,
            arquivo_de_origem,
            {{ ts_dt_referencia_arquivo("arquivo_de_origem") }} as dt_referencia
        from todas
    ),

    com_chave as (
        select
            t.*,
            {{ target.schema }}.normalize_apf(t.contrato) as apf,
            count(*) over (partition by t.linha, t.agente_financeiro, {{ target.schema }}.normalize_apf(t.contrato)) as qt_linhas_origem,
            row_number() over (
                partition by t.linha, t.agente_financeiro, {{ target.schema }}.normalize_apf(t.contrato)
                order by t.vr_global_ts desc nulls last, t.dt_ultimo_relatorio desc nulls last
            ) as ordem
        from tipada t
        where t.contrato is not null
    ),

    situacao as (
        select
            upper(trim(agente_financeiro)) as agente_financeiro,
            upper(trim(situacao_origem)) as situacao_origem,
            situacao_padronizada,
            lower(ic_ativo) = 'true' as ic_ativo,
            lower(ic_critico) = 'true' as ic_critico,
            lower(ic_equivalencia_a_confirmar) = 'true' as ic_equivalencia_a_confirmar
        from {{ ref("bronze_seed_ts_dominio_situacao") }}
    ),

    vigencia as (
        select
            portaria_ts,
            nullif(dt_inicio_vigencia, '')::date as dt_inicio,
            nullif(dt_fim_vigencia, '')::date as dt_fim,
            linhas
        from {{ ref("bronze_seed_ts_portarias") }}
    )

select
    c.linha || '|' || c.agente_financeiro || '|' || c.apf as id_ts,
    c.linha,
    c.agente_financeiro,
    c.apf,
    c.contrato,
    c.cim_registro_af,
    c.recurso,
    c.empreendimento_nome,
    c.municipio,
    c.uf,
    c.qt_uh,
    c.tipologia,
    c.fase_mcmv,
    c.dt_contratacao,
    c.vr_global_ts,
    c.vr_desembolsado_ts,
    c.vr_a_desembolsar_ts,
    c.vr_nao_desembolsado_concluidos,
    case
        when c.agente_financeiro = 'BB' and c.pc_execucao_ts_origem <= 1 then round(c.pc_execucao_ts_origem * 100, 2)
        else round(c.pc_execucao_ts_origem, 2)
    end::numeric(6, 2) as pc_execucao_ts,
    case
        when c.agente_financeiro = 'BB' and c.pc_obra_origem <= 1 then round(c.pc_obra_origem * 100, 2)
        else round(c.pc_obra_origem, 2)
    end::numeric(6, 2) as pc_obra,
    c.portaria_ts_origem,
    c.portaria_ts_informada,
    -- Portaria em vigor na data de contratação. É só uma referência: as portarias de 2014 e 2018
    -- podem ser aplicadas a contratos anteriores com anuência do ente, e a 75/2025 é preferencial
    -- para contratos antigos sem TS iniciado. Não substitui a informada.
    v.portaria_ts as portaria_ts_vigente_contratacao,
    c.instrumento_planejamento,
    c.natureza_execucao,
    c.dt_entrega,
    c.situacao_ts_origem,
    coalesce(s.situacao_padronizada, 'Sem equivalência') as situacao_ts,
    coalesce(s.ic_ativo, false) as ic_ativo,
    coalesce(s.ic_critico, false) as ic_critico,
    coalesce(s.ic_equivalencia_a_confirmar, true) as ic_equivalencia_a_confirmar,
    c.motivo_situacao_ts,
    c.dt_primeiro_relatorio,
    c.dt_ultimo_relatorio,
    c.dt_avf,
    c.qt_linhas_origem::integer as qt_linhas_origem,
    c.dt_referencia,
    c.arquivo_de_origem
from com_chave c
left join situacao s
    on s.agente_financeiro = c.agente_financeiro
    and s.situacao_origem = c.situacao_ts_origem
left join vigencia v
    on c.dt_contratacao >= coalesce(v.dt_inicio, '1900-01-01'::date)
    and c.dt_contratacao <= coalesce(v.dt_fim, '2999-12-31'::date)
    and position(c.linha in v.linhas) > 0
where c.ordem = 1
{% endmacro %}

{% macro corpo_prata_ts_portaria_remessa() %}
-- Prata: portaria de TS informada para cada contrato em CADA remessa guardada na staging
-- (jan/2025 a jul/2026). Grão: linha × agente financeiro × contrato × arquivo. Chave: id_ts_remessa.
-- Fonte: bronze_shpt_ts_remessas_historico (todas as planilhas da pasta Trabalho Social).
--
-- Linha, agente e competência saem do nome do arquivo:
--   MCMV_FAR_TS_CAIXA_BB_ 202607__ts_af_caixa_far_07_26  -> FAR, CAIXA, 2026-07-31
--   BASE_PNHR_TS_01_2025__pnhr_af_bb_31_01_2025          -> RURAL, BB, 2025-01-31
-- Ficam de fora as abas auxiliares (planilha1, planilha2) e a base de datas de 30/80/100% de obra.

with
    arquivos as (
        select
            b.*,
            regexp_replace(b.filename, '^.*/', '') as arquivo
        from {{ ref("bronze_shpt_ts_remessas_historico") }} b
    ),

    classificada as (
        select
            a.*,
            case
                when a.arquivo ~* 'FDS' then 'FDS'
                when a.arquivo ~* 'RURAL|PNHR' then 'RURAL'
                when a.arquivo ~* 'FAR' then 'FAR'
            end as linha,
            case
                when split_part(a.arquivo, '__', 2) ~* '(^|_)bb(_|\.|$)' then 'BB'
                when split_part(a.arquivo, '__', 2) ~* 'caixa|caxa' then 'CAIXA'
                when a.arquivo ~* 'FDS' then 'CAIXA'
                when a.arquivo ~* '_TS_BB_' then 'BB'
            end as agente_financeiro,
            case
                when a.arquivo ~ '(20\d{2})(0[1-9]|1[0-2])'
                    then (to_date((regexp_match(a.arquivo, '(20\d{2}(0[1-9]|1[0-2]))'))[1], 'YYYYMM')
                        + interval '1 month' - interval '1 day')::date
                when a.arquivo ~ '_(0[1-9]|1[0-2])_(20\d{2})'
                    then (to_date(array_to_string(regexp_match(a.arquivo, '_(0[1-9]|1[0-2])_(20\d{2})'), '/'), 'MM/YYYY')
                        + interval '1 month' - interval '1 day')::date
            end as dt_referencia
        from arquivos a
        where a.arquivo !~* 'planilha|data_30_80'
    ),

    tipada as (
        select
            linha,
            agente_financeiro,
            dt_referencia,
            arquivo,
            {{ ts_texto("coalesce(nullif(trim(contrato), ''), nullif(trim(contratoregistroao), ''), nullif(trim(contrato_registro_ao), ''), nullif(trim(contratoregistro_ao), ''))") }} as contrato,
            {{ ts_texto("coalesce(nullif(trim(portaria_adotada), ''), nullif(trim(portaria_ts_utilizada), ''))") }} as portaria_ts_origem,
            {{ portaria_ts_normalizada("coalesce(nullif(trim(portaria_adotada), ''), nullif(trim(portaria_ts_utilizada), ''))") }} as portaria_ts,
            {{ ts_data("coalesce(nullif(trim(data_da_contratacao), ''), nullif(trim(data_contratacao_empreendimento), ''))") }} as dt_contratacao,
            upper({{ ts_texto("situacao_ts") }}) as situacao_ts_origem
        from classificada
        where linha is not null and agente_financeiro is not null
    ),

    com_chave as (
        select
            t.*,
            {{ target.schema }}.normalize_apf(t.contrato) as apf,
            row_number() over (
                partition by t.linha, t.agente_financeiro, {{ target.schema }}.normalize_apf(t.contrato), t.arquivo
                order by t.portaria_ts nulls last
            ) as ordem
        from tipada t
        where t.contrato is not null
    )

select
    linha || '|' || agente_financeiro || '|' || apf || '|' || arquivo as id_ts_remessa,
    linha || '|' || agente_financeiro || '|' || apf as id_ts,
    linha,
    agente_financeiro,
    apf,
    contrato,
    dt_referencia,
    arquivo,
    portaria_ts_origem,
    portaria_ts,
    dt_contratacao,
    situacao_ts_origem
from com_chave
where ordem = 1
{% endmacro %}

{% macro corpo_prata_ts_marco_portaria() %}
-- Prata: régua de marcos temporais do Trabalho Social por portaria e linha. É a regra que o painel
-- usa para dizer o que já deveria estar pronto em cada contrato (junta por portaria_ts e linha com
-- prata_ts_portaria_contrato). Grão: marco. Chave: id_marco.
-- Fontes: seeds ts_marcos_portaria_464_2018 (Anexos III, IV e V) e ts_marcos_portaria_75_2025
-- (Anexo II). As portarias 21/2014 e 168/2013 ainda não têm régua: os marcos estão no Manual de
-- Instruções do TS (21/2014) e no Anexo V da 168/2013, que não foram transcritos.

with
    marcos as (
        select '464/2018' as portaria_ts, * from {{ ref("bronze_seed_ts_marcos_portaria_464_2018") }}
        union all
        select '75/2025' as portaria_ts, * from {{ ref("bronze_seed_ts_marcos_portaria_75_2025") }}
    )

select
    case when portaria_ts = '75/2025' then '75-' || trim(id_marco) else trim(id_marco) end as id_marco,
    portaria_ts,
    trim(linha) as linha,
    trim(ordem)::integer as ordem,
    trim(fase) as fase,
    trim(gatilho) as gatilho,
    nullif(trim(pc_obra_gatilho), '')::numeric(6, 2) as pc_obra_gatilho,
    nullif(trim(prazo_dias), '')::integer as prazo_dias,
    nullif(trim(exigencia), '') as exigencia,
    nullif(trim(consequencia_descumprimento), '') as consequencia_descumprimento,
    trim(referencia) as referencia,
    nullif(trim(rotulo_painel), '') as rotulo_painel
from marcos
{% endmacro %}

{% macro corpo_prata_ts_selecao_contrato() %}
-- Prata: portaria de SELEÇÃO de onde saiu cada contrato (FAR, FDS e Rural). Não é a portaria de
-- Trabalho Social: diz em que ciclo de seleção o empreendimento entrou. Grão: linha × APF.
-- Chave: id_selecao_contrato.
--
-- Fontes e método do vínculo:
--   FAR    bronze_seed_manual_far_portaria_selecao_apf: já vem por APF                       -> 'Exata'
--   FDS    bronze_seed_manual_fds_portaria_selecao_698_2026 + Cad PJ FDS (bronze, número da proposta) -> 'Exata'
--          (em out/2026 nenhuma das 393 propostas da 698/2026 está contratada no Cad PJ)
--   RURAL  corpo_prata_rural_selecao_contrato (a regra da prata_rural_selecao_contrato): 162/2018 por APF;
--          354/2024 e 697/2026 por CNPJ da EO
--          + município, com a confiança calculada lá (Exata, Alta, Média, Baixa)
-- Quando o mesmo APF cai em mais de uma seleção, fica a de maior confiança e, no empate, a portaria
-- mais recente; qt_selecoes_candidatas mostra quantas havia.

with
    far as (
        select
            'FAR'::text as linha,
            {{ target.schema }}.normalize_apf(f.apf) as apf,
            replace((regexp_match(f.portaria, 'n[ºo°]\s*([\d.]+)', 'i'))[1], '.', '')
                || '/' || (regexp_match(f.portaria, '(20\d{2})'))[1] as portaria_selecao,
            nullif(trim(f.portaria), '') as ato_selecao,
            null::text as id_proposta,
            'APF (seeds.far_portaria_selecao_apf)'::text as metodo_vinculo,
            'Exata'::text as confianca_vinculo,
            null::integer as qt_selecoes_mesmo_contrato
        from {{ ref("bronze_seed_manual_far_portaria_selecao_apf") }} f
        where nullif(trim(f.apf), '') is not null
    ),

    fds as (
        select
            'FDS'::text,
            {{ target.schema }}.normalize_apf(c.apf),
            '698/2026'::text,
            'Portaria MCID nº 698/2026'::text,
            lower(trim(s."IDENTIFICACAO DA PROPOSTA")),
            'Número da proposta (Cad PJ FDS)'::text,
            'Exata'::text,
            null::integer
        from {{ ref("bronze_seed_manual_fds_portaria_selecao_698_2026") }} s
        join (
            select distinct on ({{ target.schema }}.normalize_apf(nu_apf::text))
                {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
                nullif(trim(no_identificacao_proposta::text), '') as id_proposta,
                nullif(trim(no_eo::text), '') as eo_nome
            from {{ ref("bronze_shpt_monit_cad_pj_fds_mensal") }}
            where nullif(trim(nu_apf::text), '') is not null
            order by {{ target.schema }}.normalize_apf(nu_apf::text), filename desc
        ) c
            on lower(trim(c.id_proposta)) = lower(trim(s."IDENTIFICACAO DA PROPOSTA"))
        where c.apf is not null
    ),

    rural as (
        select
            'RURAL'::text,
            {{ target.schema }}.normalize_apf(r.apf_contrato),
            replace(r.portaria_codigo, '_', '/'),
            r.ato_selecao,
            r.protocolo_proposta,
            r.metodo_vinculo,
            r.confianca_vinculo,
            r.qt_selecoes_mesmo_contrato::integer
        from ({{ corpo_prata_rural_selecao_contrato() }}) r
        where r.ic_contratado
    ),

    todas as (
        select * from far
        union all select * from fds
        union all select * from rural
    ),

    ordenada as (
        select
            t.*,
            count(*) over (partition by t.linha, t.apf) as qt_selecoes_candidatas,
            row_number() over (
                partition by t.linha, t.apf
                order by
                    case t.confianca_vinculo when 'Exata' then 1 when 'Alta' then 2 when 'Média' then 3 else 4 end,
                    split_part(t.portaria_selecao, '/', 2) desc,
                    t.portaria_selecao desc
            ) as ordem
        from todas t
        where t.apf is not null
    )

select
    linha || '|' || apf as id_selecao_contrato,
    linha,
    apf,
    portaria_selecao,
    ato_selecao,
    id_proposta,
    metodo_vinculo,
    confianca_vinculo,
    qt_selecoes_mesmo_contrato,
    qt_selecoes_candidatas::integer as qt_selecoes_candidatas
from ordenada
where ordem = 1
{% endmacro %}

{% macro corpo_prata_ts_portaria_contrato() %}
-- Prata: a portaria de Trabalho Social de cada contrato, resolvida, e de onde ela veio.
-- Grão: linha × agente financeiro × contrato da remessa de jul/2026. Chave: id_ts.
-- Fontes: prata_ts_acompanhamento (jul/2026), prata_ts_portaria_remessa (remessas desde jan/2025),
-- o seed ts_portarias (vigência e se os marcos já foram transcritos) e prata_ts_selecao_contrato
-- (portaria de SELEÇÃO de onde o contrato saiu, com o método e a confiança do vínculo).
--
-- Ordem de preferência (coluna origem_portaria_ts):
--   1. 'informada'         -> a planilha de jul/2026 traz a portaria;
--   2. 'remessa anterior'  -> jul/2026 veio vazia, mas uma remessa anterior trouxe
--                             (vale a mais recente que trouxe);
--   3. 'sem portaria'      -> nenhuma remessa trouxe. portaria_ts fica nula.
--
-- A portaria em vigor na data de contratação (portaria_ts_vigente_contratacao) fica só como
-- referência e NÃO preenche portaria_ts. Conferido na remessa de jul/2026 (CAIXA, contratos com
-- portaria informada): ela acerta 5,6% no FAR contratado antes de 2014, 11,5% no FAR de 2018 a
-- 2025, 55% no FDS e 76% no Rural contratados depois de 05/02/2025. Os contratos antigos migraram
-- para normas mais novas (art. 2º da 21/2014, art. 3º da 464/2018, art. 36 da 75/2025).

with
    historico as (
        select
            id_ts,
            count(*) as qt_remessas,
            count(portaria_ts) as qt_remessas_com_portaria,
            count(distinct portaria_ts) as qt_portarias_distintas,
            min(dt_referencia) as dt_primeira_remessa,
            (array_agg(portaria_ts order by dt_referencia desc) filter (where portaria_ts is not null))[1]
                as portaria_ts_historico,
            (array_agg(dt_referencia order by dt_referencia desc) filter (where portaria_ts is not null))[1]
                as dt_referencia_historico,
            string_agg(distinct portaria_ts, ' | ' order by portaria_ts) as portarias_no_historico
        from ({{ corpo_prata_ts_portaria_remessa() }}) as remessa
        group by id_ts
    ),

    selecao as (
        select linha, apf, portaria_selecao, ato_selecao, id_proposta, metodo_vinculo, confianca_vinculo
        from ({{ corpo_prata_ts_selecao_contrato() }}) as selecao_contrato
    ),

    portarias as (
        select
            portaria_ts,
            lower(ic_marcos_transcritos) = 'true' as ic_marcos_transcritos,
            nullif(seed_marcos, '') as seed_marcos
        from {{ ref("bronze_seed_ts_portarias") }}
    ),

    resolvida as (
        select
            a.id_ts,
            a.linha,
            a.agente_financeiro,
            a.apf,
            a.contrato,
            a.empreendimento_nome,
            a.municipio,
            a.uf,
            a.qt_uh,
            a.dt_contratacao,
            a.situacao_ts,
            a.pc_obra,
            a.pc_execucao_ts,
            a.portaria_ts_informada,
            h.portaria_ts_historico,
            h.dt_referencia_historico,
            h.portarias_no_historico,
            coalesce(h.qt_remessas, 0)::integer as qt_remessas,
            coalesce(h.qt_remessas_com_portaria, 0)::integer as qt_remessas_com_portaria,
            coalesce(h.qt_portarias_distintas, 0) > 1 as ic_portaria_mudou_no_historico,
            a.portaria_ts_vigente_contratacao,
            coalesce(a.portaria_ts_informada, h.portaria_ts_historico) as portaria_ts,
            case
                when a.portaria_ts_informada is not null then 'informada'
                when h.portaria_ts_historico is not null then 'remessa anterior'
                else 'sem portaria'
            end as origem_portaria_ts,
            a.dt_referencia
        from ({{ corpo_prata_ts_acompanhamento() }}) a
        left join historico h on h.id_ts = a.id_ts
    )

select
    r.*,
    r.portaria_ts_informada is not null
        and r.portaria_ts_vigente_contratacao is not null
        and r.portaria_ts_informada <> r.portaria_ts_vigente_contratacao as ic_informada_difere_da_vigente,
    coalesce(p.ic_marcos_transcritos, false) as ic_marcos_transcritos,
    p.seed_marcos,
    s.portaria_selecao,
    s.ato_selecao,
    s.id_proposta,
    s.metodo_vinculo as metodo_vinculo_selecao,
    s.confianca_vinculo as confianca_vinculo_selecao
from resolvida r
left join portarias p on p.portaria_ts = r.portaria_ts
left join selecao s on s.linha = r.linha and s.apf = r.apf
{% endmacro %}

{% macro corpo_prata_ts_empreendimento_perfil() %}
-- Prata: perfil de cada contrato com Trabalho Social para o mapa: coordenadas, tipo de edificação
-- (casa, apartamento...), regime (condomínio ou loteamento), tipo de demanda e público tradicional.
-- Grão: id_ts (linha × agente × APF). Chave: id_ts.
--
-- Fontes e regras:
--   coordenadas     1º bronze_shpt_dados_prioritarios_snh_empreendimentos (SNH, por APF, retrato mais
--                   recente). Só valem pontos dentro do retângulo do Brasil (há valor sem ponto
--                   decimal e pontos no oceano, ex.: -0,0154 / -0,4845, que ficam de fora).
--                   2º centro do município (seed ibge_municipio_centroide, API de malhas do IBGE),
--                   pelo código IBGE ou, sem ele, pelo nome do município + UF. origem_coordenada diz qual.
--   tipo de edificação  FAR: INT040 (dsc_tipologia); FDS: INT059 (dsc_tipologia); Rural: casa
--                   construída no terreno da família (regra do PNHR, não vem da base).
--   regime          a tipologia da planilha de TS (CONDOMINIO, LOTEAMENTO).
--   tipo de demanda FAR: Cad PJ (co_tipo_demanda) com o seed ts_dominio_tipo_demanda.
--   público tradicional  ESTIMATIVA por palavra-chave (seed ts_publico_tradicional) no nome da entidade
--                   organizadora (FDS e Rural) e no nome do empreendimento. Não há campo próprio na
--                   origem. origem_publico_tradicional diz onde a palavra apareceu: no nome da EO é
--                   indício mais forte que no nome do empreendimento.

with
    a as (select * from ({{ corpo_prata_ts_acompanhamento() }}) as acompanhamento),

    snh as (
        select distinct on ({{ target.schema }}.normalize_apf(s.codigo_da_operacao_no_agente_financeiro::text))
            {{ target.schema }}.normalize_apf(s.codigo_da_operacao_no_agente_financeiro::text) as apf,
            {{ ts_numero("s.latitude", "double precision") }} as latitude,
            {{ ts_numero("s.longitude", "double precision") }} as longitude,
            nullif(regexp_replace(s.codigo_ibge_do_municipio::text, '[^0-9]', '', 'g'), '') as cod_ibge,
            s.nome_da_regiao as regiao
        from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }} s
        where nullif(trim(s.codigo_da_operacao_no_agente_financeiro::text), '') is not null
        order by {{ target.schema }}.normalize_apf(s.codigo_da_operacao_no_agente_financeiro::text),
            (nullif(trim(s.latitude::text), '') is not null) desc,
            s.data_de_referencia desc nulls last
    ),

    tipologia_sftp as (
        select distinct on (linha, apf) linha, apf, dsc_tipologia
        from (
            select 'FAR'::text as linha, {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
                upper(trim(dsc_tipologia)) as dsc_tipologia, dt_movimento::text as dt_movimento
            from {{ ref("bronze_sftp_empreendimento_int040") }}
            union all
            select 'FDS', {{ target.schema }}.normalize_apf(nu_apf::text),
                upper(trim(dsc_tipologia)), dt_movimento::text
            from {{ ref("bronze_sftp_empreendimento_int059") }}
        ) t
        where nullif(dsc_tipologia, '') is not null and dsc_tipologia <> 'NULL'
        order by linha, apf, dt_movimento desc nulls last
    ),

    demanda as (
        select
            {{ target.schema }}.normalize_apf(c.apf) as apf,
            c.co_tipo_demanda::text as co_tipo_demanda,
            d.tipo_demanda
        from (
            select distinct on ({{ target.schema }}.normalize_apf(nu_apf::text))
                {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
                {{ parse_int("co_tipo_demanda") }} as co_tipo_demanda
            from {{ ref("bronze_shpt_monit_cad_pj_far_mensal") }}
            where nullif(trim(nu_apf::text), '') is not null
            order by {{ target.schema }}.normalize_apf(nu_apf::text), filename desc
        ) c
        left join {{ ref("bronze_seed_ts_dominio_tipo_demanda") }} d on d.co_tipo_demanda = c.co_tipo_demanda::text
    ),

    eo as (
        select 'RURAL'::text as linha, {{ target.schema }}.normalize_apf(apf) as apf, max(eo_nome) as eo_nome
        from ({{ corpo_prata_rural_operacao_eo() }}) as operacao_eo group by 2
        union all
        select 'FDS', {{ target.schema }}.normalize_apf(apf), max(eo_nome)
        from (
            select distinct on ({{ target.schema }}.normalize_apf(nu_apf::text))
                {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
                nullif(trim(no_identificacao_proposta::text), '') as id_proposta,
                nullif(trim(no_eo::text), '') as eo_nome
            from {{ ref("bronze_shpt_monit_cad_pj_fds_mensal") }}
            where nullif(trim(nu_apf::text), '') is not null
            order by {{ target.schema }}.normalize_apf(nu_apf::text), filename desc
        ) as cad_fds group by 2
    ),

    centroide as (
        select
            cod_ibge,
            {{ municipio_chave("municipio") }} as municipio_chave,
            upper(trim(uf)) as uf,
            nullif(trim(latitude), '')::double precision as latitude,
            nullif(trim(longitude), '')::double precision as longitude
        from {{ ref("bronze_seed_ibge_municipio_centroide") }}
        where nullif(trim(latitude), '') is not null
    ),

    palavras as (
        select ordem::integer as ordem, publico_tradicional, regex_inclui
        from {{ ref("bronze_seed_ts_publico_tradicional") }}
    ),

    base as (
        select
            a.id_ts, a.linha, a.agente_financeiro, a.apf, a.empreendimento_nome, a.municipio, a.uf,
            a.tipologia as regime_origem,
            upper(coalesce(eo.eo_nome, '')) as eo_nome_upper,
            eo.eo_nome,
            upper(coalesce(a.empreendimento_nome, '')) as nome_upper,
            case when snh.latitude between -34 and 6 and snh.longitude between -74 and -28 then snh.latitude end as lat_snh,
            case when snh.latitude between -34 and 6 and snh.longitude between -74 and -28 then snh.longitude end as lon_snh,
            coalesce(c1.latitude, c2.latitude) as lat_municipio,
            coalesce(c1.longitude, c2.longitude) as lon_municipio,
            coalesce(c1.cod_ibge, c2.cod_ibge, snh.cod_ibge) as cod_ibge, snh.regiao,
            t.dsc_tipologia,
            dem.co_tipo_demanda, dem.tipo_demanda
        from a
        left join snh on snh.apf = a.apf
        left join tipologia_sftp t on t.linha = a.linha and t.apf = a.apf
        left join demanda dem on a.linha = 'FAR' and dem.apf = a.apf
        left join eo on eo.linha = a.linha and eo.apf = a.apf
        left join centroide c1 on left(c1.cod_ibge, 6) = snh.cod_ibge  -- a SNH guarda o código sem o dígito verificador
        left join centroide c2 on snh.cod_ibge is null
            and c2.uf = upper(a.uf) and c2.municipio_chave = {{ municipio_chave("a.municipio") }}
    ),

    publico as (
        select distinct on (b.id_ts)
            b.id_ts,
            p.publico_tradicional,
            case when b.eo_nome_upper ~ p.regex_inclui then 'nome da entidade organizadora'
                 else 'nome do empreendimento' end as origem_publico_tradicional
        from base b
        join palavras p
            on (b.eo_nome_upper ~ p.regex_inclui)
            or (b.nome_upper ~ p.regex_inclui
                and not (p.publico_tradicional = 'Quilombola' and upper(coalesce(b.municipio, '')) ~ 'QUILOMB'
                         and b.nome_upper !~ 'QUILOMBOL'))
        order by b.id_ts, (b.eo_nome_upper ~ p.regex_inclui) desc, p.ordem
    )

select
    b.id_ts,
    b.linha,
    b.agente_financeiro,
    b.apf,
    b.empreendimento_nome,
    b.eo_nome,
    b.municipio,
    b.uf,
    b.cod_ibge,
    b.regiao,
    round(coalesce(b.lat_snh, b.lat_municipio)::numeric, 6) as latitude,
    round(coalesce(b.lon_snh, b.lon_municipio)::numeric, 6) as longitude,
    case
        when b.lat_snh is not null then 'Endereço do empreendimento (SNH)'
        when b.lat_municipio is not null then 'Centro do município (IBGE)'
        else 'Sem coordenada'
    end as origem_coordenada,
    case
        when b.linha = 'RURAL' then 'Casa'
        when b.dsc_tipologia like 'APARTAMENTO%' then 'Apartamento'
        when b.dsc_tipologia like 'VILLAGE%' then 'Casa sobreposta'
        when b.dsc_tipologia like 'CASA%' then 'Casa'
        when b.dsc_tipologia like 'MISTO%' then 'Misto'
        else 'Não informado'
    end as tipo_edificacao,
    case
        when b.linha = 'RURAL' then 'Regra do PNHR (casa no terreno da família)'
        when b.dsc_tipologia is not null then case b.linha when 'FAR' then 'INT040' else 'INT059' end
    end as origem_tipo_edificacao,
    b.dsc_tipologia as tipologia_origem,
    case
        when b.dsc_tipologia ~ 'CONDOMINIO' or upper(coalesce(b.regime_origem, '')) ~ 'CONDOM' then 'Condomínio'
        when b.dsc_tipologia ~ 'LOTEAMENTO' or upper(coalesce(b.regime_origem, '')) ~ 'LOTEAMENTO' then 'Loteamento'
        when b.linha = 'RURAL' then 'Unidade isolada (rural)'
        else 'Não informado'
    end as regime,
    b.co_tipo_demanda,
    b.tipo_demanda,
    coalesce(p.publico_tradicional, 'Não identificado') as publico_tradicional_estimado,
    p.origem_publico_tradicional,
    p.publico_tradicional is not null and p.publico_tradicional <> 'Assentamento da reforma agrária'
        as ic_publico_tradicional
from base b
left join publico p on p.id_ts = b.id_ts
{% endmacro %}

{% macro corpo_prata_ts_demanda_dossie() %}
-- Prata: indicação de demanda e dossiês dos empreendimentos FAR da lista "empreendimentos sem
-- indicação de demanda" (SNH), nas duas remessas recebidas (02/09/2026 e 06/10/2026), tipadas.
-- Grão: APF × remessa. Chave: id_demanda_remessa.
-- A remessa de 06/10 não traz UF, município, UH, data de contratação, ente, construtora nem a
-- situação do TS: essas colunas vêm nulas nela (a ouro completa pela remessa de 02/09 e pelo TS).
--
-- Leitura dos campos:
--   qt_pesquisas_sigdh   grupos familiares indicados pelo ente e pesquisados no SIGDH (a indicação
--                        de demanda). Zero = o ente ainda não indicou famílias.
--   qt_dossies_enviados  dossiês das famílias enviados pelo aplicativo; aprovados = validados.
--   qt_contratos_assinados  contratos de pessoa física já assinados.

with
    r0902 as (
        select
            nu_apf_com_dv, no_empreendimento, qt_uh, dt_contratacao, no_municipio, sg_uf,
            nu_cnpj_ente_local, no_ente_local, no_construtor, pc_acumulado_executado_sisme,
            qt_pesq_sigdh_grupo_familiar, pc_pesq_sigdh, qt_pesq_compat_sigdh, qt_dossies_enviados_app,
            qt_dossies_aprovados_app, pc_dossies_aprovados_app, qt_contratos_assinados, si_trabalho_social,
            nu_status_educacao_infantil, nu_50_obra_equip_comunitario, nu_inf_ext_rede_eletrica,
            nu_inf_ext_pavimentacao, nu_inf_ext_agua_esgoto, _source_file
        from {{ ref("bronze_ts_empreendimentos-sem-indicacao-de-demanda-2026-09-02") }}
    ),

    r1006 as (
        select
            nu_apf_com_dv, no_empreendimento, null::text, null::text, null::text, null::text,
            null::text, null::text, null::text, pc_acumulado_executado_sisme,
            qt_pesq_sigdh_grupo_familiar, pc_pesq_sigdh, null::text, qt_dossies_enviados_app,
            qt_dossies_aprovados_app, pc_dossies_aprovados_app, qt_contratos_assinados, null::text,
            nu_status_educacao_infantil, nu_50_obra_equip_comunitario, nu_inf_ext_rede_eletrica,
            nu_inf_ext_pavimentacao, nu_inf_ext_agua_esgoto, _source_file
        from {{ ref("bronze_ts_empreendimentos-sem-indicacao-de-demanda-2026-10-06") }}
    ),

    todas as (
        select * from r0902
        union all
        select * from r1006
    )

select
    {{ target.schema }}.normalize_apf(nu_apf_com_dv::text) || '|'
        || {{ ts_dt_referencia_arquivo("_source_file") }}::text as id_demanda_remessa,
    {{ target.schema }}.normalize_apf(nu_apf_com_dv::text) as apf,
    {{ ts_dt_referencia_arquivo("_source_file") }} as dt_referencia,
    {{ ts_texto("no_empreendimento") }} as empreendimento_nome,
    round({{ ts_numero("qt_uh") }})::integer as qt_uh,
    {{ ts_data("dt_contratacao") }} as dt_contratacao,
    upper({{ ts_texto("no_municipio") }}) as municipio,
    upper({{ ts_texto("sg_uf") }}) as uf,
    {{ cnpj_normalizado("nu_cnpj_ente_local") }} as ente_cnpj,
    {{ ts_texto("no_ente_local") }} as ente_nome,
    {{ ts_texto("no_construtor") }} as construtora_nome,
    {{ ts_numero("pc_acumulado_executado_sisme", "numeric(6, 2)") }} as pc_obra,
    round({{ ts_numero("qt_pesq_sigdh_grupo_familiar") }})::integer as qt_pesquisas_sigdh,
    {{ ts_numero("pc_pesq_sigdh", "numeric(9, 2)") }} as pc_pesquisas_sigdh,
    round({{ ts_numero("qt_pesq_compat_sigdh") }})::integer as qt_pesquisas_compativeis,
    round({{ ts_numero("qt_dossies_enviados_app") }})::integer as qt_dossies_enviados,
    round({{ ts_numero("qt_dossies_aprovados_app") }})::integer as qt_dossies_aprovados,
    {{ ts_numero("pc_dossies_aprovados_app", "numeric(9, 2)") }} as pc_dossies_aprovados,
    round({{ ts_numero("qt_contratos_assinados") }})::integer as qt_contratos_assinados,
    {{ ts_texto("si_trabalho_social") }} as situacao_ts_snh,
    {{ ts_texto("nu_status_educacao_infantil") }} as status_educacao_infantil,
    {{ ts_texto("nu_50_obra_equip_comunitario") }} as status_equip_comunitario_50,
    {{ ts_texto("nu_inf_ext_rede_eletrica") }} as status_infra_rede_eletrica,
    {{ ts_texto("nu_inf_ext_pavimentacao") }} as status_infra_pavimentacao,
    {{ ts_texto("nu_inf_ext_agua_esgoto") }} as status_infra_agua_esgoto,
    _source_file as arquivo_de_origem
from todas
where nullif(trim(nu_apf_com_dv::text), '') is not null
{% endmacro %}

{% macro corpo_ouro_ts_alerta_marco() %}
-- Ouro: alertas de marco temporal do Trabalho Social. Uma linha por contrato × marco da portaria
-- de TS do contrato que JÁ ESTÁ VALENDO para ele (o gatilho foi atingido). ic_descumprido diz se os
-- dados mostram que o marco não foi cumprido, e `evidencia` diz por quê, em texto.
-- Grão: id_ts × id_marco. Chave: id_alerta.
-- Fontes: prata_ts_portaria_contrato (portaria resolvida), prata_ts_acompanhamento (obra, relatórios,
-- desembolso, situação) e prata_ts_marco_portaria (réguas da 464/2018 e da 75/2025).
--
-- Só entram os marcos que dá para verificar com o que a planilha traz (% de obra, entrega, datas de
-- relatório, execução e desembolso do TS). Ficam de fora as penalidades, a pré-obras (sem prazo na
-- norma), o 464-RURAL-02 (a norma não fixa intervalo) e as linhas SUB50 e Pró-Moradia (sem planilha).
-- Contratos com portaria 21/2014, 168/2013, anterior à 168 ou sem portaria não têm régua e não entram.
--
-- Sinais usados (dt_referencia = competência da planilha, jul/2026):
--   ts_encerrado   situação Concluído ou Distrato: nenhum marco é cobrado.
--   ts_com_sinal   há execução do TS > 0, desembolso > 0 ou algum relatório. Sem nenhum desses, o TS
--                  não deu sinal de ter começado (indício de convênio/PTS ausente; a planilha não traz
--                  a data do convênio nem da aprovação do PTS).
--   dias_sem_relatorio  dt_referencia - último relatório (ou o primeiro, no BB, se só houver ele).

with
    contrato as (
        select
            c.id_ts, c.linha, c.agente_financeiro, c.apf, c.contrato, c.empreendimento_nome,
            c.municipio, c.uf, c.qt_uh, c.dt_contratacao, c.portaria_ts, c.origem_portaria_ts,
            c.portaria_selecao, c.situacao_ts,
            a.situacao_ts_origem, a.recurso, a.pc_obra, a.pc_execucao_ts, a.vr_global_ts,
            a.vr_desembolsado_ts, a.dt_entrega, a.dt_primeiro_relatorio, a.dt_ultimo_relatorio,
            a.natureza_execucao, a.instrumento_planejamento, a.motivo_situacao_ts, a.dt_referencia
        from {{ ref("prata_ts_portaria_contrato") }} c
        join {{ ref("prata_ts_acompanhamento") }} a on a.id_ts = c.id_ts
        where c.ic_marcos_transcritos
    ),

    sinais as (
        select
            c.*,
            c.situacao_ts in ('Concluído', 'Distrato') as ic_ts_encerrado,
            (coalesce(c.pc_execucao_ts, 0) > 0 or coalesce(c.vr_desembolsado_ts, 0) > 0
                or c.dt_primeiro_relatorio is not null or c.dt_ultimo_relatorio is not null) as ic_ts_com_sinal,
            (c.dt_primeiro_relatorio is not null or c.dt_ultimo_relatorio is not null) as ic_tem_relatorio,
            (coalesce(c.pc_obra, 0) >= 100 or c.dt_entrega <= c.dt_referencia) as ic_obra_entregue,
            c.dt_referencia - coalesce(c.dt_ultimo_relatorio, c.dt_primeiro_relatorio) as dias_sem_relatorio,
            c.dt_referencia - c.dt_contratacao as dias_desde_contratacao
        from contrato c
    ),

    marcos as (
        select * from {{ ref("prata_ts_marco_portaria") }}
    ),

    avaliado as (
        select
            s.*,
            m.id_marco, m.ordem as ordem_marco, m.fase, m.gatilho, m.pc_obra_gatilho, m.prazo_dias,
            m.exigencia, m.consequencia_descumprimento, m.referencia, m.rotulo_painel,
            -- o gatilho do marco já foi atingido?
            case m.id_marco
                when '75-FAR-01' then coalesce(s.pc_obra, 0) >= 60
                when '75-FAR-02' then coalesce(s.pc_obra, 0) >= 80
                when '75-FAR-03' then s.ic_obra_entregue
                when '75-FDS-02' then s.dias_desde_contratacao > 90
                when '75-RURAL-02' then s.dias_desde_contratacao > 90
                when '464-FAR-02' then coalesce(s.pc_obra, 0) >= 30
                when '464-FAR-03' then coalesce(s.pc_obra, 0) >= 80
                when '464-FAR-04' then s.ic_tem_relatorio
                when '464-FAR-05' then s.ic_obra_entregue
                when '464-FDS-02' then s.dias_desde_contratacao > 180
            end as ic_aplicavel,
            -- os dados mostram que o marco não foi cumprido?
            not s.ic_ts_encerrado and case m.id_marco
                when '75-FAR-01' then not s.ic_ts_com_sinal
                when '75-FAR-02' then not s.ic_tem_relatorio
                when '75-FAR-03' then not s.ic_tem_relatorio or s.dias_sem_relatorio > 90
                when '75-FDS-02' then not s.ic_tem_relatorio or s.dias_sem_relatorio > 90
                when '75-RURAL-02' then not s.ic_tem_relatorio or s.dias_sem_relatorio > 90
                when '464-FAR-02' then not s.ic_ts_com_sinal
                when '464-FAR-03' then not s.ic_tem_relatorio
                when '464-FAR-04' then s.dias_sem_relatorio > 180
                when '464-FAR-05' then not s.ic_ts_com_sinal
                when '464-FDS-02' then not s.ic_tem_relatorio or s.dias_sem_relatorio > 180
            end as ic_descumprido
        from sinais s
        join marcos m on m.portaria_ts = s.portaria_ts and m.linha = s.linha
    )

select
    id_ts || '|' || id_marco as id_alerta,
    id_ts,
    id_marco,
    -- rótulo legível (pedido da UnB): linha · quando · o que se exige (portaria)
    coalesce(rotulo_painel, portaria_ts || ' · ' || id_marco || ' · ' || gatilho) as marco,
    portaria_ts,
    origem_portaria_ts,
    linha,
    agente_financeiro,
    apf,
    contrato,
    empreendimento_nome,
    municipio,
    uf,
    qt_uh,
    dt_contratacao,
    portaria_selecao,
    ordem_marco,
    fase,
    gatilho,
    exigencia,
    consequencia_descumprimento,
    referencia,
    coalesce(ic_descumprido, false) as ic_descumprido,
    case
        when not coalesce(ic_descumprido, false) then null
        when id_marco in ('75-FAR-01', '464-FAR-02', '464-FAR-05')
            then 'Obra em ' || coalesce(pc_obra::text, '?') || '%; TS sem execução, sem desembolso e sem relatório'
        when id_marco in ('75-FAR-02', '464-FAR-03')
            then 'Obra em ' || coalesce(pc_obra::text, '?') || '%; nenhum relatório de TS informado'
        when not ic_tem_relatorio
            then 'Nenhum relatório de TS informado; contratado há ' || dias_desde_contratacao || ' dias'
        else 'Último relatório há ' || dias_sem_relatorio || ' dias (limite de ' || prazo_dias || ')'
    end as evidencia,
    case
        when not coalesce(ic_descumprido, false) then null
        when prazo_dias is not null and ic_tem_relatorio then dias_sem_relatorio - prazo_dias
    end as dias_alem_do_prazo,
    situacao_ts,
    situacao_ts_origem,
    motivo_situacao_ts,
    recurso,
    natureza_execucao,
    instrumento_planejamento,
    pc_obra,
    pc_execucao_ts,
    vr_global_ts,
    vr_desembolsado_ts,
    dt_entrega,
    dt_ultimo_relatorio,
    dias_sem_relatorio,
    ic_ts_com_sinal,
    ic_tem_relatorio,
    ic_obra_entregue,
    dt_referencia,
    -- uma linha por contrato com alerta: a do marco mais avançado descumprido (para somar UH e valor
    -- sem contar o contrato duas vezes)
    coalesce(ic_descumprido, false) and row_number() over (
        partition by id_ts, coalesce(ic_descumprido, false)
        order by ordem_marco desc
    ) = 1 as ic_alerta_principal
from avaliado
where ic_aplicavel
{% endmacro %}
