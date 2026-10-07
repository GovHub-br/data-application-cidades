{{ config(materialized="table") }}

-- Prata: Operação do Rural com a ENTIDADE ORGANIZADORA resolvida e a situação simplificada.
-- Fonte: prata_rural_empreendimento (EO do Cad PJ / INT065 / INT057) e, só onde ela não traz EO,
-- o CNPJ "construtora/entidade" do arquivo AF CAIXA (bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa).
-- Grão: APF.
--
-- Por que o fallback: no Novo Rural a SNH não informa EO em nenhuma das ~1.260 operações e o
-- Cad PJ só cobre 127. O AF CAIXA traz um CNPJ de "construtora/entidade" — no Rural a EO é quem
-- contrata a obra, mas isso NÃO foi confirmado com a CAIXA; por isso `fonte_eo` diz de onde veio
-- e a ouro mostra a cobertura por fonte.

with
    emp as (select * from {{ ref("prata_rural_empreendimento") }}),

    af_caixa as (
        select distinct on (apf)
            {{ target.schema }}.normalize_apf(apf::text) as apf,
            nullif(regexp_replace(cnpj_da_construtora_entidade::text, '[^0-9]', '', 'g'), '') as cnpj,
            nullif(trim({{ target.schema }}.corrigir_mojibake(razao_social_da_construtora_entidade::text)), '') as nome
        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa") }}
        where upper(trim(modalidade::text)) = 'RURAL'
          and nullif(regexp_replace(cnpj_da_construtora_entidade::text, '[^0-9]', '', 'g'), '') is not null
        order by apf
    )

select
    e.apf,
    e.agente_financeiro,
    e.ic_novo_mcmv,
    e.empreendimento_nome,
    e.municipio,
    e.uf,
    e.regiao,
    e.cod_ibge,
    coalesce(regexp_replace(e.entidade_organizadora_cnpj::text, '[^0-9]', '', 'g'), a.cnpj) as eo_cnpj,
    coalesce(e.entidade_organizadora_nome, a.nome) as eo_nome,
    case
        when e.entidade_organizadora_cnpj is not null then 'cadastro EO (Cad PJ / INT065 / INT057)'
        when a.cnpj is not null then 'AF CAIXA construtora/entidade (a confirmar)'
        else 'sem EO'
    end as fonte_eo,
    e.situacao_empreendimento,
    case
        when e.situacao_empreendimento ~* 'distrat|cancel' then 'Distratada'
        when e.situacao_empreendimento ~* 'paralis' then 'Paralisada'
        when e.situacao_empreendimento ~* 'conclu|entreg' then 'Concluída'
        when e.situacao_empreendimento ~* 'n.o inic|projeto' then 'Não iniciada'
        else 'Em obra'
    end as status_operacao,
    e.dt_contratacao,
    e.dt_conclusao_obra,
    e.quantidade_uh_contratadas,
    e.quantidade_uh_entregues,
    e.quantidade_uh_distratadas,
    e.percentual_execucao_fisica,
    e.valor_contratado,
    e.valor_desembolsado
from emp e
left join af_caixa a on a.apf = e.apf
