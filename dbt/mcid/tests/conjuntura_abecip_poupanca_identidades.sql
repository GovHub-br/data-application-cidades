-- Confere as identidades contábeis da poupança SBPE da ABECIP.
--
-- A prata lê as colunas da aba `SBPE_Mensal` pelo cabeçalho, mas a do % da
-- captação não tem nome e as demais podem mudar de posição sem mudar de título. As
-- identidades amarram cada coluna ao significado:
--
--     captacao_liquida = deposito - retirada          (exata)
--     saldo[t] = saldo[t-1] + captacao + rendimento   (falha em 8 de 534 meses,
--                                                      mudanças de metodologia)
--
-- A primeira falha com mais de 1% das linhas fora; a segunda, com mais de 10%:
-- exceção histórica não derruba o build, coluna trocada desalinha em massa. Veio
-- do `ClienteAbecip`, que fazia a mesma conferência na ingestão antiga.

with serie as (
    select
        abs(deposito - retirada - captacao_liquida_valor) as erro_captacao,
        abs(lag(saldo) over (order by data_referencia)
            + captacao_liquida_valor + rendimento - saldo) as erro_saldo
    from {{ ref('prata_conjuntura_abecip_poupanca_sbpe') }}
),

conferencia as (
    select
        'captacao = deposito - retirada' as identidade,
        count(*) filter (where erro_captacao > 0.01)::numeric
            / nullif(count(erro_captacao), 0) as proporcao_fora,
        0.01 as limite
    from serie
    union all
    select
        'saldo = saldo anterior + captacao + rendimento',
        count(*) filter (where erro_saldo > 1.0)::numeric
            / nullif(count(erro_saldo), 0),
        0.10
    from serie
)

select identidade, proporcao_fora
from conferencia
where proporcao_fora > limite
