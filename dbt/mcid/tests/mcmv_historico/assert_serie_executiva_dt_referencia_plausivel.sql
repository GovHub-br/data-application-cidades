-- Teste singular: dt_referencia da serie executiva historica deve cair na janela
-- plausivel do dump (2009-01 a 2019-12). Depois disso a serie mensal e coberta
-- pelo GEFUS (silver_mcmv_historico_empreendimento) e pela SNH
-- (bronze_mcmv_historico_empreendimento_snh). Retorna linhas fora da janela.

select
    fonte_familia,
    dt_referencia,
    count(*) as n_linhas
from {{ ref("silver_mcmv_historico_serie_executiva") }}
where dt_referencia < date '2009-01-01'
   or dt_referencia >= date '2020-01-01'
group by fonte_familia, dt_referencia
order by dt_referencia
