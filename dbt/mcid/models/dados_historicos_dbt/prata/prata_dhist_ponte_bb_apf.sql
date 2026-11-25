-- Ponte BB → APF dos dados históricos: o INT054 (2018-08) traz o código do empreendimento
-- no BB e o APF lado a lado (prova no notebook 04, seção 2). Chave única pelo código BB, sem
-- zeros à esquerda, para o join nas silvers do BB não multiplicar linhas (research R1).
{% call duckdb_query() %}
select distinct ltrim(trim(nu_empreendimento), '0') as codigo_empreendimento_bb, trim(nu_apf) as apf
from {{ rel_pgduckdb(ref("bronze_dhist_int054_far_bb")) }}
where nullif(trim(nu_empreendimento), '') is not null and nullif(trim(nu_apf), '') is not null
{% endcall %}
