-- FR-005: a ponte BB → APF tem um APF por código BB. Código repetido multiplicaria as linhas
-- das silvers do BB (o dhist_prata_linhas também pega isso, família por família).
{% call duckdb_query() %}
select codigo_empreendimento_bb, count(*) as n
from {{ rel_pgduckdb(ref("prata_dhist_ponte_bb_apf")) }}
group by 1
having count(*) > 1
{% endcall %}
