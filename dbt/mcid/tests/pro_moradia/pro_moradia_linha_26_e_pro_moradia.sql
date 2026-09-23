{#-
    O Pró-Moradia é identificado no Canal FGTS por UM código: a linha `26`, que a prata
    `prata_pro_moradia_contrato` usa como filtro fixo. Se a CAIXA reatribuir o código — ou a
    tabela de domínio chegar sem ele —, o produto passaria a mostrar outro programa, ou nada,
    sem erro nenhum. Este teste é o aviso: devolve linha quando o `26` não é mais Pró-Moradia.
-#}
{{ config(severity="error") }}

with
    linha_26 as (
        select trim(linha::text) as linha
        from {{ ref("bronze_shpt_fgts_canal_tdom_linha") }}
        where trim(codigo::text) = '26'
    )

select 'linha 26 ausente da tabela de domínio' as problema
where not exists (select 1 from linha_26)

union all

select 'linha 26 agora é: ' || linha as problema
from linha_26
where upper(linha) not like '%PRO-MORADIA%'
