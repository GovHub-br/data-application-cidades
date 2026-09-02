{#-
  Teste genérico: falha se a coluna contiver mojibake.

  Mojibake é o resultado de ler bytes utf-8 como latin-1/cp1252: "São" vira "SÃ£o",
  "código" vira "cÃ³digo". Já aconteceu no lake — 739 arquivos foram decodificados como
  latin-1 porque um único byte inválido derrubava a leitura inteira (corrigido em
  scripts/lake_utils.py e scripts/raw_para_staging.py).

  O estrago era invisível: quando o cabeçalho do arquivo é ASCII, os NOMES das colunas
  saem limpos e só os VALORES ficam corrompidos, então nada quebra — o dado errado só
  aparece no dashboard. Este teste existe para que isso não passe silencioso de novo.

  Marcadores:
    Ã     acentos latinos (Ã£=ã, Ã©=é, Ã³=ó, Ã§=ç ...)
    Â     símbolos (Âº, Â°, Â§)
    â€    pontuação tipográfica (aspas curvas, travessão)
    U+FFFD  o "�" que errors="replace" deixa onde o byte era realmente inválido

  Uso no schema.yml:

      columns:
        - name: municipio
          data_tests:
            - sem_mojibake
-#}
{% macro test_sem_mojibake(model, column_name) %}

select {{ column_name }} as valor, count(*) as ocorrencias
from {{ model }}
where
    {{ column_name }} like '%Ã%'
    or {{ column_name }} like '%Â%'
    or {{ column_name }} like '%â€%'
    or {{ column_name }} like '%' || chr(65533) || '%'
group by {{ column_name }}
order by ocorrencias desc

{% endmacro %}
