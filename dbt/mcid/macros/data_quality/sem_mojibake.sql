{#-
  Teste genérico: falha se a coluna contiver mojibake.

  Mojibake é o resultado de ler bytes utf-8 como latin-1/cp1252: "São" vira "SÃ£o",
  "código" vira "cÃ³digo". Quando o cabeçalho do arquivo é ASCII, os NOMES das colunas
  saem limpos e só os VALORES ficam corrompidos, então nada quebra: o dado errado
  aparece direto no dashboard. Daí o teste.

  Marcadores:
    Ã / Â seguidos de outro caractere NÃO-ASCII que não seja maiúscula acentuada
          É essa vizinhança que separa corrupção de português correto. Ã e Â são letras
          legítimas (SÃO, PORTÃO, ASSOCIAÇÃO, CÂMARA, AMANHÃ), e ali vem sempre ASCII
          depois — outra maiúscula, espaço ou pontuação. No mojibake, o segundo byte da
          sequência utf-8 reinterpretada cai no bloco latin-1: Ã£=ã, Ã©=é, Ã³=ó, Ã§=ç,
          Âº, Â°. Por isso a regra exige não-ASCII, e não apenas "não maiúscula".
    â€     pontuação tipográfica (aspas curvas, travessão)
    U+FFFD  o "�" que errors="replace" deixa onde o byte era realmente inválido

  Uso no schema.yml:

      columns:
        - name: municipio
          tests:
            - sem_mojibake
-#}
{% macro test_sem_mojibake(model, column_name) %}

    select {{ column_name }} as valor, count(*) as ocorrencias
    from {{ model }}
    where {{ column_name }} ~ ('[ÃÂ][^[:ascii:]À-Þ]|â€|' || chr(65533))
    group by {{ column_name }}
    order by ocorrencias desc

{% endmacro %}
