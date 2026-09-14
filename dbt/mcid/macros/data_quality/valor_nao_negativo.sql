{#-
  Teste genérico: retorna as linhas em que uma coluna de valor financeiro é
  menor que zero.

  Valor negativo em campo de repasse/investimento/subsídio é sempre defeito de
  origem (estorno mal tipado, sinal invertido, placeholder). A convenção do
  eixo (glossario-valores-financeiros.md, D6):

    - BRONZE: severidade `warn` — o bronze é cópia fiel; o negativo fica visível
      com a contagem, o `dbt build` conclui.
    - SILVER: severidade `error` — a silver já passou pela quarentena
      (seed quarentena_valores_financeiros.csv). Uma falha aqui = registro novo
      a triar (entra no seed com motivo, ou a regra é ajustada).

  Só sinaliza; nunca zera nem anula (isso esconderia casos novos).

  A coluna é comparada via `try_cast(... as double)` — funciona tanto no bronze
  (valores em texto, formato dot-decimal) quanto no silver (numérico já tipado).
  Texto em formato brasileiro no bronze pode escapar do try_cast; onde isso
  importar, aplicar o teste sobre a coluna já parseada.

  Uso no schema.yml:

      columns:
        - name: valor_investimento
          data_tests:
            - valor_nao_negativo:
                config:
                  severity: error   # warn no bronze
-#}
{% macro test_valor_nao_negativo(model, column_name) %}

select
    {{ column_name }} as valor,
    count(*) as ocorrencias
from {{ model }}
where try_cast({{ column_name }} as double) < 0
group by {{ column_name }}
order by valor

{% endmacro %}
