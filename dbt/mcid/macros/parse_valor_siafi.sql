{#
    Converte valor monetário do SIAFI/Tesouro Gerencial pra numeric.

    Delega pro `parse_financial_value` (que já trata formato pt-BR
    "1.000.971,15" e string vazia), mas antes normaliza a notação contábil
    de negativo entre parênteses — "(6570011.00)" significa -6570011,00.
    Sem isso o cast quebra com
    'invalid input syntax for type numeric: "(6570011.00)"'.

    Fica separado do `parse_financial_value` de propósito: aquele macro é
    compartilhado com outros projetos do repo, e a notação de parênteses é
    específica do extrato do Tesouro Gerencial.
#}
{% macro parse_valor_siafi(column_name) %}
    {{ parse_financial_value(
        "case when trim(" ~ column_name ~ ") like '(%)' "
        ~ "then '-' || btrim(trim(" ~ column_name ~ "), '()') "
        ~ "else " ~ column_name ~ " end"
    ) }}
{% endmacro %}
