{% macro arquivo_mais_recente(padrao, excluir=[]) -%}
    {#-
        Subquery que devolve UM arquivo: o mais recente entre os que casam com o padrão.

        É a peça comum dos models da bronze, que são todos `select *` do parquet vencedor:
        texto puro, sem tipagem (isso é da prata), lidos com `union_by_name => true` para
        que competências de layout diferente convivam no mesmo glob sem quebrar a leitura.

        A staging guarda todas as competências já recebidas do mesmo dado
        (MONIT_CAD_PJ_FAR_MENSAL_202601_*, _202602_*, ...), e cada arquivo é um retrato
        completo, não um delta. Ler o glob inteiro empilharia retratos; a bronze quer só
        o último.

        A ordenação é pelo NOME do arquivo, não pelo caminho: as competências ficam
        espalhadas em pastas diferentes e o caminho pesaria mais que a data. Como o nome
        tem prefixo fixo e data de largura fixa, ordem alfabética decrescente = mais
        recente primeiro.

        `excluir` tira da disputa os nomes que contêm um dos termos. É necessário quando
        a origem manda variante do mesmo dado sob nome parecido: o INT059 tem remessas
        `..._EMPREENDIMENTOS_VALIDACAO_<data>` que, em ordem alfabética, vencem a remessa
        boa (`V` vem depois de qualquer dígito) e trariam dado de teste para a bronze.

            {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO"]) }}
    -#}
    (
        select arquivo
        from
            (
                select distinct cast(arq['filename'] as varchar) as arquivo
                from
                    read_parquet(
                        '{{ padrao }}', filename => true, union_by_name => true
                    ) as arq
            ) as candidatos
        where
            true
            {%- for termo in excluir %} and arquivo not like '%{{ termo }}%' {%- endfor %}
        order by split_part(arquivo, '/', -1) desc
        limit 1
    )
{%- endmacro %}
