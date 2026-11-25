-- O de-para da silver histórica como tabela: para cada família, cada coluna da silver com o
-- tipo e as colunas do cleansed de onde ela vem (em ordem de preferência), e as colunas do
-- cleansed descartadas. É a decisão sobre os nomes; a proposta e a evidência ficam no grafo
-- (bronze_dhist_colunas_*), e o teste dhist_de_para_grafo confere que os dois concordam.
{%- set linhas = [] %}
{%- for f, dp in de_para_dhist().items() %}
    {%- for c in dp["colunas"] %}
        {%- for o in c["origens"] %}
            {%- do linhas.append(
                "('"
                ~ f
                ~ "', '"
                ~ c["nome"]
                ~ "', '"
                ~ c["tipo"]
                ~ "', "
                ~ loop.index
                ~ ", '"
                ~ o
                ~ "')"
            ) %}
        {%- endfor %}
    {%- endfor %}
    {%- for o in dp["descartar"] %}
        {%- do linhas.append("('" ~ f ~ "', null, 'descartada', null, '" ~ o ~ "')") %}
    {%- endfor %}
{%- endfor %}
select familia, coluna, tipo, ordem_origem::integer as ordem_origem, coluna_cleansed
from (values {{ linhas | join(", ") }}) as t(familia, coluna, tipo, ordem_origem, coluna_cleansed)
