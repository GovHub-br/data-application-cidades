-- Padrão de nomes da silver histórica: um conceito, um nome, em todas as famílias.
-- - Prefixo pelo tipo: `dt_` (ou `mes_`, mês) para data, `valor_` para dinheiro,
-- `quantidade_` para contagem, `percentual_`, `codigo_` para código, `nome_` para nome e
-- `ic_` para indicador. Os prefixos das fontes (`dat_`, `dte_`, `data_`, `vlr_`, `qtd_`,
-- `qt_`, `cod_`, `idc_`, `bln_`, `prc_`, `nm_`, `txt_`, `num_`, `uh_`, os húngaros da BEXT)
-- não entram na silver.
-- - Nomes únicos para as chaves e o território: `apf`, `codigo_empreendimento_bb`,
-- `codigo_contrato_pf`, `uf` (sigla; o nome por extenso é `nome_uf`), `municipio`,
-- `codigo_ibge_municipio`, `cnpj_proponente`, `agente_financeiro`, `produto`, `faixa`,
-- `origem`. Os sinônimos abaixo não entram.
-- - O mesmo nome tem o mesmo tipo em todas as famílias, e nenhum nome passa de 63 caracteres
-- (o Postgres cortaria e o nome no banco deixaria de ser o do de-para).
-- Devolve família, coluna e o motivo. SQL do Postgres, fora do envelope (lê o catálogo).
{%- set proibidos = "^(dat_|dte_|data_|vlr_|qtd_|qt_|quant_|iqde_|cod_|idc_|bln_|prc_|nm_|txt_|num_|uh_|unid_|unidades_|soma_de_|vnome_|vinstituicao|vporte_|cfaixa|csigla_|iprograma_)" %}
{%- set sinonimos = [
    "estado",
    "inst",
    "if_af",
    "instituicao",
    "ibge_codigo",
    "porte_de_municipio",
    "anomes",
    "anomes2",
    "vinculacoes",
    "codigo_proposta_pnhr",
    "cod_empreendimento",
    "empresa",
    "entregues",
    "comercializados",
    "desligados",
    "unidadeentregue",
] %}
{%- set tipos = {
    "dt_": ["data", "data_excel", "ano_mes"],
    "mes_": ["data", "data_excel", "ano_mes"],
    "valor_": ["valor", "valor_calculado"],
    "quantidade_": ["inteiro", "valor", "valor_calculado"],
    "percentual_": ["percentual", "percentual_auto"],
} %}
{%- set erros = [] %}
{%- for f, dp in de_para_dhist().items() %}
    {%- for c in dp["colunas"] %}
        {%- if modules.re.search(proibidos, c.nome) %}
            {%- do erros.append([f, c.nome, "prefixo fora do padrão"]) %}
        {%- endif %}
        {%- if c.nome | length > 63 %}
            {%- do erros.append([f, c.nome, "nome com mais de 63 caracteres (o Postgres corta)"]) %}
        {%- endif %}
        {%- if c.nome in sinonimos %}
            {%- do erros.append([f, c.nome, "sinônimo de nome canônico"]) %}
        {%- endif %}
        {%- for p, ts in tipos.items() if c.nome.startswith(p) and c.tipo not in ts %}
            {%- do erros.append([f, c.nome, "prefixo " ~ p ~ " com tipo " ~ c.tipo]) %}
        {%- endfor %}
        {%- if c.tipo in ["data", "data_excel", "ano_mes"] and not (
            c.nome.startswith("dt_") or c.nome.startswith("mes_")
        ) %}
            {%- do erros.append([f, c.nome, "data sem dt_ ou mes_"]) %}
        {%- endif %}
    {%- endfor %}
{%- endfor %}
{%- set rel = ref("prata_dhist_bext") %}
with

    regras as (
        {% if erros %}
            select *
            from
                (
                    values
                        {% for e in erros %}
                            ('{{ e[0] }}', '{{ e[1] }}', '{{ e[2] }}') {{- "," if not loop.last }}
                        {% endfor %}
                ) as t(familia, coluna, motivo)
        {% else %}
            select null::text as familia, null::text as coluna, null::text as motivo where false
        {% endif %}
    ),

    tipos_divergentes as (
        select
            null::text as familia,
            column_name::text as coluna,
            'mesmo nome com tipos diferentes: ' || string_agg(distinct data_type, ', ') as motivo
        from information_schema.columns
        where table_schema = '{{ rel.schema }}' and table_name like 'prata_dhist%'
        group by column_name
        having count(distinct data_type) > 1
    )

select *
from regras
union all
select *
from tipos_divergentes
