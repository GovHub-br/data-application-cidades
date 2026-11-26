{#
    Dá a um teste singular os pais que ele não tem por `ref()`: os que leem o
    `information_schema` ou montam o SQL a partir do grafo. Sem pai, o Cosmos não agenda o
    teste (ele nunca roda no mcid_cosmos_dag) e o `dbt build` pode rodá-lo antes de as
    tabelas que ele vigia existirem.

        {{ depende_de(['ouro_x', 'ouro_y']) }}

    O `ref()` registra a dependência já no parse; a macro não escreve nada no SQL. Model
    novo no recorte do teste entra na lista: sem ele, o teste pode rodar antes da tabela e
    conferir a versão anterior. O tests/test_cosmos_grupos.py compara cada lista com o
    recorte calculado do grafo e falha quando falta ou sobra model.

    Com DOIS ou mais pais, o Cosmos põe o teste numa task própria, depois de todos, e a
    falha dele não bloqueia nenhum model. Com UM pai só, ele entra na task de teste desse
    model, e os filhos do model passam a esperar por ele: uma falha trava o que vem depois.
    Por isso o dhist_familias_cobertura e o sem_recurso_no_schema_de_fallback, que não
    vigiam um model específico, ficaram sem pai.
#}
{% macro depende_de(modelos) -%}
    {%- for modelo in modelos -%} {%- set _ = ref(modelo) -%} {%- endfor -%}
{%- endmacro %}


{#
    Folhas da conjuntura: os models de `conjuntura_dbt` sem filho dentro do domínio. Todo
    model do domínio é ancestral de uma delas, então depender delas garante que a conjuntura
    inteira já rodou. Serve aos testes que vigiam o domínio todo pelo grafo
    (`relacoes_do_produto`). Model novo que não tenha filho na conjuntura entra aqui.
#}
{% macro folhas_conjuntura() %}
    {{
        return(
            [
                "ouro_conjuntura_boletim_p6_ogu",
                "ouro_conjuntura_boletim_valores",
                "ouro_conjuntura_canal_fgts",
                "ouro_conjuntura_fgts_valor_medio",
                "ouro_conjuntura_funding",
                "ouro_conjuntura_fundo_social",
                "ouro_conjuntura_novos_financiamentos_banco",
                "ouro_conjuntura_pib_construcao_civil",
                "ouro_conjuntura_qualidade_completude",
                "ouro_conjuntura_qualidade_inventario",
                "ouro_conjuntura_qualidade_schema_drift",
                "prata_conjuntura_empresas_balanco_lancamentos_vendas",
                "prata_conjuntura_ibge_paic_obras",
                "prata_conjuntura_ibge_paic_pessoal_salarial",
                "prata_conjuntura_ibge_paic_resultados",
                "prata_conjuntura_ibge_pib_consolidado_trimestral",
                "prata_conjuntura_ibge_pib_corrente",
                "prata_conjuntura_ibge_pnadc_populacao_decis_renda",
                "prata_conjuntura_ibge_pnadc_rendimento_domiciliar",
                "prata_conjuntura_sbpe_financiamentos_aquisicao",
            ]
        )
    }}
{% endmacro %}
