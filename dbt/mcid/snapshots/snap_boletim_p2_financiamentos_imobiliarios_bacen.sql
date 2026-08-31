{#
  Congelamento das edições do boletim.

  Por que existe: as fontes revisam o passado. BACEN, IBGE, CAGED, CBIC e
  FipeZap reescrevem meses já publicados, e o OGU do boletim é um retrato do
  SIAFI numa data ("Dados de 02/01/26"). Rodar o dbt hoje devolve números
  diferentes dos que o boletim imprimiu — na conferência do 1T26, 62 de 194
  células divergiram por esse motivo.

  Um snapshot resolve melhor que copiar a tabela: ele guarda o HISTÓRICO das
  revisões, não só um retrato. Dá para ver as três versões que o 1T2026 teve e
  responder "por que o boletim publicou 22.623 e hoje o dado é 25.196".

  `strategy='check'` com `check_cols='all'`: não há coluna de atualização
  confiável na origem, então a mudança é detectada comparando os valores.

  Roda com `dbt snapshot`. Cada execução acrescenta versão só do que mudou.
#}

{% snapshot snap_boletim_p2_financiamentos_imobiliarios_bacen %}
{{ config(
    target_schema='conjuntura_continuo_snapshots',
    unique_key='chave',
    strategy='check',
    check_cols='all',
) }}

select
    edicao || '|' || coalesce("periodo"::text, '') as chave,
    edicao,
    "periodo", "PF Concessões (R$ mi)", "PF Taxa de Juros (%a.a)", "PF Inadimplência (%)", "PJ Concessões (R$ mi)", "PJ Taxa de Juros (%a.a)", "PJ Inadimplência (%)"
from {{ ref('gold_boletim_p2_financiamentos_imobiliarios_bacen') }}

{% endsnapshot %}
