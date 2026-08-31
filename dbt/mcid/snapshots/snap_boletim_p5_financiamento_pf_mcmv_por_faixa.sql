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

{% snapshot snap_boletim_p5_financiamento_pf_mcmv_por_faixa %}
{{ config(
    target_schema='conjuntura_continuo_snapshots',
    unique_key='chave',
    strategy='check',
    check_cols='all',
) }}

select
    edicao || '|' || coalesce("faixa"::text, '') as chave,
    edicao,
    "faixa", "Trim. ano anterior — Nº UH", "Trim. ano anterior — FIN (Bi R$)", "Trim. selecionado — Nº UH", "Trim. selecionado — FIN (Bi R$)"
from {{ ref('gold_boletim_p5_financiamento_pf_mcmv_por_faixa') }}

{% endsnapshot %}
