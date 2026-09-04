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

{% snapshot snap_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc %}
{{ config(
    target_schema='conjuntura',
    unique_key='chave',
    strategy='check',
    check_cols='all',
) }}

select
    edicao || '|' || coalesce("periodo"::text, '') as chave,
    edicao,
    "periodo", "INCC trimestral", "MRV trimestral", "Direcional trimestral", "Tenda trimestral", "INCC acum. 4T20", "MRV acum. 4T20", "Direcional acum. 4T20", "Tenda acum. 4T20"
from {{ ref('gld_boletim_p6_ticket_medio_das_unidades_lancadas_vs_incc') }}

{% endsnapshot %}
