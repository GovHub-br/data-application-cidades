# Checklist do modelo de documentação semântica

Baseado no ADR-0014 do `graphrag-tais` (modelo de documentação para o
GraphRAG). Vale para **qualquer** projeto dbt deste repositório
(`airflow_lappis/dags/dbt/mcid`, `.../ipea`, `.../mir`, ou outro futuro) — nada
aqui é específico do MCid.

## As camadas

| Camada | Construção dbt | Onde vive | O que resolve |
|---|---|---|---|
| 0. Tabela/Coluna | `schema.yml` — `description`, `tags` | Já existe hoje, ao lado do modelo | Descrição documental básica — **pré-requisito** das demais camadas |
| 1. Entidades | `semantic_models[].entities` | `semantic_models.yml`, ao lado do `schema.yml` da camada gold (ou silver, se não houver gold) | Chave/grain da tabela, de forma estruturada e testável |
| 2. Dimensões | `semantic_models[].dimensions` | idem | Atributos categóricos/temporais, resolvíveis por nome de negócio |
| 3. Medidas | `semantic_models[].measures` | idem | Fatos agregáveis, com `agg` explícito — evita *fan-out* |
| 4. Métricas | `metrics.yml`, mesmo diretório | KPIs de negócio nomeados, lastreados em medidas reais |
| 5. Glossário | `meta.synonyms` local, ou glossário centralizado do projeto | Sinônimos e siglas — linguagem natural → valor de domínio |

Nunca declare `entities`/`dimensions`/`measures`/`metrics` sobre um modelo
**bronze** (dado bruto, ainda não tipado/validado). Use gold; use silver só
quando não existir gold para o subject area.

Nunca declare `semantic_models` sobre um modelo **já pré-agregado/pivotado**
— isto é, um `.sql` cujo `SELECT` final tem `GROUP BY` que reduz o grain de
um modelo de origem mais atômico, ou `UNION ALL` que mistura mais de um
nível de agregação numa coluna genérica (ex.: `secao`/`dimensao`/`valor_1..N`
cujo significado depende de outra coluna da mesma linha). MetricFlow calcula
agregações a partir do grain atômico — declarar `measures`/`agg` em cima de
um rollup já materializado produz erros silenciosos (média de médias, dupla
contagem). Nesses casos: identifique o modelo de origem de grain atômico
(geralmente outro gold/silver na mesma pasta) e documente semântica **nele**;
marque o modelo pivotado como "fora de escopo do semantic layer — tabela de
serialização/rollup para BI, não candidata a `semantic_models`" no relatório,
sem tentar forçar `entities`/`measures` artificiais nele.

## Regras gerais (camadas 1–4)

- Nome de **negócio**, nunca físico/técnico (`total_financiamento_far`, não
  `sum_valor_col3`).
- `snake_case`, em português, sem abreviação não documentada — sigla sem
  explicação na `description` de origem vira pendência de glossário.
- `description` nunca vazia — mínimo uma frase em linguagem de negócio, que
  não repita o nome do campo.
- Sem colisão de nome entre `dimension` e `metric` no mesmo projeto — ambos
  precisam ser resolvíveis sem ambiguidade por quem (ou o quê) consome.
- Coluna **passthrough** (sem transformação no SQL do modelo — `select *`/
  seleção direta, mesmo nome e mesmo dado da camada de origem) herda a
  `description` da camada de origem como evidência válida de camada 0 — não
  precisa duplicar o texto no `schema.yml` da camada atual para virar
  candidata a entidade/dimensão/medida. O rascunho deve citar a origem
  (ex.: "herdado de `silver.empreendimento`").
- Coluna com **transformação real** no SQL (`case`, `concat`, cálculo,
  agregação) sempre exige `description` própria na camada onde a
  transformação ocorre — nunca herda a description de origem, porque o
  significado mudou.

## Checklist — Entidade (`entities`)

- [ ] `name` é o nome de negócio da chave, não um identificador técnico
      genérico (`id_1`, `pk`).
- [ ] `type` declarado: `primary` (grain desta tabela) | `foreign` | `unique`.
- [ ] `expr` aponta para a coluna física real.
- [ ] Se a chave exige normalização (ex.: remover máscara de documento), isso
      está explícito em `expr` — nunca implícito.
- [ ] Toda tabela tem **exatamente uma** entidade `primary`.
- [ ] Há evidência da chave na `description` original ou em `data_tests`
      (`unique`, `not_null`, teste customizado) — não é suposição.

## Checklist — Dimensão (`dimensions`)

- [ ] `type` sempre declarado: `categorical` ou `time`.
- [ ] Se `time`, `time_granularity` declarado.
- [ ] `description` explica o domínio de valores quando não óbvio (ex.:
      códigos `1`/`2`/`3`) — herdada da `description` original da coluna, não
      reescrita do zero.
- [ ] Sinônimos de negócio registrados em `meta.synonyms` quando existirem.

## Checklist — Medida (`measures`)

- [ ] `agg` sempre explícito — nunca herdar default implícito.
- [ ] `description` diz o que está sendo somado/contado e a unidade (R$,
      unidades, %).
- [ ] Contagem de entidade usa `count_distinct` na entidade, nunca `count(*)`.
- [ ] `agg` só foi escolhido quando a `description` original permitia decidir
      com segurança; caso contrário está marcado `PENDENTE_REVISAO_HUMANA`.

## Checklist — Métrica (`metrics`)

- [ ] `label` em português natural.
- [ ] `description` inclui a régua de negócio (período padrão, se aplicável).
- [ ] Toda métrica referencia uma **medida existente e documentada** — nunca
      uma expressão solta.

## Checklist — Glossário

- [ ] Toda sigla/abreviação usada em nome de entidade, dimensão, medida ou
      métrica tem entrada de glossário (mesmo curta).
- [ ] Sinônimo de linguagem natural relevante (ex.: "faturamento" → `receita`)
      registrado, não deixado para inferência do LLM em tempo de consulta.

## Exemplo de referência (MCid, `empreendimento_far_dbt`)

Camada 0 (já existe):

```yaml
- name: cadastro_pj
  description: >
    Cadastro detalhado de pessoa jurídica (empreendimento contratado).
  columns:
    - name: apf
      description: APF normalizado para 8 dígitos.
```

Camadas 1–3 (`semantic_models.yml`):

```yaml
semantic_models:
  - name: cadastro_pj
    model: ref('cadastro_pj')
    description: "Cadastro de PJ (empreendimento contratado) do FAR."
    entities:
      - name: apf
        type: primary
        expr: apf
    dimensions:
      - name: uf
        type: categorical
      - name: data_contratacao
        type: time
        type_params: {time_granularity: day}
    measures:
      - name: valor_far
        agg: sum
        description: "Valor total do empréstimo FAR, em R$."
      - name: qtd_empreendimentos
        agg: count_distinct
        expr: apf
        description: "Quantidade de empreendimentos distintos (por APF)."
```

Camada 4 (`metrics.yml`):

```yaml
metrics:
  - name: total_financiamento_far
    type: simple
    label: "Total de financiamento FAR"
    description: "Soma do valor FAR contratado no período."
    type_params: {measure: valor_far}
```
