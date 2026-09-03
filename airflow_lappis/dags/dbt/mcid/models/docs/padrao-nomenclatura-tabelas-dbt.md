# Padrão de Nomenclatura e Organização de Tabelas dbt — MCID

Documento normativo. Complementa `arquitetura-medalhao-mcid.md` (o *quê* de cada
camada) definindo o *como* nomear e onde materializar. Onde houver conflito com o
ADR de arquitetura (issue #117), o ADR prevalece.

> Aplica-se a **todo modelo novo**. Modelos existentes fora do padrão migram de
> forma controlada — ver seção 8.

---

## 1. Princípios

1. **Um schema por camada, global.** Existem exatamente três schemas de dados:
   `bronze`, `silver` e `gold`. Toda tabela é gravada no schema da sua camada,
   **independente do domínio**.
2. **O domínio vive no nome da tabela e na pasta**, nunca no schema.
3. **Toda tabela é prefixada pela camada** (`bronze_`, `silver_`, `gold_`).
4. **A pasta do modelo reflete camada e domínio**
   (`models/<dominio>_dbt/<camada>/`).
5. **`snake_case`** em tudo: pasta, arquivo, schema, coluna, alias de CTE.
6. **Materialização `table`** em todas as camadas (decisão do time em
   2026-08-29: `materialized_view` derrubava o banco — não reverter).
7. **Governança declarada no `dbt_project.yml`** via `+meta`, herdada por todos os
   modelos da pasta (ver seção 5).

---

## 2. Domínios

O *domínio* é a área de negócio, não a fonte. Um domínio agrupa bronze + silver +
gold de um mesmo produto de dados. Ele aparece em:

- **pasta**: `models/<dominio>_dbt/`;
- **nome da tabela**: como token de desambiguação (seção 4);
- **`+meta.governance.product`** no `dbt_project.yml`.

| Domínio (`product`) | Token no nome de tabela | Conteúdo |
|---|---|---|
| `conjuntura` | `conjuntura` | Séries macro do mercado imobiliário e construção civil |
| `empreendimento_far` | `far` | Empreendimentos MCMV frente FAR |
| `empreendimento_fds` | `fds` | Empreendimentos MCMV frente Entidades (FDS) |
| `empreendimento_rural` | `rural` | Empreendimentos MCMV frente Rural (PNHR) |
| `indicadores_mcmv` | `reloginho`, `gargalo`, ... | Reloginho (grupo A), gargalo/desempenho (grupo B) |
| `mcmv_historico` | `mcmv` | Séries históricas multi-mês (pré-2024, backtest, análise preditiva) |

Novo domínio ⇒ registrar nesta tabela **e** criar o bloco correspondente no
`dbt_project.yml`.

---

## 3. Schemas do banco de dados

| Camada | Schema | Observação |
|---|---|---|
| Bronze | `bronze` | Cópia fiel da staging. Toda tabela `bronze_*`, de qualquer domínio |
| Silver | `silver` | Camada tratada. Toda tabela `silver_*`, de qualquer domínio |
| Gold | `gold` | Marts e indicadores. Toda tabela `gold_*`, de qualquer domínio |
| Qualidade | `gold` | Tabelas de resultado de checagem (`quality_*`) — no schema `gold`, marcadas `classification: restricted` e `rag_publication: prohibited` |

- **Não** existe schema por domínio (`conjuntura_bronze`, `empreendimento_far`
  etc. são legado — seção 8).
- **Não** existe schema `mart` / `_continuo`. Gold é `gold`.
- Como os três schemas são compartilhados por todos os domínios, **o nome da
  tabela precisa ser único dentro do schema** — daí o token de domínio na
  seção 4.

---

## 4. Nomes de tabela (modelo dbt)

Formato:

```
<camada>_<assunto>[_<recorte>]
```

- `<camada>` ∈ `bronze` | `silver` | `gold` | `quality`.
- `<assunto>` — substantivo do que a tabela representa (`consolidado`,
  `empreendimento`, `evolucao_financeira`, `serie_mensal`).
- `<recorte>` — frente, domínio, agregação ou consumidor
  (`_far`, `_fds`, `_rural`, `_uf`, `_mensal`, `_chart`, `_dashboard`).

**O nome precisa ser único no schema da camada.** Se `<assunto>` sozinho puder
colidir entre domínios/frentes, o token de domínio é **obrigatório**:

- `silver_empreendimento` ❌ (colide entre FAR, FDS, Rural)
- `silver_empreendimento_far`, `silver_empreendimento_fds`,
  `silver_empreendimento_rural` ✅

O nome do arquivo `.sql` **é** o nome da tabela. Não usar `alias`.

### Exemplos por camada

| Camada | Bom | Evitar |
|---|---|---|
| Bronze | `bronze_consolidado_far`, `bronze_reloginho_snh_serie_mensal` | `consolidado`, `far_raw`, `stg_far` |
| Silver | `silver_empreendimento_far`, `silver_reloginho_snh_apf_mes` | `empreendimento`, `empreendimento_tratado` |
| Gold | `gold_evolucao_financeira_far`, `gold_ficha_empreendimento_far`, `gold_serie_historica_mensal` | `evolucao_financeira_chart` sem prefixo, `mart_ficha` |
| Qualidade | `quality_reloginho_reconciliacao_66`, `quality_far_completude` | `assert_*` como modelo (isso é teste, fica em `tests/`) |

### Regras de coluna

- `snake_case`, termo canônico do `glossario-mcid.md`.
- Código com zero à esquerda (IBGE, CNPJ) ⇒ `text`.
- Chave lógica ⇒ `not_null`; se grão 1:1, também `unique`.
- Campos técnicos: ver seção 6.

---

## 5. Configuração no `dbt_project.yml`

Cada domínio é um bloco sob `models: mcid:`. A camada define **sempre** o mesmo
schema global (`bronze`/`silver`/`gold`), a materialização e a governança; os
modelos herdam. O que muda entre domínios é só `governance.product`.

```yaml
    empreendimento_rural_dbt:
      +materialized: table
      +meta:
        governance:
          product: empreendimento_rural
          owner_key: mcid_data_engineering

      bronze:
        +materialized: table
        +schema: bronze
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: bronze
            classification: restricted
            rag_publication: prohibited
          openmetadata:                       # lido pelo conector dbt do OpenMetadata
            domain: MCid.Habitacao
            tier: Tier.Tier3
            owner: mcid-data-engineering

      silver:
        +materialized: table
        +schema: silver
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: silver
            classification: internal
            rag_publication: eligible_after_security_validation
          openmetadata:
            domain: MCid.Habitacao
            tier: Tier.Tier2
            owner: mcid-data-engineering

      gold:
        +materialized: table
        +schema: gold
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: gold
            classification: internal
            rag_publication: eligible_after_security_validation
          openmetadata:
            domain: MCid.Habitacao
            tier: Tier.Tier1
            owner: mcid-data-engineering

      qualidade:
        +materialized: table
        +schema: gold
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: quality
            classification: restricted
            rag_publication: prohibited
```

> `+schema` é literal (`bronze`/`silver`/`gold`) porque o projeto usa o
> `generate_schema_name_for_env` padrão: no target `prod` o schema custom é usado
> como está; em targets de dev vira `<target>_bronze` etc.

### Valores fixos de `+meta` por camada

| Chave | bronze | silver | gold | qualidade |
|---|---|---|---|---|
| `+schema` | `bronze` | `silver` | `gold` | `gold` |
| `governance.layer` | `bronze` | `silver` | `gold` | `quality` |
| `governance.classification` | `restricted` | `internal` | `internal` | `restricted` |
| `governance.rag_publication` | `prohibited` | `eligible_after_security_validation` | `eligible_after_security_validation` | `prohibited` |
| `openmetadata.tier` | `Tier.Tier3` | `Tier.Tier2` | `Tier.Tier1` | *(herda gold)* |
| `openmetadata.domain` | `MCid.Habitacao` | `MCid.Habitacao` | `MCid.Habitacao` | `MCid.Habitacao` |
| `openmetadata.owner` / `governance.owner_key` | `mcid-data-engineering` / `mcid_data_engineering` | idem | idem | idem |

### Gating por motor

Modelos que leem `staging/` no MinIO só rodam no DuckDB. Aplicar no nível da
camada ou do modelo:

```yaml
      bronze:
        +enabled: "{{ target.type == 'duckdb' }}"
```

O `+database` já é resolvido no topo (`mcid:`):
`{{ 'cidades' if target.type == 'postgres' else 'mcid_staging' }}`.

---

## 6. Campos técnicos por camada

Alinhado com `arquitetura-medalhao-mcid.md` §5 e o piloto #118.

| Campo | Bronze | Silver | Gold | Função |
|---|:--:|:--:|:--:|---|
| `source_file` | ✅ | ✅ | — | Arquivo de origem na staging |
| `dt_ingest` | ✅ | ✅ | — | Momento da carga na bronze |
| `hash_linha` | ✅ | ✅ | — | Hash do conteúdo (dedup / detecção de mudança) |
| `dt_referencia` | ✅ | ✅ | ✅ | Período do snapshot — derivado do **nome do arquivo** |
| `dt_silver` | — | ✅ | — | `current_timestamp` da transformação silver |
| `dt_gold` | — | — | ✅ | `current_timestamp` da materialização gold |
| `id_negocio_historico` | — | ✅ | opcional | Chave lógica estável (programa + linha + período) |
| `is_current` / `dt_valid_from` / `dt_valid_to` | — | opcional | — | SCD2, só onde há versionamento explícito |

Bronze **não deduplica** e **não tipa** (tudo `text`/genérico). Silver é o único
lugar de achatamento, tipagem, domínio e dedup.

---

## 7. `schema.yml`

- Um `schema.yml` por pasta de camada.
- `description` do modelo **sempre** declara o **grão** em uma frase.
- `meta.tags: [<camada>]` em cada modelo (redundante com o `+meta` mas usado por
  seleção: `dbt build --select tag:bronze`).
- Testes mínimos:
  - bronze: `row_count_match` contra a fonte (quando houver tabela equivalente).
  - silver: `not_null` + `unique` na chave de grão; `accepted_values` em domínios.
  - gold: teste de reconciliação / totalização quando houver referência oficial.
- Métrica derivada em gold ⇒ fórmula documentada no `description` da coluna.

---

## 8. Modelos fora do padrão (migração controlada)

| Modelo / bloco | Desvio | Ação |
|---|---|---|
| `conjuntura_dbt` (schemas `conjuntura_bronze`/`_silver`/`_gold`) | Schema por domínio | Repontar `+schema` para `bronze`/`silver`/`gold`; tabelas já têm prefixo de camada. View de compat nos nomes de schema antigos enquanto o Superset migra |
| `empreendimento_far_dbt` (schema único `empreendimento_far`) | Schema por domínio; `silver/empreendimento.sql` e `evolucao_financeira.sql` sem prefixo; golds `*_chart`/`ficha_*`/`panorama_*` sem prefixo | Mover para `bronze`/`silver`/`gold`; renomear tabelas com prefixo + token `_far`; view/alias de compat até migrar cards |
| `entidades_dbt` (schema `entidades_fds`, prefixo `fds_`) | Schema por domínio; domínio deveria ser `empreendimento_fds` | Renomear domínio; mover para schemas globais; manter token `_fds` no nome da tabela |
| `indicadores_mcmv_dbt/{bronze,silver,gold}` (schema `mcmv_indicadores`) | Schema por domínio | Repontar para `bronze`/`silver`/`gold`; nomes já prefixados |
| ~~`mcmv_historico_dbt/{piloto,empreendimentos,serie_executiva}`~~ | Sem prefixo de camada; schema `mcmv_historico` | **Feito** (`separacao-silver-historico-por-frente`): modelos classificados em `bronze/silver/gold/`, renomeados com prefixo + token `mcmv_historico`, schemas globais via `get_custom_schema` também no DuckDB. `seeds/mcmv_historico/` (schema do seed do piloto) segue como resíduo menor |

**Regra de renome físico** (issue #119): nunca renomear tabela — ou schema —
consumida por dashboard sem uma view/alias de compatibilidade no nome antigo, e
sem validar no Superset: row count antes/depois, campos de cards/filtros/mapas,
data máxima de referência, cards sem erro.

---

## 9. Checklist para tabela nova

- [ ] Domínio existe na seção 2 (senão, registrar + criar bloco no `dbt_project.yml`).
- [ ] Pasta: `models/<dominio>_dbt/<camada>/`.
- [ ] Arquivo `<camada>_<assunto>[_<recorte>].sql` — sem `alias`.
- [ ] Nome único dentro do schema da camada (token de domínio se houver risco de colisão).
- [ ] `{{ config(materialized="table") }}` (ou herdado).
- [ ] `+schema` da camada = `bronze` / `silver` / `gold`.
- [ ] Bronze: cópia fiel, sem tipagem, sem dedup; `source_file`, `dt_ingest`, `hash_linha`, `dt_referencia`.
- [ ] Silver: tipagem + domínio + dedup; grão declarado; `dt_silver`.
- [ ] Gold: regra de negócio; grão declarado; `dt_gold`.
- [ ] Entrada no `schema.yml` com `description` (grão), `meta.tags`, testes mínimos.
- [ ] Leitura de `staging/` ⇒ `+enabled: target.type == 'duckdb'`.
- [ ] Credenciais MinIO **fora do commit** — usar `.env` / `profiles.yml` local.

---

## 10. Validação

```bash
cd airflow_lappis/dags/dbt/mcid
dbt parse
dbt build --select <dominio>_dbt.bronze <dominio>_dbt.silver --target staging_duckdb
dbt build --select tag:gold --target prod
dbt docs generate
```

Antes de renome físico (tabela ou schema) usado por dashboard:

```bash
dbt run  --select +<mart>
dbt test --select +<mart>
```
