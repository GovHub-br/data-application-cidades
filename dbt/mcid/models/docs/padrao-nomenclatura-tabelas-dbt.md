# Padrão de Nomenclatura e Organização de Tabelas dbt — MCID

Documento normativo. Complementa `arquitetura-medalhao-mcid.md` (o *quê* de cada
camada) definindo o *como* nomear e onde materializar. Onde houver conflito com o
ADR de arquitetura (issue #117), o ADR prevalece.

> Aplica-se a **todo modelo novo**. Modelos existentes fora do padrão migram de
> forma controlada — ver seção 8.

> **Atualização 2026-09-11**: o prefixo/schema global de camada é **português**
> (`bronze`/`prata`/`ouro`), não `bronze`/`silver`/`gold`. A versão anterior
> deste documento (e a `migracao-bronze-minio-mcmv`, issue #119, que a
> originou) planejava schemas globais em inglês — mas o prod real já
> convergiu para `bronze`/`prata`/`ouro` em **todos** os domínios (far, fds,
> rural, histórico, reloginho): os schemas `silver`/`gold` têm **zero**
> tabelas em prod, assim como os schemas por-domínio legados
> (`empreendimento_far`, `empreendimentos_fds`, `dados_historicos`,
> `mcmv_historico`, `reloginho`, `entidades_fds`). Verificado por consulta
> direta a `information_schema.tables`/`.columns` em prod (contagem por
> schema: `bronze` 72, `ouro` 103, `prata` 70, `seeds` 14 — nada em
> `silver`/`gold`/schemas por-domínio). Este documento foi reescrito para
> refletir essa realidade; onde a seção 8 antiga dizia "Feito" com schema
> inglês, o de-fato hoje é português.

---

## 1. Princípios

1. **Um schema por camada, global.** Existem exatamente três schemas de dados:
   `bronze`, `prata` e `ouro`. Toda tabela é gravada no schema da sua camada,
   **independente do domínio**.
2. **O domínio vive no nome da tabela e na pasta**, nunca no schema.
3. **Toda tabela é prefixada pela camada** (`bronze_`, `prata_`, `ouro_`).
4. **A pasta do modelo reflete camada e domínio**
   (`models/<dominio>_dbt/<camada>/`, pastas em português: `bronze/`, `prata/`, `ouro/`).
5. **`snake_case`** em tudo: pasta, arquivo, schema, coluna, alias de CTE.
6. **Materialização `table`** em todas as camadas (decisão do time em
   2026-08-29: `materialized_view` derrubava o banco — não reverter).
7. **Governança declarada no `dbt_project.yml`** via `+meta`, herdada por todos os
   modelos da pasta (ver seção 5).

---

## 2. Domínios

O *domínio* é a área de negócio, não a fonte. Um domínio agrupa bronze + prata +
ouro de um mesmo produto de dados. Ele aparece em:

- **pasta**: `models/<dominio>_dbt/`;
- **nome da tabela**: como token de desambiguação (seção 4);
- **`+meta.governance.product`** no `dbt_project.yml`.

| Domínio (`product`) | Token no nome de tabela | Conteúdo |
|---|---|---|
| `conjuntura` | `conjuntura` | Séries macro do mercado imobiliário e construção civil |
| `empreendimento_far` | `far` | Empreendimentos MCMV frente FAR |
| `empreendimento_fds` | `fds` | Empreendimentos MCMV frente Entidades (FDS) |
| `empreendimento_rural` | `rural` | Empreendimentos MCMV frente Rural (PNHR) |
| `reloginho` (`indicadores_mcmv_dbt`) | bronze: `dhist`; prata/ouro: `reloginho` | Reloginho (grupo A), gargalo/desempenho (grupo B) |
| `mcmv_historico` (`mcmv_historico_dbt`) | bronze: `dhist`/`sftp`/`shpt` (origem); prata/ouro: `dhist`/`far`/`rural`/`fds` (domínio) | Séries históricas multi-mês (pré-2024, backtest, análise preditiva) |

O token vem **imediatamente após** o prefixo de camada (seção 4) — `bronze_far_…`,
`prata_dhist_…` — nunca como sufixo.

Novo domínio ⇒ registrar nesta tabela **e** criar o bloco correspondente no
`dbt_project.yml`.

---

## 3. Schemas do banco de dados

| Camada | Schema | Observação |
|---|---|---|
| Bronze | `bronze` | Cópia fiel da staging. Toda tabela `bronze_*`, de qualquer domínio |
| Prata | `prata` | Camada tratada. Toda tabela `prata_*`, de qualquer domínio |
| Ouro | `ouro` | Marts e indicadores. Toda tabela `ouro_*`, de qualquer domínio |
| Qualidade | `ouro` | Tabelas de resultado de checagem (`quality_*`) — no schema `ouro`, marcadas `classification: restricted` e `rag_publication: prohibited` |

- **Não** existe schema por domínio (`conjuntura_bronze`, `empreendimento_far`,
  `empreendimentos_fds`, `dados_historicos`, `mcmv_historico`, `reloginho`,
  `entidades_fds` etc. são legado, vazios em prod — seção 8).
- **Não** existe schema `mart` / `_continuo`. Ouro é `ouro`.
- Como os três schemas são compartilhados por todos os domínios, **o nome da
  tabela precisa ser único dentro do schema** — daí o token de domínio na
  seção 4.

---

## 4. Nomes de tabela (modelo dbt)

Formato:

```
<camada>_<token-dominio>_<assunto>[_<recorte>]
```

- `<camada>` ∈ `bronze` | `prata` | `ouro` | `quality`.
- `<token-dominio>` — token da seção 2 (`far`, `fds`, `rural`, `conjuntura`,
  `reloginho`, `mcmv_historico`, ...), **logo após a camada**.
- `<assunto>` — substantivo do que a tabela representa (`consolidado`,
  `empreendimento`, `evolucao_financeira`, `serie_mensal`).
- `<recorte>` — desambiguação adicional: agregação, consumidor ou (no caso do
  domínio `mcmv_historico`, cujo token não é a frente) a própria frente
  (`_uf`, `_mensal`, `_chart`, `_dashboard`, `_far`, `_fds`, `_rural`).

**O nome precisa ser único no schema da camada.** Como o token de domínio é
obrigatório e vem logo após a camada, `<assunto>` sozinho nunca colide:

- `prata_empreendimento` ❌ (sem token; colidiria entre FAR, FDS, Rural)
- `prata_far_empreendimento`, `prata_fds_empreendimento`,
  `prata_rural_empreendimento` ✅
- `prata_far_historico_empreendimento` ✅ (token `mcmv_historico`; a frente
  `_far` é recorte porque o token do domínio histórico não é a frente)

O nome do arquivo `.sql` **é** o nome da tabela. Não usar `alias`.

### Exemplos por camada

| Camada | Bom | Evitar |
|---|---|---|
| Bronze | `bronze_far_consolidado`, `bronze_reloginho_snh_serie_mensal` | `consolidado`, `bronze_consolidado_far`, `far_raw`, `stg_far` |
| Prata | `prata_far_empreendimento`, `prata_reloginho_snh_apf_mes` | `empreendimento`, `prata_empreendimento_far`, `empreendimento_tratado`, `silver_far_empreendimento` |
| Ouro | `ouro_far_evolucao_financeira`, `ouro_far_ficha_empreendimento`, `ouro_mcmv_historico_serie_mensal` | `evolucao_financeira_chart` sem prefixo, `ouro_ficha_empreendimento_far`, `mart_ficha`, `gold_far_ficha_empreendimento` |
| Qualidade | `quality_reloginho_reconciliacao_66`, `quality_far_completude` | `assert_*` como modelo (isso é teste, fica em `tests/`) |

### Regras de coluna

- `snake_case`, termo canônico do `glossario-mcid.md`.
- Código com zero à esquerda (IBGE, CNPJ) ⇒ `text`.
- Chave lógica ⇒ `not_null`; se grão 1:1, também `unique`.
- Campos técnicos: ver seção 6.

---

## 5. Configuração no `dbt_project.yml`

Cada domínio é um bloco sob `models: mcid:`. A camada define **sempre** o mesmo
schema global (`bronze`/`prata`/`ouro`), a materialização e a governança; os
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

      prata:
        +materialized: table
        +schema: prata
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: prata
            classification: internal
            rag_publication: eligible_after_security_validation
          openmetadata:
            domain: MCid.Habitacao
            tier: Tier.Tier2
            owner: mcid-data-engineering

      ouro:
        +materialized: table
        +schema: ouro
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: ouro
            classification: internal
            rag_publication: eligible_after_security_validation
          openmetadata:
            domain: MCid.Habitacao
            tier: Tier.Tier1
            owner: mcid-data-engineering

      qualidade:
        +materialized: table
        +schema: ouro
        +meta:
          governance:
            product: empreendimento_rural
            owner_key: mcid_data_engineering
            layer: quality
            classification: restricted
            rag_publication: prohibited
```

> `+schema` é literal (`bronze`/`prata`/`ouro`) porque o projeto usa o
> `generate_schema_name_for_env` padrão (ver `macros/get_custom_schema.sql`):
> no target `prod` o schema custom é usado como está; no target `staging_duckdb`
> também é honrado literalmente (necessário pra ler o MinIO); nos demais
> targets de dev vira `<target>_bronze` etc.
>
> Um modelo isolado que ainda não teve o domínio inteiro migrado pode
> sobrepor só o próprio `+schema` via `config(schema="prata")` no `.sql`,
> sem mexer no bloco da pasta no `dbt_project.yml` — ver seção 8.

### Valores fixos de `+meta` por camada

| Chave | bronze | prata | ouro | qualidade |
|---|---|---|---|---|
| `+schema` | `bronze` | `prata` | `ouro` | `ouro` |
| `governance.layer` | `bronze` | `prata` | `ouro` | `quality` |
| `governance.classification` | `restricted` | `internal` | `internal` | `restricted` |
| `governance.rag_publication` | `prohibited` | `eligible_after_security_validation` | `eligible_after_security_validation` | `prohibited` |
| `openmetadata.tier` | `Tier.Tier3` | `Tier.Tier2` | `Tier.Tier1` | *(herda ouro)* |
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

| Campo | Bronze | Prata | Ouro | Função |
|---|:--:|:--:|:--:|---|
| `source_file` | ✅ | ✅ | — | Arquivo de origem na staging |
| `dt_ingest` | ✅ | ✅ | — | Momento da carga na bronze |
| `hash_linha` | ✅ | ✅ | — | Hash do conteúdo (dedup / detecção de mudança) |
| `dt_referencia` | ✅ | ✅ | ✅ | Período do snapshot — derivado do **nome do arquivo** |
| `dt_silver` / `dt_prata` | — | ✅ | — | `current_timestamp` da transformação prata |
| `dt_gold` / `dt_ouro` | — | — | ✅ | `current_timestamp` da materialização ouro |
| `id_negocio_historico` | — | ✅ | opcional | Chave lógica estável (programa + linha + período) |
| `is_current` / `dt_valid_from` / `dt_valid_to` | — | opcional | — | SCD2, só onde há versionamento explícito |

> Nomes de coluna técnica existentes (`dt_silver`, `dt_gold`) não precisam ser
> retroativamente renomeados só por causa da mudança de schema — são coluna,
> não schema/tabela; ver seção 8 sobre o que migra e o que não.

Bronze **não deduplica** e **não tipa** (tudo `text`/genérico). Prata é o único
lugar de achatamento, tipagem, domínio e dedup.

---

## 7. `schema.yml`

- Um `schema.yml` por pasta de camada.
- `description` do modelo **sempre** declara o **grão** em uma frase.
- `meta.tags: [<camada>]` em cada modelo (redundante com o `+meta` mas usado por
  seleção: `dbt build --select tag:bronze`).
- Testes mínimos:
  - bronze: `row_count_match` contra a fonte (quando houver tabela equivalente).
  - prata: `not_null` + `unique` na chave de grão; `accepted_values` em domínios.
  - ouro: teste de reconciliação / totalização quando houver referência oficial.
- Métrica derivada em ouro ⇒ fórmula documentada no `description` da coluna.

---

## 8. Modelos fora do padrão (migração controlada)

| Modelo / bloco | Desvio | Ação |
|---|---|---|
| `conjuntura_dbt` (schemas `conjuntura_bronze`/`_silver`/`_gold`) | Schema por domínio, inglês | Repontar `+schema` para `bronze`/`prata`/`ouro`; tabelas já têm prefixo de camada (precisa também renomear `silver_*`/`gold_*` → `prata_*`/`ouro_*`) |
| `empreendimento_far_dbt` / `empreendimento_fds_dbt` (schema único por domínio: `empreendimento_far` / `empreendimentos_fds`, prefixo `silver_`/`gold_`, muitas vezes com `alias` pro nome físico) | Schema por domínio, inglês; ambos os schemas legados estão **vazios em prod** — e o domínio inteiro é **mantido pelos colegas**, só copiado nesta branch pra teste de compilação (não é escopo deste projeto) | **Parcial** (2026-09-11): os modelos referenciados por `mcmv_historico_dbt`/`indicadores_mcmv_dbt` já foram repontados individualmente (`config(schema="prata"/"ouro")`, sem `alias`, arquivo renomeado `prata_*`/`ouro_*`) — ver `prata_fds_empreendimento`, `prata_far_evolucao_financeira`, `ouro_far_ficha_empreendimento`, `ouro_far_execucao_fisica_financeira_chart`, `ouro_fds_ficha_empreendimento`, `ouro_fds_evolucao_financeira_chart`. **Todos batem coluna a coluna, exatos, com as tabelas já publicadas em prod** — não são pipelines divergentes, só nomenclatura desatualizada localmente; **regra**: modelo copiado destes domínios reflete prod tal como está, não enriquece. `prata_fds_empreendimento` tinha `id_empreendimento`/`fase_empreendimento` e 12 colunas SNH adicionadas localmente (fora do padrão) — removidas 2026-09-11; a identidade de empreendimento que `mcmv_historico_dbt` precisa agora mora em `prata_fds_historico_dim_empreendimento` (`mcmv_historico_dbt/prata/`, dentro do escopo). Os modelos NÃO consumidos por fora do domínio (ex.: `gold_far_mapa_nacional`, `gold_far_panorama_estadual`, `gold_far_resumo_gerencial`, `gold_fds_panorama_entidade`, os bronzes, `silver_far_empreendimento`, `silver_fds_evolucao_financeira`) **ainda não foram migrados** — não mexer neles a menos que passem a ser consumidos por `mcmv_historico_dbt`/`indicadores_mcmv_dbt`. `seeds/entidades_fds/` (schema do seed `seed_apf_fase_fds`) segue como resíduo menor |
| `indicadores_mcmv_dbt/{bronze,prata,ouro}` (schema `mcmv_indicadores`) | Schema por domínio | **Feito** (`renomear-camadas-pt-historico-reloginho`): repontado pra `bronze`/`prata`/`ouro`, nomes já prefixados em português |
| `mcmv_historico_dbt/{bronze,prata,ouro}` (schema `mcmv_historico`) | Schema por domínio | **Feito** (`renomear-camadas-pt-historico-reloginho`): repontado pra `bronze`/`prata`/`ouro`, nomes já prefixados em português. `seeds/mcmv_historico/` (schema do seed do piloto) segue como resíduo menor |

**Regra de renome físico** (issue #119): nunca renomear tabela — ou schema —
consumida por dashboard sem uma view/alias de compatibilidade no nome antigo, e
sem validar no Superset: row count antes/depois, campos de cards/filtros/mapas,
data máxima de referência, cards sem erro. Os 8 modelos migrados em
2026-09-11 (linha acima) nunca tiveram linha materializada em prod sob o nome
antigo (schemas `empreendimento_far`/`empreendimentos_fds`/`silver`/`gold`
vazios) — checado por consulta direta a `information_schema` antes do
rename, então a view de compatibilidade não se aplicou.

---

## 9. Checklist para tabela nova

- [ ] Domínio existe na seção 2 (senão, registrar + criar bloco no `dbt_project.yml`).
- [ ] Pasta: `models/<dominio>_dbt/<camada>/` (camada em português: `bronze/`, `prata/`, `ouro/`).
- [ ] Arquivo `<camada>_<token-dominio>_<assunto>[_<recorte>].sql` — sem `alias`.
- [ ] Nome único dentro do schema da camada (token de domínio logo após a camada).
- [ ] `{{ config(materialized="table") }}` (ou herdado).
- [ ] `+schema` da camada = `bronze` / `prata` / `ouro`.
- [ ] Bronze: cópia fiel, sem tipagem, sem dedup; `source_file`, `dt_ingest`, `hash_linha`, `dt_referencia`.
- [ ] Prata: tipagem + domínio + dedup; grão declarado; `dt_silver`/`dt_prata`.
- [ ] Ouro: regra de negócio; grão declarado; `dt_gold`/`dt_ouro`.
- [ ] Entrada no `schema.yml` com `description` (grão), `meta.tags`, testes mínimos.
- [ ] Leitura de `staging/` ⇒ `+enabled: target.type == 'duckdb'`.
- [ ] Credenciais MinIO **fora do commit** — usar `.env` / `profiles.yml` local.
- [ ] Antes de assumir que um nome/schema está vazio ou livre em prod: **checar
      prod de verdade** (`information_schema.tables`/`.columns`, somente
      leitura) — não confiar só na documentação ou no `dbt_project.yml` local,
      que podem estar desatualizados (foi exatamente esse o caso desta seção
      até 2026-09-11).

---

## 10. Validação

```bash
cd dbt/mcid
dbt parse
dbt build --select <dominio>_dbt.bronze <dominio>_dbt.prata --target staging_duckdb
dbt build --select tag:ouro --target prod
dbt docs generate
```

Antes de renome físico (tabela ou schema) usado por dashboard:

```bash
dbt run  --select +<mart>
dbt test --select +<mart>
```
