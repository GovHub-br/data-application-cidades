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
| `empreendimento_far` | `far` | Empreendimentos MCMV frente FAR (uma **frente**) |
| `empreendimento_fds` | `fds` | Empreendimentos MCMV frente Entidades (FDS) (uma **frente**) |
| `empreendimento_rural` | `rural` | Empreendimentos MCMV frente Rural (PNHR) (uma **frente**) |
| `reloginho` (`indicadores_mcmv_dbt`) | `reloginho` | Reloginho (grupo A), gargalo/desempenho (grupo B) — domínio, **não** é frente |
| `mcmv_historico` (`mcmv_historico_dbt`) | ver seção 4.1 — sem token fixo de domínio; usa o token da **frente** (`far`/`fds`/`rural`) ou nenhum, conforme a regra `historico` | Séries históricas multi-mês (pré-2024, backtest, análise preditiva), cross-frente por natureza |

`far`, `fds`, `rural` e `reloginho` são os quatro tokens de **domínio** aceitos em
`prata_`/`ouro_` (ver seção 4.1 — `conjuntura` também é domínio, mas com regras
próprias, sem o conceito de frente/histórico). O token de bronze é **fonte de
staging**, não domínio — ver seção 4.

O token vem **imediatamente após** o prefixo de camada (seção 4) — `bronze_shpt_…`,
`prata_far_…` — nunca como sufixo.

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

Formato geral:

```
<camada>_<token>_<assunto>[_<recorte>]
```

- `<camada>` ∈ `bronze` | `prata` | `ouro` | `quality`.
- `<assunto>` — substantivo do que a tabela representa (`consolidado`,
  `empreendimento`, `evolucao_financeira`, `serie_mensal`).
- `<recorte>` — desambiguação adicional: agregação ou consumidor (`_uf`,
  `_mensal`, `_chart`, `_dashboard`).

O que entra em `<token>` **difere por camada** — bronze usa a **fonte de
staging**; prata/ouro usam o **domínio**. Ver 4.1.

**Bronze — token de fonte.** Reflete a pasta de staging de origem, não o
domínio de negócio:

| Token | Pasta de staging | Exemplo local |
|---|---|---|
| `dhist` | `staging/dados_historicos/` | `bronze_dhist_serie_bases_relatorio_executivo` |
| `sftp` | `staging/sftp/` | `bronze_sftp_empreendimento_int040` |
| `shpt` | `staging/sharepoint/` | (domínio dos colegas — ver nota) |

O domínio dos colegas (`empreendimento_far_dbt`/`empreendimento_fds_dbt`/
`empreendimento_rural_dbt`) já publica em prod com um padrão de bronze mais
específico, `bronze_<fonte>_monit_<assunto>_<frente>_mensal` (ex.:
`bronze_shpt_monit_cad_pj_far_mensal`), além de nomes ad-hoc por tabela
(`bronze_shpt_his_mcidades_consolidado`,
`bronze_shpt_dados_prioritarios_snh_empreendimentos`). Isso é convenção deles,
fora deste documento — **antes de nomear ou renomear um bronze que espelha
staging/sharepoint desse domínio, confira o nome real em prod**
(`information_schema.tables`, schema `bronze`) em vez de aplicar
`bronze_shpt_<nometabela>` por dedução; nem toda tabela de lá tem
equivalente publicado ainda.

O nome do arquivo `.sql` **é** o nome da tabela. Não usar `alias`.

### 4.1 Prata e Ouro: domínio + histórico

`<token>` em prata/ouro é sempre um **domínio** (seção 2): `far`, `fds`,
`rural`, `reloginho` ou `conjuntura`. Três formatos, escolhidos pela natureza
do dado — não pela pasta onde o arquivo mora:

| Situação | Formato | Exemplo |
|---|---|---|
| Estado atual (current-state), de uma frente/domínio | `<camada>_<dominio>_<assunto>` | `prata_fds_empreendimento`, `ouro_far_ficha_empreendimento`, `ouro_reloginho_indicadores` |
| Série histórica multi-mês, mas de **uma única frente** (`far`/`fds`/`rural`) | `<camada>_<dominio>_historico_<assunto>` | `prata_far_historico_empreendimento`, `prata_fds_historico_empreendimento`, `prata_fds_historico_dim_empreendimento` |
| Série histórica multi-mês **cross-frente** (une FAR+FDS+Rural, ou não é específica de nenhuma frente) | `<camada>_historico_<assunto>` — **sem** token de domínio | `prata_historico_entrega_apf`, `prata_historico_serie_executiva`, `ouro_historico_serie_mensal`, `ouro_historico_marco_empreendimento` |

Regras de decisão:
- `reloginho` é domínio, não frente — nunca leva o infixo `historico` só por ser
  série mensal; usa sempre a 1ª linha (`prata_reloginho_…`/`ouro_reloginho_…`),
  **exceto** quando a tabela é genuinamente consumida cross-domínio (por
  `mcmv_historico_dbt` além do próprio reloginho) — aí vira a 3ª linha
  (`prata_historico_snh_entregas_mes`, consumida por
  `prata_historico_entrega_apf`). Antes de aplicar essa exceção, confirme o
  consumo real com `grep` — não deduza pela pasta.
- Um token que não é `far`/`fds`/`rural`/`reloginho`/`conjuntura` (ex.: `dhist`,
  que é fonte de bronze, não domínio) **nunca** aparece em nome de prata/ouro —
  se a tabela é cross-frente, o formato certo não leva token nenhum, é
  `<camada>_historico_<assunto>` direto.
- `mcmv_historico_dbt` (o domínio/pasta) não tem token próprio em prata/ouro —
  ele *é* o cross-frente da 3ª linha, ou empresta o token da frente na 2ª.

**O nome precisa ser único no schema da camada.** Como o formato certo (seção
4.1) já desambigua por domínio ou por `historico`, `<assunto>` sozinho nunca
colide:

- `prata_empreendimento` ❌ (sem domínio nem `historico`; colidiria entre FAR,
  FDS, Rural)
- `prata_far_empreendimento`, `prata_fds_empreendimento`,
  `prata_rural_empreendimento` ✅ (current-state, uma frente cada)
- `prata_far_historico_empreendimento` ✅ (série histórica, frente `far`)
- `prata_dhist_serie_anual_ogu_fgts` ❌ (`dhist` não é domínio válido em
  prata; é cross-frente ⇒ deveria ser `prata_historico_serie_anual_ogu_fgts`)

### Exemplos por camada

| Camada | Bom | Evitar |
|---|---|---|
| Bronze | `bronze_sftp_empreendimento_int040`, `bronze_dhist_serie_bases_relatorio_executivo` | `consolidado`, `bronze_consolidado_far`, `far_raw`, `stg_far` |
| Prata (current-state) | `prata_far_empreendimento`, `prata_reloginho_snh_apf_mes` | `empreendimento`, `prata_empreendimento_far`, `silver_far_empreendimento` |
| Prata (histórico, 1 frente) | `prata_far_historico_empreendimento`, `prata_fds_historico_dim_empreendimento` | `prata_dhist_far_empreendimento`, `prata_historico_far_empreendimento` (ordem trocada) |
| Prata (histórico, cross-frente) | `prata_historico_entrega_apf`, `prata_historico_serie_executiva` | `prata_dhist_entrega_apf`, `prata_mcmv_historico_entrega_apf` |
| Ouro | `ouro_far_evolucao_financeira`, `ouro_far_ficha_empreendimento`, `ouro_historico_serie_mensal` | `evolucao_financeira_chart` sem prefixo, `ouro_ficha_empreendimento_far`, `mart_ficha`, `gold_far_ficha_empreendimento` |
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
| `models/mcmv_historico_dbt/piloto/prata_dhist_serie_anual_ogu_fgts.sql` (`config(enabled=false)`) | Viola a regra 4.1: `dhist` usado como se fosse domínio em nome de prata | **Pendente**: renomear pra `prata_historico_serie_anual_ogu_fgts` (cross-frente, sem token). Achado 2026-09-11 verificando os 25 modelos prata/ouro do escopo contra prod — como está `enabled=false`, não há tabela em prod pra colidir |

**Nomenclatura ok, ainda não publicadas em prod** (não é desvio de padrão —
checado 2026-09-11 nos 25 modelos prata/ouro do escopo, cruzando com o
`information_schema` de prod): `bronze_shpt_obra_mensal_far`/`_fds`/`_rural` e
`prata_historico_entrega_apf` (`mcmv_historico_dbt`) — vieram de changes depois
do último `publicar-historico.sh` (2026-09-08); e `prata_fds_historico_dim_empreendimento`,
criada/realocada em 2026-09-11 (ver linha `empreendimento_far_dbt`/`empreendimento_fds_dbt`
acima). Publicar quando fizer sentido — nome já está certo.

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
- [ ] Arquivo `<camada>_<token>_<assunto>[_<recorte>].sql` — sem `alias`. Bronze:
      token de fonte (seção 4). Prata/ouro: token de domínio + regra
      `historico` da seção 4.1.
- [ ] Nome único dentro do schema da camada.
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
