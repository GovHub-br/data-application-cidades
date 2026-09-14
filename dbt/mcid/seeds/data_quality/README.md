# seeds/data_quality/

Seeds de referência consumidos pelos testes genéricos de
`macros/data_quality/`. Changes de origem: `testes-data-quality-dbt` e
`vocabulario-e-qualidade-financeira-historica` (seeds financeiros — ver o fim
deste arquivo).

Todos materializam no catálogo `cidades`, schema `seeds` (bloco
`seeds.data_quality` do `dbt_project.yml`; change
`renomear-camadas-pt-historico-reloginho`, D8).

## `colunas_esperadas.csv` (Fase 3)

`familia, coluna, desde` — o conjunto de colunas aceito por família do eixo
histórico. Semeado do `drift_schema.csv` do change `modelo-carga-drift-schema`.
Consumido por `colunas_esperadas` (nível `warn`).

## `dominio_status.csv`

`valor_bruto, situacao_canonica, classe` — mapa canônico de situação de obra.
`situacao_canonica ∈ { nao_iniciada, em_obras, paralisada, concluida,
cancelada }`; `classe ∈ { valido, pendente }` (`pendente` = ainda sem acordo
com o negócio, não bloqueia).

**Fonte de verdade do domínio de situação de obra no dbt.** Criado e mantido
pela change `serie-historica-situacao-obra-regiao` — **não** é derivação do
`mapa_status.csv` de `padronizacao-dominio-nulos-duplicados` (change
desatualizada, não será executada sem revisão). Se aquela change ressuscitar, a
reconciliação converge **para** este seed. O levantamento do domínio real e as
decisões de mapeamento estão em
`models/mcmv_historico_dbt/docs/dominio-status-operacional.md`.

Consumido pelas silvers históricas por frente e pelo reloginho via `left join`
sobre `lower(trim(status_operacional)) = lower(trim(valor_bruto))`, e testado
com `dentro_do_dominio` (nível `warn`) sobre a coluna derivada:

```yaml
- name: situacao_canonica
  data_tests:
    - dentro_do_dominio:
        arguments: { seed: dominio_status, seed_column: situacao_canonica }
        config: { severity: warn }
```

## `dominio_regiao_uf.csv`

`uf, regiao_sigla, regiao_nome` — 27 UFs → 5 macrorregiões IBGE
(`N`/`NE`/`CO`/`SE`/`S`). Cópia estável de `mcmv_staging.api_ibge_uf`, para o
eixo histórico fazer o rollup `uf → regiao` sem depender de `source()` MinIO e
permanecer Postgres-válido (D3 da change `serie-historica-situacao-obra-regiao`).
Consumido pelas silvers históricas (por frente, série executiva, reloginho) e
pelos golds de série mensal via `left join` sobre `uf`; testável com
`relationships` sobre `regiao_sigla`.

---

## Seeds financeiros — change `vocabulario-e-qualidade-financeira-historica`

### `faixa_valor_uh.csv`

`frente, faixa, valor_col, piso, teto` — intervalo de plausibilidade do
**valor por unidade habitacional** (`valor_col / uh`), por combinação de
frente × faixa × coluna-de-valor. Consumido por `valor_dentro_faixa_uh`
(nível `warn`).

- `frente`: para a série executiva, é a `fonte_familia`
  (`bext`, `bases_relatorio_executivo`, `min_cidades`); para as silvers por
  frente, é `frente_mcmv` (`FAR`, `Entidades`, `Rural`). O teste mapeia a
  coluna do modelo → `frente` via `seed_key_columns`.
- `faixa`: normalizada case-insensitive; o seed traz as duas convenções
  observadas (`faixa 1` e `1`) para a mesma linha.
- `valor_col`: nome literal da coluna testada. Hoje: `valor_investimento`
  (série executiva); `valor_contratado` / `valor_desembolsado` (silvers por
  frente — bordas provisórias, ainda não wiradas). Valores acumulados a partir
  de zero (`valor_liberado`) não entram — a faixa R$/UH não se aplica nos
  primeiros snapshots.
- **Critério de calibração:** bordas ≈ p2 / p98 da distribuição observada em
  `dados_historicos.prata_dhist_serie_executiva` (2026-09-06),
  arredondadas para fora ~2–3× ("começa largo, aperta com dado" — Risks do
  design). As linhas de `FAR` / `Entidades` / `Rural` **não** foram calibradas
  contra dado local (silvers por frente não materializadas neste arquivo) —
  bordas provisórias amplas, revisar quando as silvers rodarem.
- Revisável com a área de negócio (Open Question 2 do design — hoje: por frente
  × faixa, sem recorte de ano).

```yaml
data_tests:
  - valor_dentro_faixa_uh:
      arguments:
        valor_column: valor_investimento
        uh_column: uh_contratadas
        seed: faixa_valor_uh
        seed_key_columns: { fonte_familia: frente, faixa: faixa }
      config: { severity: warn }
```

### `quarentena_valores_financeiros.csv`

`fonte_familia, chave_natural, motivo, evidencia` — registros comprovadamente
inválidos, **excluídos por anti-join** das silvers do eixo histórico
(`serie_executiva` hoje; por frente quando aplicável). Padrão de
`seed_correcao_fase_projeto`.

- **Critério de entrada** (varredura sobre a silver materializada, 2026-09-06):
  - `valor_negativo_repetido` — a chave tem valor financeiro `< 0`
    (investimento / empréstimo / liberado / subsídio) em ≥ 2 snapshots. Erro
    de sinal persistente na origem, não estorno pontual.
  - `valor_negativo_isolado` — idem, em 1 snapshot (raro; incluído para a
    silver ficar limpa).
  - `valor_por_uh_extremo` — `valor / uh > 100 ×` a mediana da família, em
    ≥ 2 snapshots (ex.: `bext/2694194` = R$ 2,4 bi / 128 UH).
- `evidencia`: resumo quantitativo (valor mínimo, nº de snapshots, R$/UH).
- **Regenerável:** a varredura é determinística sobre a silver; ao reconstruir
  a silver do zero, re-rodar a varredura e reconciliar (não editar à mão sem
  registrar o motivo).
- **Contrato com o teste `valor_nao_negativo`:** `warn` no bronze (cópia fiel),
  `error` na silver (pós-quarentena). Uma falha `error` na silver = registro
  **novo** a triar → entra aqui com `motivo`, ou a regra é ajustada.

```yaml
# na silver, após strip_float_text:
where not exists (
  select 1 from {{ ref('quarentena_valores_financeiros') }} q
  where q.fonte_familia = s.fonte_familia and q.chave_natural = s.chave_natural
)
```
