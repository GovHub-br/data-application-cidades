# Issue #130 - Estrategia de Identidade de Empreendimento (APF x Fases) na Silver

## Resumo

Este documento formaliza a estrategia de modelagem para resolver a variacao do
codigo APF ao longo das fases do empreendimento na frente Entidades/FDS do MCMV.
O modelo atual trata o APF como identidade estavel, mas o APF muda entre as fases
Projeto, Obra e Desligamento. Cerca de 19% dos empreendimentos tem mais de um APF.

Decisao (revisada pelo @oracle): manter o APF como chave fisica da linha, criar uma
chave logica estavel `id_empreendimento` derivada do **APF-ancora (Fase Projeto)**,
e uma dimensao `dim_empreendimento` (relacao 1:N empreendimento -> APFs por fase).
SCD2 plena fica adiada; adota-se snapshot com chave logica estavel em v1.

## Contexto e Problema

- Um empreendimento habitacional tem um codigo APF.
- O APF **nao e estavel**: muda quando o empreendimento avanca de fase
  (Projeto -> Obra -> Desligamento). O mesmo empreendimento tem de 1 a 3 APFs.
- O modelo atual usa APF como identidade (snapshot 1 linha por APF). Consequencias:
  - Um empreendimento multi-fase aparece como 2 ou 3 "empreendimentos" distintos.
  - Agregacoes por empreendimento (ex.: UH contratadas/entregues) ficam incorretas.
  - Nao ha como reconstruir a trajetoria do empreendimento ao longo do tempo.

## Evidencia no Repositorio

1. `entidades_dbt/bronze/fds_cadastro_pj.sql:108-109` ja captura `ic_mudanca_fase`
   (flag `S`) e `apf_mudanca_fase`, mas nenhum modelo silver/gold os consome.
   Sao campos orfaos hoje.
2. `macros/udfs/f_normalize_apf.sql`: APF canonico de 8 digitos. Formatos de origem:
   GFAR `0626780-03` (com traco), cadastro `62678003` (8 dig.), financeiro
   `626780` (6 dig., sem verificadores). Os 6 digitos sao a "raiz" e os 2 finais
   sao verificadores.
3. `entidades_dbt/silver/fds_empreendimento.sql:34,193` e
   `entidades_dbt/silver/fds_evolucao_financeira.sql:23,82` usam
   `left/right(apf, 6)` para casar financeiro (6 dig.) com cadastro (8 dig.).
   Isso resolve o casamento de **formato dentro da mesma APF**, mas nao resolve
   **mudanca de fase**: APFs de fases distintas sao totalmente diferentes e nao
   compartilham a raiz de 6 digitos.
4. Arquivo `data-science/dados-historicos-tratamento/data/RELAÇÃO_APF_FASES_FDS_ARQUIVO_JANEIRO_E_ABRIL_2026.xlsx`
   (abas JAN26 e ABR26):
   - Distribuicao `QT_FASES` (ABR26): 720 com 1 fase, 131 com 2 fases, 37 com
     3 fases (total 888; ~19% multi-fase).
   - Exemplo multi-fase: `26920322` (Fase Projeto) <-> `41979800` (Fase Obra).
   - Taxonomia evoluiu: JAN26 usa `NU_APF_FDS_FASE_1/2/3` (generico); ABR26 usa
     `APF FDS Fase Projeto / Fase Obra / Fase Desligamento`.
5. `mcmv_historico_dbt/piloto/*`: ja estabelece o contrato de versionamento
   (`id_negocio_historico`, `hash_linha`, `dt_valid_from`, `dt_valid_to`,
   `is_current`), mas hoje cobre serie anual agregada, nao por empreendimento.

## Decisao de Modelagem (endossada pelo @oracle)

1. **Grao da silver `fds_empreendimento`**: 1 linha por **APF** (chave fisica
   mantida). Adicionar `id_empreendimento` (FK logica estavel) e
   `fase_empreendimento` (atributo).
2. **Nova `dim_empreendimento`** (schema `entidades_fds`, materialized table):
   grao 1 linha por `(id_empreendimento, apf)`.
3. **`id_empreendimento`** = hash do **APF-ancora (Fase Projeto)**, nunca do
   conjunto de APFs (ver secao "id_empreendimento").
4. **Fontes da dim**: seed versionado `seed_apf_fase_fds` (xlsx curado, passado)
   uniao bronze `fds_mudanca_fase_eventos` (deteccao por ingestao). Em conflito,
   o seed curado vence.
5. **Sem SCD2 plena em v1**: snapshot com chave logica estavel. As colunas do
   piloto #118 (`dt_valid_from`, `dt_valid_to`, `is_current`, `hash_linha`) sao
   herdadas de forma trivial (`dt_valid_to = null`, `is_current = true`).
6. **Regra de agregacao de UH por empreendimento** a confirmar com os dados
   (provavel `max` pela fase canonica, nao `sum`) - bloqueia a correcao do
   indicador de UH. Ver "Decisoes Pendentes".
7. **Downstream**: `mcmv_silver_dbt/silver/entidades/silver_mcmv_entidades_base.sql`
   passa a agrupar por `id_empreendimento`, com
   `quantidade_empreendimentos = count(distinct id_empreendimento)` e a regra de
   UH do item 6.

## Modelo-Alvo

### `dim_empreendimento` (nova)

| Coluna | Tipo | Descricao |
|---|---|---|
| `id_empreendimento` | text | Chave logica estavel (hash do APF-ancora). |
| `apf` | text | APF (chave fisica) em uma determinada fase. |
| `fase_empreendimento` | text | `Projeto`, `Obra` ou `Desligamento`. |
| `apf_ancora` | boolean | True quando este APF e a ancora (Fase Projeto/fallback). |
| `nome_empreendimento_canonico` | text | Nome canonico do empreendimento. |
| `arquivo_origem` | text | `JAN26` ou `ABR26` (rastreio do xlsx). |
| `dt_carga` | timestamp | Data de carga do mapeamento. |
| `dt_valid_from` | timestamp | Herdado do piloto #118 (v1: trivial). |
| `dt_valid_to` | timestamp | Herdado do piloto #118 (v1: null). |
| `is_current` | boolean | Herdado do piloto #118 (v1: true). |
| `hash_linha` | text | Herdado do piloto #118. |

Grao: 1 linha por `(id_empreendimento, apf)`. Um `id_empreendimento` tem 1..N APFs.

### `fds_empreendimento` (ajuste)

- Manter `apf` como chave fisica.
- Adicionar `id_empreendimento` e `fase_empreendimento` via join com
  `dim_empreendimento` por `apf`.
- Remover a dependencia da raiz de 6 digitos para identidade de empreendimento
  (manter apenas para o casamento financeiro<->cadastro dentro da mesma APF).

### `silver_mcmv_entidades_base` (ajuste)

- Agrupar por `id_empreendimento` em vez de `apf`.
- `quantidade_empreendimentos = count(distinct id_empreendimento)`.
- Regra de UH conforme item 6 (a confirmar).

## `id_empreendimento`

Formula:

```sql
md5('empreendimento-fds|' || coalesce(apf_fase_projeto, apf_min_conhecido))
```

- `apf_fase_projeto` = APF da fase Projeto, quando existir.
- `apf_min_conhecido` = APF mais antigo conhecido (fallback determinista, por
  `dt_contratacao` minima), quando nao ha APF de Projeto em nenhum extrato.

Por que hash do **ancora** e nao do conjunto de APFs:

- O conjunto de APFs cresce ao longo do ciclo de vida (Projeto -> Obra ->
  Desligamento). Hashear o conjunto quebra a estabilidade do `id_empreendimento`
  quando uma nova fase aparece em uma ingestao futura.
- A ancora (Fase Projeto) nao muda, entao o hash e estavel e reprodutivel.

Por que nao surrogate sequencial: quebra idempotencia em full-refresh/reprocessamento.

## Fontes de Verdade do Mapeamento

1. **INT059 (fonte mais direta, ja no raw)**: a tabela
   `__dados_brutos.int_empreendimentos_int_059_fds_caixa_pj` ja contem o vinculo
   de fase: `nu_apf` (APF atual/Obra) e `nu_apf_nao_obra` (APF da fase Projeto).
   No snapshot atual, 125 registros tem `nu_apf_nao_obra` preenchido. E o grafo
   APF(Projeto) -> APF(Obra) extraido diretamente da fonte, sem planilha manual.
2. **Seed `seed_apf_fase_fds`** (versionado, PR-review): derivado do xlsx
   `RELAÇÃO_APF_FASES_FDS_*.xlsx` (888 linhas, curado, muda raramente). Cobre o
   passado e os casos de 3 fases (Desligamento) que o INT059 nao captura. Segue o
   padrao do seed #118.
3. **Bronze `fds_mudanca_fase_eventos`**: derivado de `fds_cadastro_pj` onde
   `ic_mudanca_fase = true`. Colunas: `apf`, `apf_mudanca_fase`, `dt_movimento`,
   `arquivo_de_origem`. Deteccao por ingestao (presente/futuro).
   OBS: no snapshot atual `ic_mudanca_fase = false` para os 343 registros
   (mudancas ja resolvidas) - quem carrega o vinculo hoje e o INT059.
4. **Silver `dim_empreendimento`**: uniao INT059 + seed + eventos, dedup,
   resolve `id_empreendimento` pela regra do ancora, expoe a relacao 1:N.

Sincronizacao: o INT059 e a fonte operacional primaria do vinculo; o seed curado
vence em conflito; `ic_mudanca_fase` e flag de evento (sinaliza, nao mapeia).

## SCD2 (escopo v1)

Adiar SCD2 plena. Justificativa:

- O reloginho de UH contratadas/entregues e reconstruido do **grao de fato**
  (mensal em `fds_financeiro_mensal` / `fds_obra_mensal`), nao do historico da
  dimensao. A identidade estavel resolve o "quem e quem"; a serie temporal ja
  esta no grao.
- O caso de versionamento relevante (novo APF-fase associado) e um log pequeno
  e auditavel (`fds_mudanca_fase_eventos`), nao uma dimensao versionada completa.
- Adotar as colunas do piloto #118 preenchidas de forma trivial mantem a porta
  aberta para SCD2 futura sem pagar o custo agora (YAGNI).

## Regra de Agregacao de UH (RESOLVIDA: usar `max`)

Investigacao empirica (banco `cidades`, VPN habilitada): 125 pares multi-fase
identificados via `int_empreendimentos_int_059_fds_caixa_pj.nu_apf_nao_obra`.
Comparando `uh_contratadas` do APF Obra vs APF Projeto:

- 106 pares: so o APF Obra carrega UH (o APF Projeto nao aparece no snapshot atual).
- 19 pares: ambos os APFs tem UH e os valores sao IGUAIS.
- 0 pares: UH diferente entre fases.

Conclusao: **as UHs sao DUPLICADAS (nao particionadas) entre fases**. `sum`
dobraria a contagem nos 19 casos; `max` (equivalente a preferir a fase mais
avancada, Obra > Projeto) e o correto.

Regra final: `max(quantidade_uh)` por `id_empreendimento` (ou coalesce pela fase
canonica Obra > Projeto > Desligamento).

### Valores financeiros (BUG-4, validado)

Mesma investigacao para `valor_contratado`/`valor_desembolsado` (via INT059):

- `vr_investimento` (total) e SEMPRE maior que `vr_projeto` + `vr_obra` nos 125
  pares; `vr_obra` e 0 em 120/125 casos.
- `vr_projeto`/`vr_obra` sao componentes (projeto/obra), nao "totais de fase".

Conclusao: `valor_contratado` e `valor_desembolsado` sao totais unicos por
empreendimento, nao particionados entre fases. Regra `max` tambem e a correta
para o financeiro (nao `sum`).

## Testes dbt Recomendados

Genericos (schema.yml):

- `not_null` em `dim_empreendimento.id_empreendimento`, `apf`, `fase_empreendimento`.
- `unique` em `dim_empreendimento.id_empreendimento` (grao 1 linha por
  empreendimento) ou em `(id_empreendimento, apf)` (grao 1:N por linha).
- `accepted_values` em `fase_empreendimento` ∈ {Projeto, Obra, Desligamento}.
- `relationships` de `fds_empreendimento.apf` -> `dim_empreendimento.apf`
  (sem orfaos na silver).

Singulares (tests/entidades_dbt/):

- `assert_apf_fase_uniqueness` - um APF aparece em no maximo um
  `(id_empreendimento, fase_empreendimento)`.
- `assert_empreendimento_tem_ancora` - todo `id_empreendimento` tem exatamente um
  APF-ancora.
- `assert_sem_ciclo_mudanca_fase` - o grafo `apf -> apf_mudanca_fase` e aciclico.
- `assert_fase_taxonomia_consistente_jan_abr` - mesmo APF nao mapeado para fases
  conflitantes entre JAN26/ABR26.
- `assert_cobertura_apf_financeiro` - todo APF de `fds_financeiro_mensal` existe
  em `dim_empreendimento`.
- `assert_sem_duplicidade_uh_empreendimento` - para multi-fase, captura dupla
  contagem de UH conforme regra confirmada.

## Riscos e Armadilhas

1. **Dupla contagem de UH (critico)**: somar `quantidade_uh` de APFs de fases
   distintas do mesmo empreendimento infla UH se houver duplicacao. Resolver com
   a regra do item 6 antes de modelar.
2. **Taxonomia JAN26 vs ABR26**: `NU_APF_FASE_1/2/3` (generico) -> Projeto/Obra/
   Desligamento (canonico). Definir regra de mapeamento e testar que o mesmo APF
   nao recebe fase conflitante entre extratos.
3. **Nome de empreendimento divergente entre fases**: `Nome Empreendimento` pode
   variar (typo/abreviacao). A dim escolhe nome canonico e expoe variantes. Nunca
   fazer join por nome.
4. **APFs orfaos**: APFs de `fds_financeiro_mensal`/`fds_obra_mensal` ausentes do
   mapeamento. Fallback: tratar como single-fase com
   `id_empreendimento = md5('empreendimento-fds|' || apf)`.
5. **Ciclos/contradicoes em `apf_mudanca_fase`**: A->B e B->A, ou A->B num mes e
   A->C noutro. Regra: `dt_movimento` mais recente vence; seed curado vence sobre
   detector.
6. **Ancora ausente**: empreendimento sem Fase Projeto. Fallback documentado (APF
   mais antigo) + teste.
7. **Nao estender `left/right(apf,6)` para cross-fase**: a raiz de 6 digitos e
   casamento de formato, nao de identidade.

## Decisoes Pendentes

| # | Decisao | Default recomendado |
|---|---|---|
| 1 | Regra de UH multi-fase (`max` vs `sum`) | RESOLVIDO: `max` (UHs duplicadas, nao particionadas) |
| 2 | Fallback de ancora quando nao ha Fase Projeto | APF mais antigo por `dt_contratacao` minima |
| 3 | Auto-merge vs curador humano para `ic_mudanca_fase` | Comecar com curador (seed vence) |
| 4 | Schema da dim (`entidades_fds` vs `entidades_fds_ref`) | `entidades_fds_ref` (separa referencia curada de derivados) |

## Plano de Implementacao

1. Criar seed `seed_apf_fase_fds` a partir do xlsx, com mapeamento
   JAN26/ABR26 -> fases canonicas.
2. Criar bronze `fds_mudanca_fase_eventos` (de `fds_cadastro_pj` onde
   `ic_mudanca_fase = true`).
3. Criar silver `dim_empreendimento` (uniao seed + eventos, resolve
   `id_empreendimento` pela regra do ancora).
4. Ajustar `fds_empreendimento` para expor `id_empreendimento` +
   `fase_empreendimento` via join na dim.
5. Ajustar `silver_mcmv_entidades_base` para agrupar por `id_empreendimento`
   (regra de UH = `max`).
6. Criar testes dbt (genericos + singulares) listados acima.
