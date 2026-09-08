# Entrega — série histórica por situação de obra e por região

Change: `serie-historica-situacao-obra-regiao`. Build/verificação local em
2026-09-06, `target=staging_duckdb` (`/mnt/data/duckdb/cidades.duckdb`), staging
MinIO real (`10.0.0.56:9000`, bucket `data-lake-mcid`).

## O que entrou

| artefato | tipo | onde |
|---|---|---|
| `seeds/data_quality/dominio_status.csv` | seed (35 linhas) | `data_quality.dominio_status` |
| `seeds/data_quality/dominio_regiao_uf.csv` | seed (27 linhas) | `data_quality.dominio_regiao_uf` |
| `situacao_canonica` + `regiao_sigla` + `regiao_nome` | colunas | `silver_mcmv_historico_empreendimento_{far,fds,rural}` |
| `situacao_derivada` + `regiao_sigla` + `regiao_nome` | colunas | `silver_mcmv_historico_serie_executiva` |
| `situacao_canonica` + `regiao_sigla` + `regiao_nome` | colunas | `silver_historico_snh_apf_mes` (reloginho) |
| `regiao_sigla` + `regiao_nome` + nível `regiao` | colunas / grouping set | `gold_serie_mensal` |
| `gold_serie_situacao_mensal` | modelo novo | `dados_historicos.gold_serie_situacao_mensal` |

Escopo **só local** — nenhum modelo entra na carga noturna do Cosmos nem é
promovido a `prod` nesta change.

## Domínio de situação de obra

Enumeração canônica: `nao_iniciada`, `em_obras`, `paralisada`, `concluida`,
`cancelada`. O levantamento do domínio real (34 grafias) e as decisões de
mapeamento estão em `dominio-status-operacional.md`. `classe=pendente` marca 5
valores ainda sem acordo com o negócio (`risco de paralisação`,
`concluida_com_vlr_a_liberar`, `a_distratar`, `vendido_rj`, valor composto) —
não bloqueia o build.

### Cobertura do mapa (linhas das silvers por frente)

| frente | linhas | mapeadas | `nao_mapeada` | `NULL` (sem status na fonte) | % mapeada |
|---|---:|---:|---:|---:|---:|
| Entidades | 53.442 | 53.442 | 0 | 0 | 100,00 % |
| FAR | 342.977 | 341.815 | 0 | 1.162 | 99,66 % |
| Rural | 736.830 | 677.555 | 0 | 59.275 | 91,96 % |

**`nao_mapeada` = 0 nas 3 frentes e no reloginho** — 100 % dos valores de
`status_operacional` *preenchidos* casam no seed. O que fica de fora é ausência
na fonte (`NULL`), não falha de mapeamento: Rural tem ~8,5 mil APF sem o campo,
FAR tem 1.140 vazios + 22 com o texto literal `null`. O teste
`dentro_do_dominio` (`warn`) sobre `situacao_canonica` acusa **lista vazia** — é
o loop de manutenção do seed quando um snapshot novo trouxer grafia inédita.

### Sanity de `CONCLUIDA_COM_VLR_A_LIBERAR` (FAR)

`CONCLUIDA_COM_VLR_A_LIBERAR` domina o FAR (181.250 linhas / 3.514 APF) e foi
mapeada como `concluida` (`classe=pendente` — é conclusão **física** com
pendência **financeira**). Total de APF FAR em `situacao_canonica='concluida'`:
4.168 na série; 4.144 no snapshot corrente.

⚠️ **Não foi possível cruzar `concluida` contra `dt_entrega`**: no FAR o
`dt_entrega` da silver está nulo em 100 % das linhas `concluida` do snapshot
(a interface SFTP não popula `dt_ultima_entrega` de forma utilizável). O
cruzamento com entregas conhecidas fica para a integração com o fluxo de
entregas do reloginho (`silver_historico_snh_entregas_mes`), fora do escopo
desta change. Se a classificação de `CONCLUIDA_COM_VLR_A_LIBERAR` como
`concluida` estiver errada, ela distorce ~53 % do gráfico de fase FAR — daí o
`classe=pendente` e esta nota.

## `gold_serie_situacao_mensal`

- **Grão:** `(mes, frente_mcmv, situacao_canonica, nivel_geografico, uf,
  regiao_sigla)`; `nivel_geografico ∈ {nacional, regiao, uf}` via `grouping
  sets`. 19.395 linhas, janela **2019-12 → 2026-06**.
- **Estoque:** `n_empreendimentos` (APF distintos na situação no mês), `uh`.
- **Fluxo:** `entradas` / `saidas` por `lag(situacao_canonica) over (frente_mcmv,
  apf order by mes)`. A **primeira observação** de um APF não conta como
  `entrada` (não há situação anterior). Uma mudança gera +1 `entrada` na
  situação nova e +1 `saida` na anterior no mesmo mês.
- **Colapso da janela sobreposta:** na janela 2024-06 → 2024-11 a silver mantém
  a linha SFTP (fim do mês) **e** a SNH (dia 1) com `dt_referencia` distintos.
  O gold colapsa ao grão mensal com precedência SNH (mesma regra D6/D8 da
  silver) — sem isso a `lag()` criava transição falsa SFTP→SNH e o estoque
  dobrava.
- **Coerência entre níveis:** `nacional == Σ(uf) == Σ(regiao)` para
  `n_empreendimentos`, `entradas` e `saidas` — conferido, 999/999 combos
  `(mes, frente, situacao)` batem. **Não somar `entradas`/`saidas` *entre*
  `nivel_geografico`** (cada transição de APF acontece numa única UF).

### `entradas`/`saidas` de `paralisada` (nível nacional) e a descontinuidade de 2024

`fonte_serie` (predominante do mês por frente) vira `snh` a partir de 2024-06 e
alterna nos meses sem snapshot SNH no dump. A virada de feed é **visível na
série e não é suavizada**:

- O **estoque** (`n_empreendimentos` em `paralisada`) do Rural cai de ~660
  (SFTP, até 2024-05) para ~470 → 430 → 390 em 2024-06/07/09 — a SNH reporta
  menos paralisações que o SFTP reportava para o mesmo universo.
- Isso aparece como **rajada de `saidas`** em 2024-06 (Rural 117) e 2024-09
  (Rural 157), sem `entradas` correspondentes — é reclassificação por troca de
  fonte, não retomada de obra.
- Nos meses ainda cobertos por SFTP dentro de 2024 (`fonte_serie='sftp'`:
  2024-02, 2024-08) há picos de `entradas` (Rural 96 e 122) que **não** têm
  contraparte SNH.

**Leitura recomendada para a #59:** filtrar por `fonte_serie` e tratar
2024-06 → 2024-11 como quebra de série; comparar tendências *dentro* de cada
regime de fonte, não atravessando a virada.

### Amostra por região (2025-12, Rural, `paralisada`)

```
nivel      regiao uf   n_empr  uh    entradas saidas
nacional   BR     BR      197  6683       0     17
regiao     N      BR       63  2797       0      8
regiao     NE     BR       86  2900       0      4
regiao     CO     BR       10   300       0      3
regiao     SE     BR       20   450       0      2
regiao     S      BR       18   236       0      0
uf         N      AM       48  2227       0      7
uf         NE     BA       31  1036       0      0
uf         NE     PI       29  1060       0      1
...
```
`Σ(regiao) n_empr = 63+86+10+20+18 = 197 == nacional`. ✔

## `situacao_derivada` (série executiva pré-2019)

Domínio **distinto** — `contratada`, `em_entrega`, `concluida`, `nao_mapeada` —
derivado **só de quantidade** (regra D2). Nome e propósito distintos de
`situacao_canonica`; **nunca** combinar as duas séries sem ressalva. Fica só na
silver nesta fase (não entra em `gold_serie_situacao_mensal` — Open Question 4).
Distribuição (linhas · chaves distintas): `concluida` 5.053.064 · 219.538 |
`nao_mapeada` 2.525.271 · 181.370 | `contratada` 2.337.664 · 167.709 |
`em_entrega` 247.427 · 12.505.

## Fronteira com `rastreabilidade-empreendimento-fases-apf`

**Ortogonais — não confundir:**

| | esta change (`situacao_canonica`) | `rastreabilidade-empreendimento-fases-apf` (`fase_atual` / `apf_fase_*`) |
|---|---|---|
| o que mede | estado **físico da obra** reportado no tempo (contrato / obras / paralisada / concluída) | **fase administrativa do APF** no FDS (projeto → obra → desligamento), cada fase com APF, data e valor próprios |
| frentes | FAR, Entidades, Rural | só Entidades/FDS (única que populou as colunas de fase) |
| fonte | `status_operacional` / `situacao_do_empreendimento` | 9 colunas `APF FDS Fase *` da SNH dados prioritários |
| grão | empreendimento × mês | linhagem de APFs de um mesmo empreendimento |

Um empreendimento FDS pode estar em `fase_atual = 'obra'` (administrativo) e
`situacao_canonica = 'paralisada'` (físico) ao mesmo tempo. As duas dimensões
convivem no contrato silver; nenhuma deriva da outra.

## `dominio_status.csv` — autoria e manutenção

O seed é **fonte de verdade** desta change, **não** derivação do `mapa_status.csv`
de `padronizacao-dominio-nulos-duplicados` (change desatualizada, não será
executada sem revisão — decisão do usuário, 2026-09-06). `seeds/data_quality/
README.md` foi atualizado: a entrada `dominio_status.csv — PENDENTE, cópia/
derivação do mapa_status.csv` deixou de existir. Se
`padronizacao-dominio-nulos-duplicados` ressuscitar, a reconciliação converge
**para** este seed (mesma estrutura de colunas: `valor_bruto`, canônico,
`classe`).

## Resultados de teste

| suíte | resultado |
|---|---|
| `dbt seed --select data_quality` | PASS=5 (dominio_status INSERT 35, dominio_regiao_uf INSERT 27) |
| `dbt build` silvers por frente + snapshot | PASS=57, ERROR=0 |
| `dbt build silver_mcmv_historico_serie_executiva` | PASS=11, ERROR=0 (60 s) |
| `./run-reloginho.sh reloginho` | PASS=65, WARN=3, ERROR=1 — o ERROR é `assert_reloginho_frente_cobertura_mensal` (falha **pré-existente conhecida**: buraco no dump SNH do MinIO); WARN pré-existentes de sufixo-float na bronze |
| golds do reloginho byte-idênticos | ✔ md5 do conteúdo ordenado igual antes/depois (`indicadores_reloginho` 37, `_frente` 95, `_entregas` 336) |
| `dbt build` golds históricos | PASS=22, ERROR=0 |
| `dbt test --select mcmv_historico_dbt` | PASS=151, WARN=14 (todos `sem_sufixo_float_texto` de **bronze**, pré-existentes), ERROR=0 |
| `dbt build --target prod ... --empty` (compile Postgres) | ver `openspec` task 6.4 |
