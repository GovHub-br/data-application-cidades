# Entrega — destravar-datas-obra-entrega-silver-historico (A + C)

Change OpenSpec `destravar-datas-obra-entrega-silver-historico`. Implementada e
testada local em 2026-09-06 (target `staging_duckdb`,
`/mnt/data/duckdb/cidades.duckdb`). Depende do change irmão
`enriquecer-datas-acompanhamento-historico` (B + D, commit `2528187`), já na
branch — o split `dt_entrega → dt_entrega_uh + dt_conclusao_obra`, o `left join`
na espinha e as janelas `*_grao` já estavam no lugar.

## O que mudou

### A — braço SFTP das 3 silvers para de descartar entrega / conclusão / UH concluídas

| frente / interface | antes | agora |
|---|---|---|
| **FDS** `fds_caixa` (INT059) | `quantidade_uh_entregues`, `dt_entrega_uh`, `dt_conclusao_obra` fixos em `NULL` | `qt_unidades_entregues`, `dt_ultima_entrega`, `coalesce(dt_termino_obra, dt_legalizacao)` |
| **FAR** `far_caixa`/`far_bb` (INT040/054) | `dt_conclusao_obra` já vinha do B+D | + `quantidade_uh_concluidas` (via `coalesce_present_parsed`, null-guard) |
| **Rural** `rural_caixa` (INT065) | `dt_entrega_uh = NULL` ("lean" do B+D) | `dt_ultima_entrega` (OQ2 resolvida: INT065 entra) |
| **Rural** `rural_bb` (INT057) | — | + `quantidade_uh_concluidas` (INT057 não tem `dt_ultima_entrega` — segue só espinha) |

Coluna nova em todas as frentes: **`quantidade_uh_concluidas`** (obra física
pronta — estágio anterior a "entregue"). Só o braço SFTP reporta; `NULL` no SNH.
Preservada ao longo do grão (`max(...) over grao`) para sobreviver à janela
2024-06..2024-11 em que a linha SNH vence a dedup.

`dt_entrega_uh_fonte` agora resolve `sftp:INT059` / `sftp:INT065` (antes o FDS e o
Rural rotulavam tudo como `sftp` genérico) — mesma expressão `regexp_extract` que
o FAR já usava.

### C — braço SNH projeta a previsão de entrega

`macros/historico/corpos_silver.sql` (serve as 3 frentes):

- `dt_previsao_entrega` ← `coalesce_present_parsed(['data_da_previsao_da_entrega', 'dt_previsao_entrega'])` (ISO);
- `qt_uh_previsao_entrega` ← `coalesce_present_parsed(['qt_uh_previsao_entrega', 'unidades_habitacionais_a_serem_entregues'])`.

`NULL` no braço SFTP. Resolve a **Open Question 1 do B+D**: o marco
`dt_previsao_entrega` de `ouro_dhist_marco_empreendimento` deixa de ser
`cast(null as date)` fixo e passa a resolver o maior valor observado, com
`dt_previsao_entrega_fonte = 'snh:dados_prioritarios'`.

### Lacuna de fonte — `data_de_termino` e datas de contratação por fase FDS (FORA)

A proposta original (C) também projetaria `dt_conclusao_obra` do braço SNH
(`data_de_termino`) e três colunas `dt_contratacao_fase_*`. **Removidas — sem
fonte no staging:**

| coluna esperada | onde estaria | fill FAR / FDS / Rural |
|---|---|---|
| `data_do_termino` | `mcmv_staging.dados_prioritarios_disponibilizados_snh_empreendimentos` (snapshot 30/09/2025, fonte da bronze paralela dos colegas) | **0 % / 0 % / 0 %** (só "Oferta Pública" e "FNHIS" têm) |
| `data_de_previsao_de_termino` | idem | 0,6 % / 2 % / 0,2 % |
| `data_de_contratacao_fds_fase_*` | — | **não existe em nenhuma fonte** |

`dt_conclusao_obra` do braço SNH fica `NULL`; a conclusão de obra vem só do SFTP.
As datas de fase ficam para a trilha `identidade-empreendimento`, quando/se a
extração SNH for ampliada.

## Cobertura medida (linha da silver)

| métrica | FAR | FDS | Rural |
|---|---|---|---|
| `dt_entrega_uh` | 81,2 % | **54,2 %** | 80,7 % |
| `dt_conclusao_obra` | 55,8 % (B+D) | **30,1 %** (era 0 %) | 66,1 % |
| `quantidade_uh_concluidas` | 73,0 % | 73,5 % | 72,5 % |
| `dt_previsao_entrega` | 122 linhas / 30 APF | 15 / 15 | 178 / 22 |
| `qt_uh_previsao_entrega` | 5,3 % | 6,8 % | 5,2 % |

FDS `dt_entrega_uh` em nível de APF: **445 / 1021 = 43,6 %** (era ~373 / 36 % só
com a espinha). `dt_entrega_uh_fonte` no FDS: `sftp:INT059` = 15.402 linhas,
`snh:entrega_evento` = 13.582.

`ouro_dhist_marco_empreendimento.dt_previsao_entrega`: 0 → **67 empreendimentos**
(fonte `snh:dados_prioritarios`). `marcos_coerentes = false`: 194 / 17.552
(1,1 %) — inclui as `dt_termino_obra` futuras que a OQ1 decidiu manter como
reportadas.

## Taxa de descarte de datas não parseáveis

`parse_hist_date` = `try_cast(nullif(nullif(trim(x), ''), 'None') as date)` — não
quebra o build; valor inválido vira `NULL`.

| interface / coluna | valores não-vazios | viraram `NULL` | observação |
|---|---|---|---|
| INT059 `dt_ultima_entrega` | 15.739 | 337 (2,1 %) | os 337 são a string literal `'NULL'` — nulo semântico, não perda de data |
| INT059 `dt_termino_obra` | 16.431 | 322 (2,0 %) | idem (`'NULL'`) |
| INT059 `dt_legalizacao` | 551 | 551 (100 %) | todos são `'NULL'` — INT059 não carrega legalização; `coalesce` cai em `dt_termino_obra` |
| INT065 `dt_ultima_entrega` | 52.303 | 0 | — |

Descarte de datas **reais** (formato inesperado tipo `dd/mm/aaaa`): **~0 %** nas
amostras — o parser já cobre ISO e os resíduos são o token `'NULL'`.

## Cruzamento entrega × mudança de status (FDS)

Transições de `situacao_canonica` → `concluida` no FDS: **1.246**. Com sinal de
`dt_entrega_uh` em ±3 meses da transição: **8,7 %**; com `dt_conclusao_obra` em
±3 meses: 5,3 %. Abaixo do baseline de 21 % do B+D — a maior parte das
transições `concluida` do FDS vem de linhas SNH pós-2024, e as datas do INT059
tendem a ser esparsas / mais antigas. É medição, não meta.

## Arquivos

- `macros/historico/corpos_silver.sql` — braço SNH: + `dt_previsao_entrega`, `qt_uh_previsao_entrega`
- `models/mcmv_historico_dbt/silver/prata_{fds,far,rural}_historico_empreendimento.sql`
- `models/mcmv_historico_dbt/gold/ouro_dhist_snapshot_empreendimento_atual.sql` — + `quantidade_uh_concluidas`, `dt_previsao_entrega`, `qt_uh_previsao_entrega`
- `models/mcmv_historico_dbt/gold/ouro_dhist_marco_empreendimento.sql` — marco `dt_previsao_entrega` da silver
- `models/mcmv_historico_dbt/{silver,gold}/schema.yml`

## Testes

`dbt build --select prata_dhist_entrega_apf+` e as 3 silvers isoladas:
**0 ERROR**. `dbt test --select mcmv_historico_dbt`: **175 PASS / 14 WARN / 0
ERROR** — os 14 WARN são `sem_sufixo_float_texto` em bronzes, pré-existentes e
alheios a esta change. `accepted_values` de `dt_entrega_uh_fonte` atualizado com
`sftp:INT059`; de `dt_previsao_entrega_fonte` com `snh:dados_prioritarios`.
