# seeds/entidades_fds/

Seeds de referência curados da frente Entidades/FDS. Materializam no catálogo
`cidades`, schema `entidades_fds` (bloco `seeds.entidades_fds` do
`dbt_project.yml`).

## `seed_apf_fase_fds.csv`

`apf, fase_empreendimento, apf_ancora, nome_empreendimento, arquivo_origem` —
mapeamento curado APF → fase (Projeto / Obra / Desligamento) e APF-âncora do
empreendimento, do xlsx GEFUS `RELAÇÃO_APF_FASES_FDS`. 1.093 linhas · 888
`id_empreendimento`. `arquivo_origem ∈ {JAN26, ABR26}` (hoje 100% `ABR26`).
Fonte de verdade da identidade de empreendimento FDS (change #130). Consumido
por `silver_atual_dim_empreendimento`.

## `seed_correcao_fase_projeto.csv`

`apf, situacao_anterior, fase_corrigida, dt_movimento, dt_correcao,
arquivo_origem` — correção retroativa de fase do xlsx GEFUS
`CORREÇÃO_FASE_PROJETO` (176 APFs Entidades reclassificados de "EM ANDAMENTO"
para "FASE PROJETO" em 2026-04-30). Change de origem:
`id-empreendimento-eixo-historico` (D4).

**Evidência, não sobrescrita:** `silver_atual_dim_empreendimento` faz `left
join` sobre `apf` e expõe `fase_corrigida` / `dt_correcao` / `origem_correcao`
ao lado da `fase_empreendimento` resolvida pelo seed — **não** a substitui.
Hoje há 1 APF em que os dois discordam (`fase_empreendimento = Obra`,
`fase_corrigida = Projeto`), visível para auditoria.

Seed curado provisório até a fonte `CORREÇÃO_FASE_PROJETO` entrar na staging
MinIO; quando entrar, vira `source` / bronze de cópia fiel.
