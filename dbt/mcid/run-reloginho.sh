#!/usr/bin/env bash
#
# Build local do schema `reloginho` (indicadores_mcmv_dbt) no target
# staging_duckdb — série SNH mensal + entregas por evento → indicadores de
# velocidade (reloginho) e de gargalo de desempenho.
#
# MODO A (dev) dos três modos da change pipeline-bronze-historica-destino-trocavel:
#   A — dev         este script (--target staging_duckdb): lê a staging MinIO e
#                   materializa em cidades.duckdb. NÃO toca o Postgres.
#   B — publicação  ./publicar-historico.sh: copia as tabelas já materializadas
#                   no arquivo local para o Postgres via ATTACH. Não relê o MinIO.
#   C — direto      --target prod_duckdb: lê a staging e escreve no Postgres na
#                   mesma execução, com o motor DuckDB FORA do banco.
# O corpo de cada modelo é idêntico nos três; só o target muda.
#
# Materializa o schema `reloginho` inteiro (2 silver + 6 gold do mapa + as 2
# bronzes de ENTREGA por agente bronze_reloginho_snh_entregas_evento_bb/_caixa,
# que passaram de `dados_historicos` para `reloginho` na change
# consolidar-schemas-historico-reloginho) mais o upstream que ele exige: a
# cadeia medalhão FAR/FDS (`empreendimento_far`, `empreendimentos_fds`,
# tabelas `*_atual_*`) que os golds de gargalo leem. A série mensal SNH vem das
# bronzes por agente bronze_mcmv_historico_empreendimento_snh_bb/_caixa
# (mcmv_historico_dbt), reaproveitadas se já estiverem no arquivo.
#
# Contenção de memória: ver _run-common.sh — cada `dbt` roda dentro de um teto
# RÍGIDO de RAM (cgroup). Os modelos do reloginho são estreitos (~300k linhas),
# então não precisam da serialização por modelo do run-historico.sh.
#
# FALHA CONHECIDA: `assert_reloginho_frente_cobertura_mensal` acusa 5 combos
# (agente × frente) com buraco na série mensal. É lacuna da FONTE — os
# snapshots `historico_recente_*` da SNH faltam meses no MinIO (BB esparso:
# 2024-08/09/12, 2025-02/04/11; CAIXA só 2024-08). Não é erro do build; o
# script termina com exit != 0 por causa dela.
#
# Uso:
#   ./run-reloginho.sh                # tudo: build do schema + upstream + testes
#   ./run-reloginho.sh reloginho      # só os indicadores de velocidade (sem gargalo)
#   ./run-reloginho.sh gargalo        # só os 2 golds de gargalo + upstream
#   ./run-reloginho.sh tests          # dbt test --select indicadores_mcmv_dbt
#   ./run-reloginho.sh <selector>     # dbt build --select <selector> --target staging_duckdb
#
# Overrides (env var): ver cabeçalho de _run-common.sh (DUCKDB_MCID_*, DBT).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_run-common.sh
source "$HERE/_run-common.sh"

run_common_banner
cd "$HERE"

# Num arquivo frio, as seeds de referência dos testes de DQ precisam existir
# antes do build (senão Catalog Error nos testes de schema do upstream). Barato.
[ "${1:-all}" = tests ] || run_dbt seed --target "$TARGET"

# Indicadores de velocidade (reloginho puro) — sem a cadeia de gargalo.
RELOGINHO=(
  +gold_indicadores_reloginho
  +gold_indicadores_reloginho_frente
  +gold_indicadores_reloginho_entregas
  +gold_resumo_reloginho_dashboard
)
# Indicadores de gargalo — puxam o medalhão FAR/FDS (`*_atual_*`).
GARGALO=(
  +gold_indicadores_gargalo_desempenho
  +gold_resumo_gargalo_desempenho_dashboard
)

case "${1:-all}" in
  reloginho) run_dbt build --select "${RELOGINHO[@]}" --target "$TARGET" ;;
  gargalo)   run_dbt build --select "${GARGALO[@]}"   --target "$TARGET" ;;
  tests)     run_dbt test  --select indicadores_mcmv_dbt --target "$TARGET" ;;
  all)
    # Uma invocação: o dbt ordena bronze → silver → gold e roda os testes
    # (inclusive os cross-frente do gargalo) só depois de tudo materializado.
    run_dbt build --select "${RELOGINHO[@]}" "${GARGALO[@]}" --target "$TARGET"
    ;;
  *)
    echo "dbt build --select $1"
    run_dbt build --select "$1" --target "$TARGET"
    ;;
esac

echo
echo "OK."
