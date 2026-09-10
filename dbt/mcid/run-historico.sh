#!/usr/bin/env bash
#
# Build local do eixo histórico (mcmv_historico_dbt) no target staging_duckdb,
# um modelo por vez, com o DuckDB usando disco com espaço e limite de RAM —
# evita o "no space left on device" / OOM do union_by_name das bronzes.
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
# Contenção de memória (ver _run-common.sh): cada `dbt` roda dentro de um teto
# RÍGIDO de RAM (cgroup) e as 3 bronzes maiores da série executiva
# (bases_relatorio_executivo, min_cidades, bext) + a silver da série executiva
# rodam com --threads 1, para dois modelos pesados nunca coexistirem.
#
# Uso:
#   ./run-historico.sh                # tudo: seed + bronzes + silvers + golds + testes
#   ./run-historico.sh bronzes        # só as bronzes por família deste domínio
#   ./run-historico.sh silvers        # só as silvers por frente + consolidado
#   ./run-historico.sh serie          # só a cadeia pesada: 4 bronzes + silver da série executiva
#   ./run-historico.sh golds          # só os golds
#   ./run-historico.sh tests          # dbt test --select mcmv_historico_dbt
#   ./run-historico.sh <selector>     # dbt build --select <selector> --target staging_duckdb
#
# Overrides (env var): ver cabeçalho de _run-common.sh (DUCKDB_MCID_*, DBT).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_run-common.sh
source "$HERE/_run-common.sh"

run_common_banner
cd "$HERE"

# Carrega TODAS as seeds antes de qualquer build. Num arquivo frio (rebuild do
# zero) as seeds de referencia dos testes de DQ — `seeds.colunas_esperadas`,
# os dominios canonicos, `quarentena_valores_financeiros` — precisam existir ANTES
# do primeiro `dbt build --select <modelo>`, senao os testes de schema daquele
# modelo dao Catalog Error. `dbt seed` (sem --select) carrega todas; e barato.
seed_all() { run_dbt seed --target "$TARGET"; }

# Bronzes por familia (D5 da change pipeline-bronze-historica-destino-trocavel):
# 2 agentes SNH + 5 interfaces GEFUS + 4 familias da serie executiva + 2 agentes
# de entregas por evento (compartilhados com o reloginho, alimentam a espinha
# prata_historico_entrega_apf). Ordem crescente de volume: as maiores
# (min_cidades, bext) por ultimo. O mapa vive em macros/historico/familias.sql.
BRONZES=(
  bronze_dhist_empreendimento_snh_bb
  bronze_dhist_empreendimento_snh_caixa
  bronze_sftp_empreendimento_int040
  bronze_sftp_empreendimento_int054
  bronze_sftp_empreendimento_int057
  bronze_sftp_empreendimento_int059
  bronze_sftp_empreendimento_int065
  bronze_dhist_serie_entrada_bb
  bronze_dhist_serie_bases_relatorio_executivo
  bronze_dhist_serie_min_cidades
  bronze_dhist_serie_bext
  # entregas por evento (grao APF) — mesma fonte do reloginho; alimentam
  # prata_historico_entrega_apf (change enriquecer-datas-acompanhamento-historico).
  bronze_dhist_snh_entregas_evento_bb
  bronze_dhist_snh_entregas_evento_caixa
  # obra mensal (SharePoint) — curva prevista x realizada + situacao de obra
  # (change enriquecer-quantidades-uh-e-sinais-obra-historico). Janela 202512+.
  bronze_shpt_obra_mensal_far
  bronze_shpt_obra_mensal_fds
  bronze_shpt_obra_mensal_rural
)
# Bronzes que sozinhas ja sao grandes o bastante para valer --threads 1 (limita
# a paralelizacao interna do DuckDB, que e onde o pico de RAM mora). Sao as 3
# familias volumosas da serie executiva; `entrada_bb` (18k linhas) fica de fora.
HEAVY="bronze_dhist_serie_bases_relatorio_executivo bronze_dhist_serie_min_cidades bronze_dhist_serie_bext"

# Silvers e golds são baratos — construídos numa só invocação para o dbt
# ordenar as dependências e rodar os testes cross-frente (que leem far+fds+rural
# juntos) só depois de todos materializados. EXCETO
# prata_historico_serie_executiva (uniao das 4 familias + janela sobre
# ~10M linhas): sai em invocacao propria com --threads 1.
# silver_atual_dim_empreendimento (dominio empreendimento_fds_dbt) + suas 2 bronzes
# entram aqui porque prata_fds_historico_empreendimento passou a herdar
# id_empreendimento / fase_empreendimento dela (change id-empreendimento-eixo-historico).
# Leem o mesmo source('mcmv_staging', …) -> resolvem no staging_duckdb.
SILVERS=(
  bronze_fds_cadastro_pj
  bronze_fds_mudanca_fase_eventos
  silver_atual_dim_empreendimento
  prata_historico_entrega_apf
  prata_far_historico_empreendimento
  prata_fds_historico_empreendimento
  prata_rural_historico_empreendimento
  # piloto OGU/FGTS (prata_dhist_serie_anual_ogu_fgts) está enabled=false nesta
  # branch (change renomear-camadas-pt-historico-reloginho, D4) — fora do build.
  # O modelo de obra mensal autônomo foi dissolvido (consolidar-schemas-historico-reloginho,
  # D2): a família obra_mensal virou braço/left-join das 3 pratas de frente.
)
SILVER_SERIE=prata_historico_serie_executiva
GOLDS=(
  ouro_historico_snapshot_empreendimento_atual
  ouro_historico_marco_empreendimento
  ouro_historico_serie_mensal
  ouro_historico_serie_situacao_mensal
)

build_one() {
  local sel="$1"
  local extra=()
  case " $HEAVY " in *" $sel "*) extra=(--threads 1) ;; esac
  echo "=================================================================="
  echo "dbt build --select $sel ${extra[*]}"
  echo "=================================================================="
  run_dbt build --select "$sel" "${extra[@]}" --target "$TARGET"
}

# prata_historico_serie_executiva: união das 4 famílias (~10M linhas) +
# a dedup (reenvio_rank → conteudo_rank → SUM ao grão de consumo). O SUM/GROUP BY
# de milhões de grupos NÃO derrama em disco no DuckDB — o pico é ~10,3 GiB
# medido, e baixar o soft limit só torna tudo 2× mais lento sem mexer nesse
# operador. Com o teto global de 6G esse modelo levava SIGKILL. Roda então com
# folga própria: hard 11G (pico + margem) + 2G de swap de almofada (um estouro
# pequeno degrada pra erro capturável, não SIGKILL). Numa máquina com pouca RAM
# livre, suba DUCKDB_MCID_SERIE_CGROUP_MAX ou aceite que só este modelo falha
# (o resto do build e a máquina seguem intactos).
# Overridável: DUCKDB_MCID_SERIE_MEM / _SERIE_CGROUP_MAX / _SERIE_CGROUP_SWAP_MAX.
build_silver_serie() {
  echo "=================================================================="
  echo "dbt build --select $SILVER_SERIE --threads 1  (limites ampliados: hard 11G)"
  echo "=================================================================="
  DUCKDB_MCID_MEMORY_LIMIT="${DUCKDB_MCID_SERIE_MEM:-6GB}" \
  DUCKDB_MCID_CGROUP_MAX="${DUCKDB_MCID_SERIE_CGROUP_MAX:-11G}" \
  DUCKDB_MCID_CGROUP_SWAP_MAX="${DUCKDB_MCID_SERIE_CGROUP_SWAP_MAX:-2G}" \
    run_dbt build --select "$SILVER_SERIE" --threads 1 --target "$TARGET"
}

case "${1:-all}" in
  bronzes)
    seed_all
    for m in "${BRONZES[@]}"; do build_one "$m"; done
    ;;
  silvers)
    seed_all
    run_dbt build --select "${SILVERS[@]}" --target "$TARGET"
    build_silver_serie
    ;;
  serie)
    seed_all
    for m in \
      bronze_dhist_serie_entrada_bb \
      bronze_dhist_serie_bases_relatorio_executivo \
      bronze_dhist_serie_min_cidades \
      bronze_dhist_serie_bext; do
      build_one "$m"
    done
    build_silver_serie
    ;;
  golds)   run_dbt build --select "${GOLDS[@]}"   --target "$TARGET" ;;
  tests)   run_dbt test  --select "mcmv_historico_dbt" --target "$TARGET" ;;
  all)
    seed_all
    for m in "${BRONZES[@]}"; do build_one "$m"; done
    run_dbt build --select "${SILVERS[@]}" --target "$TARGET"
    build_silver_serie
    run_dbt build --select "${GOLDS[@]}" --target "$TARGET"
    if [ -z "${DUCKDB_MCID_SKIP_TESTS:-}" ]; then
      echo "=================================================================="
      echo "dbt test --select mcmv_historico_dbt"
      echo "=================================================================="
      run_dbt test --select "mcmv_historico_dbt" --target "$TARGET"
    else
      echo "(DUCKDB_MCID_SKIP_TESTS=1 — fase de testes adiada p/ o chamador)"
    fi
    ;;
  *) build_one "$1" ;;
esac

echo
echo "OK."
