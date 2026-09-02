# empreendimento_rural_dbt

Modelo dbt do **Novo MCMV Rural** e do **PNHR histórico** (programa encerrado, série
parada em 2024). Alimenta o dashboard do MCMV Rural no Superset.

## Arquitetura

```
Etapa 01 (RAW)     CSV / TXT / XLSX  -> raw/<fonte>/<dado>.<ext>          (MinIO)
Etapa 02 (STAGING) tudo em parquet   -> staging/<fonte>/<dado>.parquet    (MinIO, TEXT)
Etapa 03 (BRONZE)  pg_duckdb         -> Postgres empreendimento_rural.bronze_*
Etapa 04 (SILVER)  tipagem + limpeza -> Postgres empreendimento_rural.silver_*
Etapa 05 (GOLD)    regra de negócio  -> Postgres empreendimento_rural.gold_*  (Superset)
```

**A bronze não é dbt.** É o resultado de `scripts/staging_para_bronze.py`, que
materializa cada parquet de `staging/` como tabela no Postgres usando o
`read_parquet` do **pg_duckdb** — o dado não passa por Python. O que carregar está
declarado na família `empreendimento_rural` do `scripts/bronze_familias.yml`, e o dbt
enxerga essas tabelas pelo source `bronze_rural` do `models/sources.yml`.

Carregar/atualizar a bronze:

```bash
set -a; source .env; set +a
python scripts/staging_para_bronze.py --familia empreendimento_rural --listar   # dry-run
python scripts/staging_para_bronze.py --familia empreendimento_rural --apply
```

É full refresh por família e idempotente: `lake._bronze_log` tem UNIQUE
(familia, staging_key, source_hash), então recarregar o mesmo arquivo não duplica
nada. Não existe DAG para isso hoje — **a bronze é carregada à mão**, aqui e no FAR.

## Padrão de nomenclatura

**Um schema por domínio, camada no prefixo do nome da tabela.**

| Camada | Nome | Onde vive |
|---|---|---|
| Bronze | `bronze_<assunto>` | `empreendimento_rural` (via `staging_para_bronze.py`) |
| Silver | `silver_<assunto>` | `empreendimento_rural` (dbt) |
| Gold | `gold_<assunto>` | `empreendimento_rural` (dbt) |

Regras:

1. **Sem o token do domínio no nome.** O schema já diz que é rural; `gold_ficha_empreendimento`,
   não `gold_ficha_empreendimento_rural`. Nomes de model são globalmente únicos no
   projeto dbt, então o prefixo de camada é o que garante a unicidade.
2. **`<assunto>` casa entre as camadas.** `bronze_cadastro_pj` -> `silver_cadastro_pj`.
   A linhagem se lê pelo nome, sem abrir o SQL.
3. **Sem sufixo de tipo de visualização.** Era `execucao_fisica_financeira_chart_rural`;
   hoje é `gold_execucao_fisica_financeira`. O nome descreve o dado, não o gráfico.
4. **Uma pasta por camada** (`silver/`, `gold/`), e o prefixo repete a pasta de
   propósito: no banco não existe pasta.

Batendo o olho no banco, `\dt empreendimento_rural.*` sai ordenado por camada.

## Inventário

### Bronze — 12 tabelas (`bronze_familias.yml`)

| Tabela | Origem na staging |
|---|---|
| `bronze_cadastro_pj` | `MONIT_CAD_PJ_RURAL_MENSAL_<aaaamm>` |
| `bronze_cadastro_pf` | `MONIT_CADASTRO_PF_RURAL_MENSAL_<aaaamm>` |
| `bronze_obra_mensal` | `MONIT_MOV_OBRA_RURAL_MENSAL_<aaaamm>` |
| `bronze_financeiro_mensal` | `MONIT_MOV_FINANC_RURAL_MENSAL_<aaaamm>` |
| `bronze_prioritarios_snh` | export canônico `staging/sharepoint/` (snapshot) |
| `bronze_prioritarios_caixa` | `<aaaamm>_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA` |
| `bronze_prioritarios_bb` | `<aaaamm>_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB` |
| `bronze_pnhr_caixa` | `INT065_..._PNHR_CAIXA_EMPREENDIMENTOS_<aaaammdd>` |
| `bronze_pnhr_bb` | `INT057_..._PNHR_BB_EMPREENDIMENTOS_<aaaammdd>` |
| `bronze_pnhr_liberacoes` | `INT055_..._LIBERACOES_CAIXA_BB_<aaaammdd>` |
| `bronze_trabalho_social_caixa` | export canônico `staging/sharepoint/` (snapshot) |
| `bronze_trabalho_social_bb` | export canônico `staging/sharepoint/` (snapshot) |

`bronze_prioritarios_caixa` sai do **mesmo arquivo** que a tabela homônima da família
`empreendimento_far`, e é de propósito: cada domínio carrega a sua cópia no seu schema,
então não existe ordem obrigatória entre as famílias e nenhuma dropa a tabela da outra.

Os três marcados como *snapshot* são sobrescritos in-place na origem e **não acompanham
o feed mensal** — a atualidade depende de alguém regerar o export. Os INT065/INT057
pararam em 2024-11-29: o PNHR é programa encerrado.

### Silver — 13 models

Tipagem (`text` -> `int`/`date`/`numeric`/`boolean`), normalização de texto e uma
linha por grão declarado. Nenhuma regra de negócio.

| Model | O que é |
|---|---|
| `silver_prioritarios_snh` | espinha dorsal: um APF por linha, filtrada em `modalidade='RURAL'` |
| `silver_prioritarios_caixa` / `_bb` | o mesmo recorte pela ótica de cada agente |
| `silver_cadastro_pj` | cadastro do empreendimento contratado no Novo MCMV Rural |
| `silver_cadastro_pf` | beneficiários PF com perfil socioeconômico |
| `silver_obra_mensal` | evolução física, snapshot mensal |
| `silver_financeiro_mensal` | liberações por componente (obra, TS, ATEC, cisternas...) |
| `silver_pnhr_caixa` / `_bb` | histórico PNHR das integrações INT065/INT057 |
| `silver_pnhr_liberacoes` | liberações do INT055 filtradas em `no_programa like '%PNHR%'` |
| `silver_trabalho_social_caixa` / `_bb` | acompanhamento do PTS (layouts diferentes entre agentes) |
| `silver_empreendimento` | visão unificada: `silver_prioritarios_snh` + left joins de tudo |

### Gold — 9 tabelas (as que o Superset consome)

| Tabela | Grão | Alimenta |
|---|---|---|
| `gold_ficha_empreendimento` | APF | Ficha do Empreendimento |
| `gold_resumo_gerencial` | agregado | KPIs, pizza e barra da visão gerencial |
| `gold_panorama_estadual` | UF | Panorama por estado |
| `gold_mapa_nacional` | UF + Região | mapa e gráficos regionais |
| `gold_evolucao_financeira` | APF x mês | série temporal de desembolso |
| `gold_execucao_fisica_financeira` | APF | físico vs. financeiro (a discrepância é o sinal) |
| `gold_ficha_trabalho_social` | APF x agente | acompanhamento do PTS |
| `gold_perfil_beneficiarios` | APF | perfil socioeconômico agregado |
| `gold_infraestrutura_agua_saneamento` | APF | cisternas e efluentes |

## Encoding

Parte dos arquivos de origem chega com **mojibake já gravado dentro deles** (os exports
canônicos do SharePoint saem do dump antigo, ingerido antes da correção no
`raw_para_staging.py`). Isso é sujeira da origem, não do nosso decode, então é tratado
na silver pela UDF `corrigir_mojibake()` (criada no schema do target pelo
`macros/udfs/`), aplicada em 124 colunas de texto. A função é **no-op sobre texto
limpo**: quando a origem for corrigida, ela para de agir sozinha, sem precisar mexer
nos models.

O teste genérico `sem_mojibake` vigia o resultado, e o `scripts/diagnostico_qualidade.py`
perfila as três camadas de uma vez.

## Testes

92 no total: **33 `error`** (chave natural, grão composto, integridade referencial) e
**59 `warn`** (mojibake, nulo indevido, domínio fechado). O `warn` é degrau, não
desistência: cada um vira `error` conforme a origem fica limpa.

Dois genéricos próprios, porque o projeto não usa `dbt_utils`:

- `sem_mojibake` — uma linha por valor corrompido distinto na coluna.
- `unique_combinacao` — unicidade de grão composto (`[apf, mes]`, por exemplo).

## Como rodar

```bash
set -a; source .env; set +a

# bronze (fora do dbt)
python scripts/staging_para_bronze.py --familia empreendimento_rural --apply

# silver + gold + testes
dbt build --project-dir dbt/mcid --profiles-dir dbt/mcid --select empreendimento_rural_dbt

# perfil de qualidade das três camadas
python scripts/diagnostico_qualidade.py --dominio rural > diagnostico_rural.md
```

## Limitações conhecidas

- **Cobertura não declarada.** O monitoramento do Novo MCMV Rural cobre ~1,2% da
  carteira (cadastro PJ), ~1,0% (cadastro PF) e ~11,7% (obra mensal); o resto é PNHR
  histórico. Por isso `gold_perfil_beneficiarios` entrega ~103 de 10.402
  empreendimentos e `gold_infraestrutura_agua_saneamento` soma zero em ~98,8% deles —
  e nenhuma métrica diz isso ao leitor do dashboard.
- **`status_prazo` é ruído.** `dt_previsao_entrega` é 99,8% nula, e o `CASE` de
  situação compara só a grafia em maiúsculas (`CONCLUÍDO E ENTREGUE`, 20 linhas)
  ignorando a outra (`Concluído e Entregue`, 8.549).
- **23 colunas 100% nulas** na silver (16 em `silver_obra_mensal`, 7 em
  `silver_cadastro_pj`), mantidas porque a origem pode voltar a preenchê-las.
- **Sem DAG.** A bronze é carregada à mão.
- **O domínio ainda não está no `docs-pages`.** `docs-pages/src/dominios.yml` tem
  `conjuntura`, `empreendimento_far` e `entidades`; falta `empreendimento_rural`. O
  bloco `no_relatorio` é obrigatório e tem que sair do relatório técnico, então quem
  tiver a seção correspondente do relatório declara o domínio e roda `make docs-collect`.
