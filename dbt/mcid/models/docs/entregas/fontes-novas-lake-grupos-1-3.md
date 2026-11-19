# Fontes novas do lake: o que virou modelo dbt e o que ainda depende de outra coisa

Levantamento de 2026-11-17 sobre as bases que estavam no lake sem tabela no Postgres, em três grupos. Fora da lista ficam as fontes paradas na origem (INT040, 054, 057, 065 e INT055 desde 03/2025), o TP_CR014 (nunca chegou) e a pasta GEAVO (o Lab deixou de ingerir por sigilo): o caminho é cobrar a CAIXA ou decidir internamente, não montar dbt.

## Grupo 1: arquivos que já estão no lake

| Fonte | Modelo | Situação |
|---|---|---|
| INT058 (PNHR, Banco do Brasil) | `bronze_sftp_int058_pnhr_bb_familias` | bronze e prata criadas (`prata_rural_pnhr_familia`, em conjunto com a INT064) |
| INT064 (PNHR, CAIXA) | `bronze_sftp_int064_pnhr_caixa_familias` | bronze criada; a prata é a mesma `prata_rural_pnhr_familia` |
| INT042 (Entidades legado, CAIXA) | `bronze_sftp_int042_fds_caixa_familias` | bronze e prata (`prata_fds_familia_legado`) criadas |
| Regularidade SNHIS semanal | `bronze_shpt_snhis_regularidade_entes_serie` e `prata_fnhis_regularidade_serie` | pronto: ente × data de referência |
| GEHIS no FAR | `prata_far_gehis_andamento_obra` | pronto: APF × mês, só APFs do FAR |
| Bronzes DuckDB paradas em 29 e 30/09 (Faixa 3, Reforma Casa Brasil, MCMV Cidades, eixo histórico) e a bronze GEHIS parada em M20260821 | sem modelo novo | operacional: rodar de novo depois de `raw_para_staging.py` |

As pratas de famílias foram escritas depois de ler o layout dos parquets (`scripts/governance/inspecionar_layout_bronze.py`, que imprime só nomes de colunas). Como são dado pessoal, a prata não carrega nome, CPF, PIS, nascimento, endereço nem CEP; guarda o contrato, o município, os valores, a situação e três atributos agregados (sexo, estado civil e faixa etária). Os dois modelos estão marcados como `restricted` e `rag_publication: prohibited`.

## Grupo 2: fontes do SharePoint com cópia manual parada em 08/2026

Já existem as bronzes de MONIT_MOV_OBRA, MONIT_MOV_FINANC, Cad PJ, trabalho social e Painel_TG. O que falta é a ingestão agendada no lugar da cópia manual, que é DAG do Airflow e não dbt. Para o HIS consolidado (74 remessas contra 45 lidas) e para a série histórica do Cad PJ do Rural, a leitura é mudança na bronze e precisa de conferência no lake.

## Grupo 3: dados que entram por arquivo de pesquisa

Orçamento da União por ação (2007 a 2026), execução da SNH 2024, LOA 2025 e 2026 com metas físicas, dicionário de situação do GFAR e as autorizações do Rural (42 das 46 portarias da Seleção 2023, mais a 1.362/2026). Os arquivos de origem (`sparql1.csv`, `loa2025_vol4.txt`, `mnpo_far_2023.txt` e os PDFs das portarias) não estão no repositório. Sem eles, criar seed seria inventar conteúdo. Quando os arquivos chegarem à pasta do repositório, cada um vira seed em `dbt/mcid/seeds/` com o schema documentado.
