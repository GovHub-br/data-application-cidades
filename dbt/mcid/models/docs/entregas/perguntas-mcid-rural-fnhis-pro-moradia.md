# Perguntas do MCID — Rural, FNHIS Sub-50 e Pró-Moradia

Modelos criados para responder às perguntas levantadas na oficina com o MCID (fotos 31–39).
Base: explorações de 2026-10-06 (`dbt/mcid/target/explorar_perguntas_mcid*.py`, fora do git).
A matriz pergunta × dado está em `ouro_mcid_cobertura_perguntas` (seed `perguntas_mcid/cobertura_perguntas_mcid`).

## Como rodar e publicar (com VPN)

Na raiz do repo, Git Bash, VPN ligada:

```
set -a; source .env; set +a                      # DB_DW_*_MCID para o profile `prod`
python scripts/governance/conferir_colunas.py rural_dbt fnhis_dbt pro_moradia_dbt   # documentação × SQL

cd dbt/mcid
poetry run dbt seed  --profiles-dir . --select path:seeds/perguntas_mcid
poetry run dbt build --profiles-dir . --select \
  bronze_sftp_gehis_andamento_obra+ bronze_sftp_portaria_162_2018_rural+ \
  bronze_shpt_fnhis_sub50_termos_compromisso_serie+ prata_rural_obra_mensal_serie+ \
  prata_rural_operacao_eo+ ouro_fnhis_motivos_nao_enquadramento ouro_fnhis_proposta_tramite+ \
  ouro_pro_moradia_contrato_selecao ouro_mcid_cobertura_perguntas \
  ouro_pro_moradia_tomador_recontratacao+ ouro_pro_moradia_obra_andamento ouro_pro_moradia_obra_situacao_mensal
cd ../..
```

Publicar no OpenMetadata (com o `.venv-om` ativo), depois do `dbt build` ter dado certo:

```
python rodar_openmetadata_pro_moradia_fnhis.py --pastas rural_dbt pro_moradia_dbt fnhis_dbt
deactivate
cd scripts/governance && poetry run python sincronizar_governanca.py --confirmar
```

O script cria as tabelas no catálogo, anexa descrição, tags, tier e linhagem do dbt e confere tabela a
tabela; a governança (domínio, produto, glossário, certificação) roda fora do `.venv-om`.
`ouro_mcid_cobertura_perguntas` entra no produto `fnhis_sub50` por prefixo declarado em
`governance/dominios.yml`.

## Modelos

| Camada | Modelo | Grão | Pergunta |
|---|---|---|---|
| seed | `dominio_rural_obra` | campo × código | decodifica o layout de obra do Rural |
| seed | `cobertura_perguntas_mcid` | pergunta | matriz de cobertura |
| bronze | `bronze_sftp_gehis_andamento_obra` | linha do arquivo (todas as remessas) | trajetória da obra |
| bronze | `bronze_sftp_portaria_162_2018_rural` | selecionado | ciclo de seleção 2018 |
| bronze | `bronze_shpt_fnhis_sub50_termos_compromisso_serie` | termo × retrato | trâmite FNHIS |
| prata | `prata_rural_obra_mensal_serie` | APF × mês | estágio da obra |
| prata | `prata_rural_gehis_andamento_obra` | APF × mês | estágio da obra (CAIXA) |
| prata | `prata_rural_selecao_portaria_162` | selecionado | ciclo 2018 |
| prata | `prata_rural_operacao_eo` | APF | EO resolvida + status |
| prata | `prata_fnhis_termo_compromisso_mensal` | termo × retrato | trâmite FNHIS |
| ouro | `ouro_rural_entidade_organizadora` | EO | Rural 2, 3, 6 |
| ouro | `ouro_rural_entidade_recontratacao` | operação | Rural 6 |
| ouro | `ouro_rural_selecao_portaria_162` | selecionado | Rural 5 |
| ouro | `ouro_rural_obra_andamento` | APF | Rural 7 |
| ouro | `ouro_rural_obra_situacao_mensal` | fonte × mês × UF × situação | Rural 4 |
| ouro | `ouro_fnhis_motivos_nao_enquadramento` | proposta × motivo | FNHIS 1 |
| ouro | `ouro_fnhis_proposta_tramite` | proposta | FNHIS 2, 3 |
| ouro | `ouro_pro_moradia_contrato_selecao` | contrato | Pró-Moradia 1, 2 |
| ouro | `ouro_mcid_cobertura_perguntas` | pergunta | todas |
| ouro | `ouro_fnhis_ente_historico` | município proponente | FNHIS 4–6, 8 (réplica do Rural) |
| ouro | `ouro_fnhis_termo_situacao_mensal` | retrato × UF × situação | FNHIS 7 (réplica) |
| ouro | `ouro_pro_moradia_tomador` | tomador | Pró-Moradia 5, 6 (réplica) |
| ouro | `ouro_pro_moradia_tomador_recontratacao` | contrato | Pró-Moradia 8 (réplica) |
| ouro | `ouro_pro_moradia_obra_andamento` | contrato | Pró-Moradia 9 (réplica) |
| ouro | `ouro_pro_moradia_obra_situacao_mensal` | mês × UF × situação | Pró-Moradia 7 (réplica) |

## Perguntas do Rural replicadas no FNHIS e no Pró-Moradia

A "entidade organizadora" do Rural vira o **município proponente** no FNHIS e o **tomador** no
Pró-Moradia. Habilitação no FNHIS = regularidade no SNHIS (retrato atual); no Pró-Moradia não há
dado. A "pendência ao concorrer" usa a data do resultado da seleção (FNHIS, var
`fnhis_data_selecao`) ou a data de assinatura (Pró-Moradia). A coluna `origem_pergunta` da matriz
separa as perguntas da oficina das réplicas.

## Decisões e pontos a confirmar

- **EO do Novo Rural**: a SNH não informa EO em nenhuma das 1.263 operações do Novo MCMV e o Cad PJ
  só cobre 127. `prata_rural_operacao_eo` usa, na falta, o CNPJ "construtora/entidade" do arquivo
  AF CAIXA — **confirmar com a CAIXA se é a EO**; `fonte_eo` marca a origem.
- **Recontratação com pendência** usa a data de contratação como aproximação da data em que a EO
  concorreu (não existe data de inscrição no dado).
- **Ciclos de seleção do Rural**: só a Portaria 162/2018 tem lista nominal. Não há propostas
  apresentadas nem motivo de não seleção.
- **FNHIS na SNH**: as 1.224 operações estão com 0% e R$ 0 desembolsado — a execução não é
  medida ali; o empenho do TransfereGov é o melhor sinal hoje.
- **Pró-Moradia**: ligação contrato → ato é só o texto `cod_ident_externo` (240 contratos sem).
- A bronze da série TransfereGov resolve arquivos e colunas na compilação (via `duckdb.query`
  + `parquet_schema`) para não repetir o problema das colunas de anexo duplicadas.
- O desenquadramento GEHIS (`CAIXA_AF_GEHIS_OPERACAO_DESENQUADRADA_M`) é do FAR, não do Rural.

## Lacunas de fonte (pedir ao MCID/CAIXA/TransfereGov)

Habilitação de EO (pedido, análise, resultado, motivo); propostas apresentadas/enquadradas por
ciclo do Rural; trâmite interno do FNHIS (SEI + planilha); medição/desembolso FNHIS; cartas-consulta
do Pró-Moradia; beneficiários qualitativos do Pró-Moradia; série histórica do Cad PJ do Rural;
regularidade SNHIS semanal (só 5 de ~70 arquivos estão na staging; o resto está em raw/ como xls/zip).

## Aba Rural refatorada (2026-10-09)

A aba Rural do painel `mcid-perguntas` passou a ser construída por código:
`scripts/superset/build_aba_rural_mcid.py` (mesmo padrão do `build_boletim.py`). O script troca
**só** a aba Rural e os filtros `NATIVE_FILTER-RURAL-*`; as outras abas e os charts antigos ficam
como estão (nada é apagado).

### Entidades e chaves

| Entidade | Chave | Observação |
|---|---|---|
| Contrato / empreendimento | APF | No Rural a APF é o contrato do empreendimento com o agente; não há código de empreendimento separado. Contam igual. |
| Entidade organizadora (EO) | CNPJ com 14 dígitos | Normalizado pela macro `cnpj_normalizado`; "0" vira nulo. |
| Portaria de seleção | 162/2018 | Única lista nominal de selecionados (0 ou 1 por APF). |
| Portaria de Trabalho Social | 21/2014, 464/2018, 75/2025 | Informada pelo agente na planilha de TS (0 ou 1 por APF). |

### Modelos novos ou alterados

| Camada | Modelo | Grão | O que mudou |
|---|---|---|---|
| macro | `cnpj_normalizado`, `cnpj_digitos_origem` | — | CNPJ com 14 dígitos; só zeros vira nulo |
| prata | `prata_rural_operacao_eo` | APF | CNPJ normalizado; nova `eo_cnpj_origem`; `fonte_eo` ganha "CNPJ inválido na origem" |
| prata | `prata_rural_selecao_portaria_162` | selecionado | CNPJ normalizado (corrige `ic_mesma_eo`) |
| prata | `prata_rural_vinculo_operacao` (novo) | APF | vínculos + execução + marcas de qualidade |
| prata | `prata_rural_selecao_portaria_162`, `prata_rural_trabalho_social_bb/caixa` | — | viraram casca + corpo em `macros/corpos/corpos_rural.sql` (reuso na prata acima) |
| ouro | `ouro_rural_selecao_portaria_162` | selecionado | CNPJ do INT065 normalizado na comparação |
| ouro | `ouro_rural_painel_operacao` (novo) | APF | dataset principal da aba |
| ouro | `ouro_rural_painel_pendencia` (novo) | registro × regra | regras R01–R10 de qualidade |

### Regras de qualidade (`ouro_rural_painel_pendencia`)

| Regra | Classificação | Critério |
|---|---|---|
| R01 Operação sem EO | Associação ausente | nem cadastro de EO nem AF CAIXA trazem CNPJ |
| R02 CNPJ da EO inválido | Inconsistência comprovada | CNPJ recebido só com zeros |
| R03 EO só pelo AF CAIXA | Requer investigação | EO veio do campo "construtora/entidade" |
| R04 Mesmo CNPJ, nomes diferentes | Requer investigação | mais de uma grafia para o CNPJ |
| R05 Desembolso acima do contratado | Inconsistência comprovada | desembolsado > contratado + aporte |
| R06 Selecionado 162/2018 sem contrato | Associação ausente | código da portaria não casa com APF |
| R07 EO diferente da selecionada | Requer investigação | CNPJ do contrato ≠ CNPJ da seleção (14 dígitos) |
| R08 TS sem contrato consolidado | Requer investigação | APF na planilha de TS e fora da visão consolidada |
| R09 Contrato sem registro de TS | Informação ausente | APF fora das planilhas de TS |
| R10 Portaria de TS não informada | Informação ausente | APF na planilha, campo de portaria vazio |

### Como publicar (sem rodar testes)

```bash
export $(grep -E '^DB_DW_[A-Z_]*_MCID=' .env | tr -d '"' | xargs)
poetry run dbt run --project-dir dbt/mcid --profiles-dir dbt/mcid --target prod -s path:models/rural_dbt/prata path:models/rural_dbt/ouro
poetry run python scripts/superset/build_aba_rural_mcid.py --dry-run
poetry run python scripts/superset/build_aba_rural_mcid.py
```

## Portarias de seleção do Rural (2026-10-12)

> Atualizado em 2026-11-04: autorizações de contratação, filtro único de portaria, Curva S e correção da APF do
> financeiro estão em [`rural-cadeia-portarias-curva-s.md`](rural-cadeia-portarias-curva-s.md). As tabelas
> `seeds.portaria_354_25` e `seeds.portariamcid` foram trocadas por `seeds.rural_portaria_selecao_354_2024` e
> `seeds.rural_portaria_selecao_697_2026`.

Pergunta: quais propostas cada portaria de seleção escolheu e o que virou contrato. Só a 162/2018 tem
lista no lake (SFTP). As outras (1.161/2025, 38/2026, 697/2026, 948/2026) não estão no banco, no lake
nem no arquivo semanal de contratações — conferido com `scripts/inspecionar_parquet_semanal_rural.py`.

| Camada | Modelo | O que faz |
|---|---|---|
| Seed | `perguntas_mcid/portarias_selecao_rural` | catálogo: número, ano, data, papel no ciclo, se tem lista |
| Seed | `perguntas_mcid/selecao_portarias_rural` | anexos das portarias novas, uma linha por proposta (começa vazio) |
| Bronze | `bronze_seed_portarias_selecao_rural`, `bronze_seed_selecao_portarias_rural` | espelho dos seeds |
| Prata | `prata_rural_portarias_selecao` | catálogo tipado |
| Prata | `prata_rural_selecao_portarias` | 162/2018 (SFTP) + anexos do seed num formato só |
| Prata | `prata_rural_selecao_contrato` | vínculo proposta → contrato |
| Ouro | `ouro_rural_selecao_portarias` | uma linha por proposta selecionada, com o contrato e a confiança |
| Ouro | `ouro_rural_portarias_selecao_resumo` | uma linha por portaria, inclusive as sem anexo |

Vínculo, na ordem (vale o primeiro que achar contrato): APF (quando o anexo traz o código do
empreendimento, caso da 162/2018) → CNPJ da EO + município pelo código IBGE → CNPJ da EO + município
pelo nome e UF. Nos dois últimos só contam contratos assinados a partir da portaria. Com mais de um
candidato, fica o de UH mais perto das selecionadas. `confianca_vinculo`: Exata, Alta, Média, Baixa.
A `ouro_rural_selecao_portaria_162` não mudou.

Para carregar uma portaria nova: colar as linhas do anexo em `seeds/perguntas_mcid/selecao_portarias_rural.csv`
com o `portaria_codigo` do catálogo (ex. `697_2026`) e rodar:

```bash
poetry run dbt seed --project-dir dbt/mcid --profiles-dir dbt/mcid --target prod --select portarias_selecao_rural selecao_portarias_rural
poetry run dbt run  --project-dir dbt/mcid --profiles-dir dbt/mcid --target prod --select bronze_seed_portarias_selecao_rural bronze_seed_selecao_portarias_rural prata_rural_portarias_selecao prata_rural_selecao_portarias prata_rural_selecao_contrato ouro_rural_selecao_portarias ouro_rural_portarias_selecao_resumo
```

