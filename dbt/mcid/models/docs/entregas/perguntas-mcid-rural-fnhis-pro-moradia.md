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
