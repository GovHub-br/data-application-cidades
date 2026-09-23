# Pró-Moradia e FNHIS Sub-50 — bronze, prata e ouro

Branch `feat/dbt_pro_moradia_fnhis`. Dois produtos novos, nos schemas compartilhados
`bronze`/`prata`/`ouro`, seguindo o padrão de FAR, FDS e Rural: pastas `pro_moradia_dbt/` e
`fnhis_dbt/`, governança no `dbt_project.yml` e em `governance/dominios.yml`, termos no
glossário do OpenMetadata.

Os números abaixo são da exploração de 2026-09-23 no banco `cidades` e na staging do lake
(`dbt/mcid/target/explorar_*.py`, descartáveis e fora do git).

## Onde os dados estavam

A issue #119 registrou o Pró-Moradia como lacuna: procurou `pro_moradia`, `promoradia` e
`moradia` pelo nome das tabelas e não achou nada. O motivo é que o programa não aparece no nome
de tabela nenhuma — nem em coluna `modalidade`.

| Programa | Onde está | Como se recorta |
|---|---|---|
| Pró-Moradia | Canal FGTS (`fgts_canal_tab_ao_1_contratos_fgts` e vizinhas), que traz **todos** os programas do FGTS | `cod_linha = '26'` — `HAB / PRO-MORADIA` na tabela de domínio `fgts_canal_tdom_ao_1_linha` |
| FNHIS Sub-50 — contrato | `dados_prioritarios_disponibilizados_snh_empreendimentos` (a mesma bronze do Rural) | `modalidade = 'FNHIS'` |
| FNHIS Sub-50 — seleção | `todas_propostas_FNHIS_v0_*` | arquivo próprio |
| FNHIS Sub-50 — termo de compromisso | `FNHIS SUB 50 Painel_TG_*` (TransfereGov, retrato mensal) | arquivo próprio |
| Regularidade no SNHIS | `FNHIS_SEMANAL_MCID_*` | arquivo próprio |

No Pró-Moradia, a **modalidade** do canal não identifica o programa: diz o tipo de intervenção
(urbanização, produção de conjunto, cesta de materiais...). A prata agrupa as modalidades nas
frentes da carta-consulta — Urbanização, Provisão habitacional, Desenvolvimento institucional —
em `tipo_intervencao`.

## Modelos

### Pró-Moradia (`pro_moradia`)

| Camada | Model | Grão |
|---|---|---|
| bronze | `bronze_shpt_fgts_canal_*` (13) e `bronze_shpt_fgts_orcamento_final` | espelho do parquet |
| prata | `prata_pro_moradia_contrato` | contrato (686) |
| prata | `prata_pro_moradia_desembolso_mensal` | contrato × mês |
| prata | `prata_pro_moradia_execucao_obra` | contrato × mês de avaliação |
| prata | `prata_pro_moradia_paralisacao` | contrato paralisado (13) |
| prata | `prata_pro_moradia_orcamento` | ano × região |
| ouro | `ouro_pro_moradia_ficha_contrato` | contrato |
| ouro | `ouro_pro_moradia_evolucao_financeira` | contrato × mês |
| ouro | `ouro_pro_moradia_panorama_estadual` | UF |
| ouro | `ouro_pro_moradia_orcamento_execucao` | ano × região |

As bronzes do Canal FGTS moram em `pro_moradia_dbt/` porque só ele as consome hoje. Um próximo
produto do FGTS deve referenciá-las, não duplicá-las.

### FNHIS Sub-50 (`fnhis_sub50`)

| Camada | Model | Grão |
|---|---|---|
| bronze | `bronze_shpt_fnhis_sub50_termos_compromisso`, `bronze_shpt_fnhis_sub50_propostas`, `bronze_shpt_snhis_regularidade_entes` | espelho do parquet |
| prata | `prata_fnhis_propostas` | proposta (7.121) |
| prata | `prata_fnhis_termo_compromisso` | termo (1.207) |
| prata | `prata_fnhis_prioritarios_snh` | operação SNH (1.224) |
| prata | `prata_fnhis_regularidade_entes` | ente (5.596) |
| ouro | `ouro_fnhis_ficha_termo_compromisso` | termo |
| ouro | `ouro_fnhis_funil_propostas` | UF |
| ouro | `ouro_fnhis_regularidade_municipios` | ente |

## Chaves e armadilhas conferidas

- **Datas do Canal FGTS** vêm como `MM/DD/AA HH:MI:SS`. O `parse_date_br` devolve NULL nesse
  formato; as pratas convertem com `to_date(..., 'MM/DD/YY')`.
- **Contrato do FGTS**: `cod_contrato` já é único no canal sem o dígito verificador; a execução
  de obra nem traz o dígito.
- **Desembolso**: a série começa em 2000 e tem 17 pares contrato × mês repetidos (somados). Para
  contrato anterior a 2000 o acumulado é um piso — `ic_serie_financeira_incompleta` na ficha.
  316 dos 686 contratos têm liberação no arquivo.
- **Execução de obra**: traz meses futuros (cronograma, até 2027-12), marcados em
  `ic_avaliacao_futura`; a última medição da ficha os ignora.
- **Município do FGTS** é código CAIXA de 4 dígitos, traduzido para IBGE pela tabela de domínio
  (678 de 686).
- **IBGE no SNH** tem 6 dígitos (sem verificador); nas propostas e no SNHIS tem 7. Cruzar exige
  `left(ibge, 6)`.
- **Proposta ↔ termo**: a primeira parte de `no_reservado_pac` é o `numero_proposta` da seleção
  — casa 1.207 de 1.207.
- **Termo ↔ contrato SNH** não têm chave comum. A ficha vincula por município + valor (1.184 de
  1.224 casam), desempatando pela data de assinatura, e só aceita vínculo 1:1.

## Governança

- `dbt_project.yml`: blocos `pro_moradia_dbt` e `fnhis_dbt`, com `meta.governance` e
  `meta.openmetadata` por camada, iguais aos do Rural.
- `governance/dominios.yml`: produtos `pro_moradia` e `fnhis_sub50` (prefixos `prata_`/`ouro_`),
  etiquetas `dbtTags.pro_moradia` e `dbtTags.fnhis_sub50`.
- Glossário (`helpers/openmetadata/glossaries/mcid.csv`): `MCID.ProgramasHabitacionais.ProMoradia`,
  `MCID.ProgramasHabitacionais.MCMV.MCMVFNHISSub50` e `MCID.FundosEFontes.FNHIS`. O Pró-Moradia
  fica fora de `MCMV` de propósito: é programa do FGTS para o setor público.

## Fora do escopo desta entrega

- `Oferta Publica` (4.412 linhas na mesma bronze da SNH) é outra linha do MCMV, ainda sem produto.
- Os arquivos arquivados do Canal FGTS (`Canal FGTS/Arquivados/MC2025*/MCidades_AO_*`) permitiriam
  série histórica das remessas; a bronze lê só a remessa corrente.
- O site `docs-pages` não ganhou página de domínio: ela exige o texto do relatório técnico
  (`no_relatorio`), que não cobre estes dois programas.
