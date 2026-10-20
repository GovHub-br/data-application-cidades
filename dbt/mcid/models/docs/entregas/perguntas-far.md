# Perguntas do MCID — MCMV FAR

Modelos e painel criados para responder às perguntas da oficina com o MCID sobre o FAR
(cinco perguntas-mestras, cartões e documento "Perguntas que consigo responder").
Painel no Superset: `/superset/dashboard/perguntas-far/`. A matriz pergunta × dado está
em `ouro_far_cobertura_perguntas` (seed `perguntas_far/cobertura_perguntas_far`).

O painel FAR existente (`ouro_far_ficha_empreendimento` e afins) não foi alterado; o novo
painel só lê `ouro_far_serie_mensal_apf`.

## Como rodar e publicar (com VPN)

Na raiz do repo:

```
set -a; source .env; set +a

cd dbt/mcid
dbt seed  --profiles-dir . --select path:seeds/perguntas_far
dbt build --profiles-dir . --select \
  bronze_sftp_mont_cont_pf_far+ bronze_sftp_int039_far_caixa_pf+ bronze_sftp_int021_far_bb_pf+ \
  prata_far_empreendimento_natureza+ ouro_far_cobertura_perguntas
cd ../..

python scripts/superset/perguntas_far.py --dry-run   # confere o que seria criado
python scripts/superset/perguntas_far.py
```

## Modelos

| Camada | Modelo | Grão | Fonte |
|---|---|---|---|
| seed | `dominio_far_perguntas` | campo × código | layouts MONT_CONT_PF_FAR, MONIT_CAD_PJ_FAR, MONIT_MOV_OBRA_FAR |
| seed | `cobertura_perguntas_far` | pergunta | curadoria |
| bronze | `bronze_sftp_mont_cont_pf_far` | contrato (remessa mais recente) | MONT_CONT_PF_FAR — Novo MCMV |
| bronze | `bronze_sftp_int039_far_caixa_pf` | contrato (remessa mais recente) | INT039 — legado CAIXA |
| bronze | `bronze_sftp_int021_far_bb_pf` | contrato (remessa mais recente) | INT021 — legado BB |
| prata | `prata_far_contrato_pf` | contrato | as três bronzes, sem identificador pessoal |
| prata | `prata_far_empreendimento_natureza` | APF | cadastro PJ + consolidado HIS |
| ouro | `ouro_far_beneficiario_contrato` | contrato | perfil, contratação, situação, inadimplência |
| ouro | `ouro_far_beneficiario_municipio` | município × geração | território e inadimplência |
| ouro | `ouro_far_empreendimento_perguntas` | APF | natureza, panorama, execução e alerta |
| ouro | `ouro_far_contrapartida` | APF × ente × tipo | pactuação dos entes |
| ouro | `ouro_far_cobertura_perguntas` | pergunta | matriz do que falta |

## Regras que o painel assume

- **Inadimplente** = contrato ativo com prestação vencida há mais de 90 dias (regra do
  IC_CONTRATO_INADIMP do layout 015, aplicada também ao legado). A taxa é sobre os ativos.
- No legado CAIXA, **liquidado** é contrato encerrado (saldo zero e data de término); o
  atraso que ainda aparece em parte deles não entra na taxa.
- **Tipo de demanda, originação e qualificação do terreno** seguem o catálogo de metadados
  do HIS_MCIDADES_CONSOLIDADO (SharePoint do MCID), também no cadastro PJ: o layout do
  MONIT_CAD_PJ_FAR troca 003 (Risco) e 004 (Calamidade) e não traz 006 (Agravamento), mas
  o 004 cai nas cheias do RS, AC e PE e o 006 anda com o fluxo Agravamento do consolidado.
- **Sinal de paralisação** = obra iniciada, não concluída, com percentual físico parado há
  3 meses ou mais; **ritmo abaixo do previsto** = 10 p.p. ou mais abaixo do cronograma.

## Lacunas de fonte (o que pedir)

| Pergunta | O que falta |
|---|---|
| Faixa etária | Idade ou faixa etária calculada antes do mascaramento (a data de nascimento é redigida) |
| PCD, situação de rua, tamanho da família, família com idoso | Campos do layout 015 preenchidos (chegam vazios) |
| Raça/cor | Campo em alguma remessa do FAR, ou cruzamento com o CadÚnico |
| Retomadas e passivo | Estoque de imóveis retomados com data e situação; retomada no legado |
| Execução das contrapartidas | O que o ente já entregou (hoje só o pactuado) |
| Equipamentos públicos, aporte/suplementação | Campos do MONIT_CAD_PJ preenchidos (chegam zerados) |
| Calamidade sem habitação destruída | Laudo/cadastro de habitações destruídas (Defesa Civil/S2iD) |
| Déficit, Censo, entorno medido | Déficit habitacional municipal (FJP), Censo 2022 por setor, INEP/CNES/CadSUAS |
| Compra Assistida | Contratos (há só propostas do RS por município) |
