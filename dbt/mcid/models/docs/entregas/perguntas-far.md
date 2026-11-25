# Perguntas do MCID — MCMV FAR

Modelos e painel criados para responder às perguntas da oficina com o MCID sobre o FAR
(cinco perguntas-mestras, cartões e documento "Perguntas que consigo responder").
Painel no Superset: `/superset/dashboard/perguntas-far/`. A matriz pergunta × dado está
em `ouro_far_cobertura_perguntas` (seed `perguntas_far/cobertura_perguntas_far`).

O novo painel só lê `ouro_far_serie_mensal_apf` do painel FAR existente. A portaria dos
dois painéis é a mesma: a normativa, pela planilha HASH-APF-Portaria do FAR (seed
`far_portaria_apf`).

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
| bronze | `bronze_sftp_cadunico_beneficiario_far` | contrato com titular no CadÚnico | CadÚnico (pessoa 12/12/2025, família 09/01/2026), casado pelo CPF em HMAC |
| prata | `prata_far_contrato_pf` | contrato | as três bronzes, sem identificador pessoal |
| prata | `prata_far_beneficiario_cadunico` | contrato com titular no CadÚnico | raça/cor, PCD, situação de rua, Bolsa Família e gastos; restrita |
| prata | `prata_far_empreendimento_natureza` | APF | cadastro PJ + consolidado HIS |
| prata | `prata_far_consolidado_serie` | proposta × remessa semanal | consolidado HIS, movimento mais recente da semana |
| ouro | `ouro_far_beneficiario_contrato` | contrato | perfil, contratação, situação, inadimplência |
| ouro | `ouro_far_beneficiario_municipio` | município × geração | território e inadimplência |
| ouro | `ouro_far_beneficiario_cadunico` | perfil no CadÚnico × geração × UF × renda | agregada, sem contrato nem município |
| ouro | `ouro_far_empreendimento_perguntas` | APF | natureza, panorama, execução e alerta |
| ouro | `ouro_far_contrapartida` | APF × ente × tipo | pactuação dos entes |
| ouro | `ouro_far_consolidado_historico` | remessa × fase × etapa × status × fluxo × UF × demanda | evolução semanal da seleção |
| ouro | `ouro_far_proposta_trajetoria` | proposta | entrada, saída, ausências e mudanças de situação |
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

- **CadÚnico**: o titular é casado pelo CPF em HMAC (mesmo segredo nas duas fontes). Pessoa
  do corte 12/12/2025 e família do corte 09/01/2026: o de 12/12/2025 tem só metade das
  famílias. É a foto do cadastro no corte, não a da assinatura. Gastos com energia e água
  só valem com cadastro atualizado depois da assinatura e no município do imóvel.
- A bronze do CadÚnico lê o parquet **filtrado pelos CPFs do FAR**. Varrer o CadÚnico
  inteiro pelo pg_duckdb estoura a memória do DW.

## Lacunas de fonte: onde conseguir

A coluna `onde_conseguir` da matriz separa o que pedir do que buscar em fonte pública.

**Pedir ao MCid ou aos agentes**

| Pergunta | O que falta |
|---|---|
| Situação de rua na seleção | IC_BENEF_SIT_RUA preenchido (o CadÚnico só mostra a situação no corte) |
| Retomadas e passivo | Estoque de imóveis retomados com data e situação; retomada no legado |
| Origem da indicação | Quem indicou a demanda além do ente (movimento, defesa civil) |
| Execução das contrapartidas | O que o ente já entregou (hoje só o pactuado) |
| Custo de condomínio | Estimativa de custo de condomínio |
| Equipamentos públicos, aporte/suplementação | Campos do MONIT_CAD_PJ preenchidos (chegam zerados) |
| Compra Assistida | Contratos (há só propostas do RS por município) |

**Fonte pública**

| Pergunta | O que falta |
|---|---|
| Entorno medido | INEP, CNES, CadSUAS e GTFS georreferenciados |
| Déficit habitacional | Déficit municipal (FJP) e Censo 2022 |

**Pedir e fonte pública**: análise por setor censitário (coordenadas do empreendimento +
malha do Censo 2022) e cadastro habitacional local (cadastro do município + déficit da FJP).

**Outro órgão**: calamidade sem habitação destruída precisa do laudo/cadastro de
habitações destruídas (Defesa Civil/S2iD).

**Falta análise, não fonte**: fatores da inadimplência, mudança de público em obras de
calamidade e alerta de paralisação. **Nenhuma fonte conhecida**: gasto com internet por
família (o CadÚnico não coleta).
