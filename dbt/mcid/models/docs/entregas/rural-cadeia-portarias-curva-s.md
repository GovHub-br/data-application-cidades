# Rural — cadeia de dados, portarias e Curva S (issue de conferência do Rural, prazo 19/11/2026)

Responsável: Milena Rocha. Conferência feita em 03–04/11/2026, com consultas somente-leitura no banco `cidades`.
O mesmo roteiro vale para o Sub-50 e o Pró-Moradia (próximas rodadas).

## O que mudou

1. **Portarias do Rural além do Trabalho Social.** O painel tinha só a portaria de TS e a seleção 162/2018.
   Agora cada contrato (APF) tem até três portarias, uma por tipo, num filtro único:
   - **Seleção**: 162/2018 (PNHR), 354/2024 (Seleção 2023) e 697/2026 (Seleção 2025);
   - **Autorização de contratação**: 1.133/2024, 1.346/2024, 1.386/2024 e 278/2025 (as 4 já extraídas das 46 da
     Seleção 2023);
   - **Trabalho Social**: 21/2014, 464/2018, 75/2025 (como antes).
2. **Funil seleção → autorização → contrato**, por proposta e por portaria.
3. **Schema drift corrigido no financeiro do Novo Rural** (detalhe abaixo). Ele deixava a Curva S sem o lado
   financeiro.
4. **Curva S físico-financeira** do Novo Rural, por mês, com recorte por portaria, UF e agente.
5. **Catálogo:** colunas documentadas em 5 modelos que divergiam do SQL (`conferir_colunas.py rural_dbt` agora
   não acusa divergência) e tabelas do schema `seeds` com nome que identifica a portaria.

## Linhagem (fonte → bronze → prata → ouro → painel)

| Fonte | Bronze | Prata | Ouro | Painel (aba Rural de `mcid-perguntas`) |
|---|---|---|---|---|
| `seeds.rural_portaria_selecao_354_2024` (PDF compilado de 18/11/2025) | `bronze_seed_manual_rural_portaria_selecao_354_2024` | `prata_rural_selecao_portarias` → `prata_rural_selecao_contrato` → `prata_rural_proposta_funil` | `ouro_rural_selecao_portarias`, `ouro_rural_portarias_selecao_resumo` | Funil; tabela de portarias |
| `seeds.rural_portaria_selecao_697_2026` (Anexo II, compilada) | `bronze_seed_manual_rural_portaria_selecao_697_2026` | idem | idem | idem |
| SFTP GEFUS/FDS — Anexo Rural da 162/2018 | `bronze_sftp_portaria_162_2018_rural` | idem | idem + `ouro_rural_selecao_portaria_162` | idem |
| `seeds.rural_portaria_autorizacao_{1133_2024,1346_2024,1386_2024,278_2025,562_2025,1362_2026,1449_2026}` | `bronze_seed_manual_rural_portaria_autorizacao` | `prata_rural_autorizacao_proposta` → `prata_rural_proposta_funil` | idem | Funil; filtro Autorização |
| `seeds.rural_portaria_enquadramento_1207_2025` (anexos da 1.207/2025) | `bronze_seed_manual_rural_portaria_enquadramento_1207_2025` | `prata_rural_selecao_portarias` → funil (vínculo exato pela APF) | idem | Funil; filtro Seleção (ciclo Calamidade RS 2023) |
| `seeds.rural_portaria_enquadramento_173_2024` (anexo original, transcrito) | `bronze_seed_manual_rural_portaria_enquadramento_173_2024` | — (histórico) | — | — |
| `seeds.rural_portaria_autorizacao_conferencia` | `bronze_seed_manual_rural_portaria_autorizacao_conferencia` | — (teste de completude) | — | — |
| seed `perguntas_mcid/portarias_selecao_rural` (catálogo, 54 portarias) | `bronze_seed_portarias_selecao_rural` | `prata_rural_portarias_selecao` | `ouro_rural_portarias_selecao_resumo` | Tabela de portarias |
| SharePoint `MONIT_MOV_OBRA_RURAL_MENSAL` | `bronze_shpt_monit_mov_obra_rural_mensal` | `prata_rural_obra_mensal_serie` | `ouro_rural_curva_s` | Curva S |
| SharePoint `MONIT_MOV_FINANC_RURAL_MENSAL` | `bronze_shpt_monit_mov_financ_rural_mensal` | `prata_rural_financeiro_mensal` → `prata_rural_evolucao_financeira` | `ouro_rural_curva_s`, `ouro_rural_evolucao_financeira` | Curva S |
| Visão consolidada + Cad PJ + TS CAIXA/BB | (bronzes existentes) | `prata_rural_vinculo_operacao` (agora com seleção, autorização e TS) | `ouro_rural_painel_operacao`, `ouro_rural_painel_portaria`, `ouro_rural_painel_pendencia` | Cartões, tabelas, pendências |

Regra das camadas mantida: bronze lê fonte/seed; prata só lê bronze (a lógica reaproveitada fica em
`macros/corpos/corpos_rural.sql`); ouro só lê prata. A linhagem vai para o OpenMetadata pelo manifest do dbt.

## Fonte, data de referência e cobertura

| Dado | Data de referência | Cobertura |
|---|---|---|
| Seleção 2023 (354/2024) | compilada de 18/11/2025 | 2.110 linhas (3 excluídas pela própria compilada), 75.230 UH |
| Seleção 2025 (697/2026) | compilada com a 948/2026 | 1.217 propostas, 50.000 UH (bate com a meta) |
| Autorizações da Seleção 2023 | 09/10/2024 a 11/06/2025 | **5 de 46** portarias (com a 562/2025, carregada em 04/11/2026); 639 propostas, 25.831 UH |
| Autorizações da Seleção 2025 | 08/10/2026 e 23/10/2026 | 1.362/2026 e 1.449/2026: 28 propostas, 1.366 UH, todas no Anexo II da 697/2026 |
| Calamidade RS — anexos vigentes (redação da 1.207/2025, DOU 20/10/2025) | 2024–2025 | 173/2024: 44 propostas/411 UH; 840/2025: 15/133; 1.207/2025: 7/56 — por APF (o anexo original da 173, 55 propostas/600 UH, fica só como histórico) |
| Catálogo de portarias | conferido em 16/10/2026 | 54 portarias (abertura, seleção, autorização, reaproveitamento) |
| Obra mensal (físico) | retratos dez/2025 a jul/2026 | 1.517 APFs do Novo Rural CAIXA |
| Financeiro mensal | liberações jul/2024 a jul/2026 | 1.582 APFs; 1.557 com APF resolvida |

## Qualidade, drift e testes

**Schema drift no MONIT_MOV_FINANC_RURAL.** O arquivo traz `nu_apf` com 6 dígitos, sem o dígito verificador (ex.
`630767`). O MONIT_MOV_OBRA e o Cad PJ trazem 8 (`63076714`). A prata completava com zeros (`00630767`), então
**nenhuma** APF do financeiro casava com o resto. O efeito era que a `prata_rural_evolucao_financeira` não tinha o
Novo Rural (ela faz inner join com o empreendimento). Correção na `prata_rural_financeiro_mensal`: a APF de 8
dígitos é resolvida pelos 6 primeiros, só quando há um candidato nas bronzes de obra e Cad PJ. Resultado:
1.557 de 1.582 APFs resolvidas. O `pc_evolucao` do financeiro tem correlação de 0,98 com o % de obra da APF
resolvida, o que confirma o casamento. As colunas `nu_apf_origem` e `metodo_chave_apf` ficam para auditoria. A
`prata_rural_evolucao_financeira` passa de 57.492 para 63.090 linhas (+5.598 APF × mês do Novo Rural).

**Funil.**
- As 436 propostas autorizadas estão todas na 354/2024, com o mesmo número de proposta e as mesmas UH.
- Das 436, 382 têm contrato ligado. Todos esses contratos foram assinados depois da autorização: de 21 a
  356 dias, com média de cerca de 185, compatível com o prazo de 180 dias prorrogáveis.
- Com o funil, 1.931 APFs do painel passam a ter portaria de seleção. Antes eram 883, só pela 162/2018.

**Testes novos.**
- Genéricos: `unique`/`not_null` nas chaves, `accepted_values` no tipo/papel da portaria, na etapa do
  funil, nas regras R01–R12 e no método da chave APF, e `relationships` da autorização para o catálogo.
- Singulares (`tests/rural/`):
  - `rural_portaria_autorizacao_bronze_bate_com_conferencia` (erro): a bronze tem as mesmas propostas e UH que a
    extração do PDF leu;
  - `rural_autorizacao_tem_selecao` (aviso): toda proposta autorizada está na seleção, com as mesmas UH;
  - `rural_painel_portaria_tres_linhas_por_apf` (erro): 3 linhas por APF e o mesmo universo da
    `ouro_rural_painel_operacao`;
  - `rural_curva_s_percentuais_validos` (aviso): % físico entre 0 e 100, financeiro até 110% e carteira que
    não encolhe;
  - `rural_financeiro_apf_resolvida` (aviso): pelo menos 95% das APFs do financeiro resolvidas.
- `sem_drift_de_colunas` nas quatro bronzes novas.

**Pendências novas.**
- **R11:** proposta autorizada sem contrato identificado (associação ausente).
- **R12:** contrato ligado assinado antes da autorização (requer investigação). Hoje não tem nenhuma ocorrência.
- **R07** passa a valer para qualquer portaria de seleção.

## Curva S — validação e granularidade

**Grão da `ouro_rural_curva_s`.** A tabela tem uma linha por mês × tipo de portaria × portaria × UF × agente × programa.

**Carteira.** Entram os contratos do Novo MCMV Rural assinados até o mês. O PNHR fica fora porque não tem medição
física mensal.

**Fórmulas.** Todas são razões de somas, nunca média de percentuais:
- físico realizado = Σ(% realizado × UH) ÷ Σ UH com medição;
- físico previsto = Σ(% previsto × UH) ÷ Σ UH com previsão;
- financeiro = Σ liberado acumulado ÷ Σ contratado.

**Meses sem dado.** No físico, o mês sem retrato repete o último retrato, e antes do primeiro retrato
(dez/2025) não há valor. No financeiro, o contrato sem liberação conta 0.

**Validação (nacional, jul/2026).**

| Indicador | Valor |
|---|---|
| Contratos | 1.263 |
| UH | 42.088 |
| Contratado | R$ 3,15 bi |
| Liberado | R$ 1,79 bi (56,8%) |
| Físico realizado | 42,4% |
| Físico previsto | 51,5% |

A curva cresce de forma monótona. Entre dez/2025 e jul/2026 o físico realizado fica de 5 a 14 p.p. abaixo do
previsto, e o financeiro fica cerca de 14 p.p. acima do físico.

**Leitura.** O financeiro inclui obra, Trabalho Social, ATEC, cisternas e custos indiretos, por isso tende a andar
à frente do físico. Para comparar só a obra, use `vr_pago_obra_mes` da `prata_rural_evolucao_financeira`.

**Granularidade do painel.**
- O gráfico de linha soma as colunas de soma por mês e responde aos filtros de tipo de portaria, portaria, UF e
  agente.
- A tabela por portaria usa o último mês.
- Sempre filtre um tipo de portaria antes de somar: cada APF aparece uma vez por tipo.

## Empreendimentos fora da curva (11/11/2026)

A Curva S agregada mostra a média da carteira; para achar **quem foge** dela, cada empreendimento é comparado
com os demais **na mesma idade de contrato** (meses desde a contratação), e não no mesmo mês do calendário.

- `ouro_rural_curva_s_referencia` (grão: idade): p10, p25, mediana, p75 e p90 do % físico realizado, mediana do
  previsto e quartis do % financeiro, de todos os retratos do MONIT_MOV_OBRA naquela idade (mínimo de 10 retratos).
- `ouro_rural_curva_s_desvio` (grão: APF × tipo de portaria): retrato mais recente de cada APF, desvio da mediana
  (p.p.), desvio do próprio cronograma (p.p.) e faixa — Muito abaixo (≤ p10 ou −20 p.p.), Abaixo (< p25), Na curva,
  Acima (> p75), Sem referência.
- As duas leem só prata; a lógica comum está na macro `rural_curva_s_referencia` (macros/rural_curva_s_referencia.sql).
- No painel: seção "Empreendimentos fora da curva" com a curva de referência por idade, contagem por faixa e UF e a
  lista dos que estão abaixo da curva, piores primeiro.

## Limitações

- Faltam 41 das 46 portarias de autorização da Seleção 2023: em 04/11/2026 os links delas no site do MCID devolvem 404 (e a 1.242/2024, que abre, tem texto com fonte codificada). As da Seleção 2025 (1.362 e 1.449/2026) estão carregadas. Até lá, "Contratada sem
  autorização carregada" é falta de dado, não irregularidade.
- **Vínculo das seleções 354/2024 e 697/2026 com o contrato.** O anexo não traz a APF, então o vínculo é por
  CNPJ da EO + município, e a coluna `confianca_vinculo` diz o quanto ele é firme. Exemplo da 354/2024: 145
  vínculos de confiança alta, 571 média e 212 baixa. Um ganho possível é exigir contrato depois da autorização
  quando ela existir.
- A 697/2026 é de jun/2026 e ainda não tem contrato ligado.
- Curva física só de dez/2025 em diante e só para a CAIXA (o MONIT_MOV_OBRA não cobre BB nem PNHR).
- 25 APFs do financeiro (1,6%) não resolvem para 8 dígitos e ficam fora da Curva S.
- A 16/2025 (correção de município de uma proposta) não aparece na compilada da 354/2024. Ver
  `entendimento/rural_portarias_selecao.md`.

## Como rodar (com VPN, na raiz do repo)

```
set -a; source .env; set +a
python scripts/governance/conferir_colunas.py rural_dbt

cd dbt/mcid
poetry run dbt seed  --profiles-dir . --select portarias_selecao_rural
poetry run dbt build --profiles-dir . --select \
  bronze_seed_portarias_selecao_rural bronze_seed_manual_rural_portaria_selecao_354_2024 bronze_seed_manual_rural_portaria_selecao_697_2026 \
  bronze_seed_manual_rural_portaria_autorizacao bronze_seed_manual_rural_portaria_autorizacao_conferencia \
  path:models/rural_dbt/prata path:models/rural_dbt/ouro prata_ts_selecao_contrato+
cd ../..
```

OpenMetadata (com o `.venv-om` ativo), depois do build:

```
python rodar_openmetadata_pro_moradia_fnhis.py --pastas rural_dbt
deactivate
cd scripts/governance && poetry run python sincronizar_governanca.py --confirmar && cd ../..
```

Superset (aba Rural de `mcid-perguntas`):

```
poetry run python scripts/superset/build_aba_rural_mcid.py --dry-run
poetry run python scripts/superset/build_aba_rural_mcid.py
```

Para carregar uma autorização nova:
1. Extraia o PDF.
2. Carregue `seeds.rural_portaria_autorizacao_<numero>_<ano>` com o mesmo layout e acrescente a linha dela em
   `seeds.rural_portaria_autorizacao_conferencia`.
3. Declare a tabela em `models/seeds_manuais.yml` e acrescente o nome na lista de
   `bronze_seed_manual_rural_portaria_autorizacao.sql`.
4. No catálogo `portarias_selecao_rural.csv`, troque `fonte_lista` da portaria para a tabela nova.
