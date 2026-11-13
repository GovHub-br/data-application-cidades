# Superset — Conjuntura

Dois scripts, com responsabilidades separadas.

## `bootstrap_conjuntura.py` — infraestrutura

Cria de forma idempotente a conexão `Cidades` e os datasets das Gold. É
pré-requisito do outro: `build_boletim.py` pressupõe que a conexão e as Gold
já estejam registradas.

```bash
poetry run python scripts/superset/bootstrap_conjuntura.py --dry-run
poetry run python scripts/superset/bootstrap_conjuntura.py
```

## `build_boletim.py` — o dashboard do boletim

Um dashboard único (`/superset/dashboard/boletim-conjuntura/`), sete abas —
uma por página do boletim — e um filtro de trimestre no topo. Cada quadro é um
dataset virtual com SQL que reproduz a tabela impressa.

```bash
poetry run python scripts/superset/build_boletim.py                # tudo
poetry run python scripts/superset/build_boletim.py --paginas 3,5  # só 3 e 5
poetry run python scripts/superset/build_boletim.py --dry-run
```

Para incluir ou tirar um chart, mexe-se só na função `pagina_0N()` da página
correspondente, na primeira metade do arquivo.

Reconstruir uma página preserva as demais, lendo o layout já publicado. Mas
uma execução **sem** `--paginas` sobrescreve edições feitas pela interface do
Superset — use `--paginas` quando quiser preservar ajustes manuais.

O `.env` precisa de `SUPERSET_URL`, `SUPERSET_USERNAME` e `SUPERSET_PASSWORD`
(nunca no Git).

## `build_aba_rural_mcid.py` — aba Rural do painel do MCID

Reconstrói só a aba **Rural** do dashboard `mcid-perguntas` (sete seções: visão executiva,
portarias, contratos/empreendimentos/EOs, execução física e financeira, Curva S, empreendimentos fora da
curva, qualidade) e os
filtros nativos `NATIVE_FILTER-RURAL-*`, com escopo só nessa aba. Reaproveita a sessão do
`build_boletim.py`. Não apaga chart, dataset nem outras abas.

Portaria tem **filtro único por tipo**: *Tipo de portaria* (obrigatório, uma opção: Seleção,
Autorização de contratação ou Trabalho Social; padrão Seleção) e *Portaria* (depende do tipo). O
dataset principal é `ouro_rural_painel_portaria` (APF × tipo de portaria), então com um tipo
escolhido cada contrato conta uma vez.

```bash
poetry run python scripts/superset/build_aba_rural_mcid.py --dry-run
poetry run python scripts/superset/build_aba_rural_mcid.py
```

Pré-requisito: materializadas pelo dbt `ouro_rural_painel_portaria`, `ouro_rural_painel_pendencia`,
`ouro_rural_curva_s`, `ouro_rural_curva_s_referencia`, `ouro_rural_curva_s_desvio`,
`ouro_rural_selecao_portarias` e `ouro_rural_portarias_selecao_resumo`.

## `build_secao_curva_s_linhas.py` — Curva S do Pró-Moradia e empenho do Sub-50

Acrescenta no fim das abas do `mcid-perguntas`: no **Pró-Moradia**, a Curva S por tempo de contrato, a execução da
carteira em obra, as obras atrasadas e as sem medição há 12+ meses; no **FNHIS Sub-50**, a evolução do empenho, as
faixas e a situação das parcelas e os termos parados na mesma parcela (o Sub-50 não tem Curva S: sem % físico). Só
troca as linhas que ele mesmo criou (ids `CS-PM-*` e `CS-FN-*`); o resto das abas e os filtros ficam como estão.

```bash
poetry run python scripts/superset/build_secao_curva_s_linhas.py --dry-run
poetry run python scripts/superset/build_secao_curva_s_linhas.py
```

Pré-requisito: `ouro_pro_moradia_curva_s*`, `ouro_fnhis_curva_s` e `ouro_fnhis_empenho_parcelas` materializadas
pelo dbt.
