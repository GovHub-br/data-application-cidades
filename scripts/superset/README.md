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
# Reforma Casa Brasil

O painel **Reforma Casa Brasil — Monitoramento e Evidências** é provisionado
por `reforma_casa_brasil.py` a partir das tabelas da camada Ouro. Ele separa:

- implementação administrativa;
- monitoramento temporal da carteira e dos valores contratuais;
- cobertura e lacunas de evidência para as perguntas da oficina.

Uso:

```bash
python scripts/superset/reforma_casa_brasil.py --dry-run
python scripts/superset/reforma_casa_brasil.py
```

As Golds de acesso e resultado só devem ser incluídas após a materialização do
CadÚnico protegido. Até lá, a aba de cobertura torna essa limitação explícita.
