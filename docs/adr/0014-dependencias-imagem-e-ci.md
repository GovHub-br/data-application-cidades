# 0014. Dependências, imagem e CI

- **Status:** Proposto
- **Data:** 2026-10-29
- **Relacionados:**
  - ADR-0003 (ambientes), ADR-0013 (teste de DagBag)
  - Issues #243 (F1), #246 (poetry → uv), #253 (CI blocante), #254 (pre-commit),
    #255 (imagem única) e #256 (Pages e dependabot)
  - `pyproject.toml`, `requirements.txt`, `infra/docker/airflow/Dockerfile`,
    `infra/docker/airflow/compile-lock.sh`, `.github/workflows/main.yaml`,
    `setup-git-hooks.sh`, `Makefile`

## Contexto

As dependências, a imagem e a CI cresceram sem uma regra comum. A varredura de 29/10
achou:

- **Quatro listas de dependências e nenhuma fonte de verdade.**
  - O `pyproject.toml` (poetry) serve aos testes e à CI.
  - O `requirements.txt` é a entrada do lock da imagem.
  - O `infra/docker/airflow/requirements.lock.txt` é o que de fato se instala,
    compilado com uv contra a imagem base.
  - O `requirements.generated.txt` é local, está parado no Airflow 2.8.1 e ninguém o
    lê.
- **Lock sem versão.** O `poetry.lock` nunca foi versionado: o `**/*.lock` do
  `.gitignore` o ignora, e o `make setup` re-resolve a cada execução. Com isso, a
  chave de cache da CI fica sempre vazia, e o cache nunca renova.
- **As listas divergem.** O `sqlglot` está em `^30` nos testes e em `29.0.1` na
  imagem. Pacotes importados direto pelo código (`yaml`, `psycopg2`, `paramiko`,
  `httpx`, `bs4`, `chardet`, `pytz`, `packaging`) só chegam como dependência de
  outros.
- **Versão do Airflow repetida.** O 3.3.2 está fixado em seis lugares.
- **Três imagens.**
  - A CI publica `ghcr.io/govhub-br/data-application-cidades-airflow`, buildando
    duas vezes.
  - O k8s espera `ghcr.io/govhub-br/cidades-airflow:main`, publicada à mão.
  - A VM builda a própria imagem no playbook.
  - Não há `.dockerignore`, então o contexto do build leva `data-science/` (32 MB) e
    o `.git`.
  - A imagem carrega a senha padrão `airflow/airflow` e bibliotecas de sistema sem
    uso (gdal, odbc, gtk).
- **A CI não barra nada.**
  - O lint roda com `|| true`, e black, ruff e mypy não rodam.
  - O `ruff check .` acusa 950 erros.
  - Não há `dbt parse` nem teste de DagBag.
- **Os hooks locais não servem.** O pre-commit formata o repositório inteiro e falha
  sempre, então o time commita com `--no-verify`.
- **O dependabot aponta para um caminho que não existe,** e os PRs de pip dele
  quebram o teste do lock por construção.

## Decisão

1. **O uv é o gerenciador, e o `pyproject.toml` (PEP 621) é a única declaração de
   dependências.**
   - Os grupos são três: `runtime` (o que vai para a imagem), `test` e `dev`.
   - O `uv.lock` é versionado, e a CI instala com `uv sync --locked`.
   - Saem o `requirements.txt`, o `requirements.generated.txt` e o poetry.
2. **A imagem é instalada a partir do `uv.lock`.** O lock da imagem
   (`requirements.lock.txt`) passa a ser gerado do `uv.lock`, só com o grupo
   `runtime`, e um teste no CI confere que os dois batem.
3. **Todo pacote importado direto pelo código é declarado**, e as versões de
   desenvolvimento também ficam fixadas no lock.
4. **A versão do Airflow e a do Python ficam declaradas num lugar só.** O
   `Dockerfile`, o `values.yaml` do k8s e o compose leem dela, ou são conferidos por
   teste.
5. **Uma imagem só.**
   - A CI builda uma vez por commit na `main`, com `.dockerignore`, e publica com a
     tag do commit.
   - A VM e o k8s sobem essa tag: o playbook deixa de buildar.
   - A senha padrão sai da imagem e fica só no compose local.
   - As bibliotecas de sistema sem uso saem.
6. **A CI é blocante.**
   - O ruff (lint e formatação) roda em `dags/`, `plugins/`, `helpers/`, `scripts/`
     e `tests/`.
   - Também rodam o `dbt parse`, sem banco, e o `pytest` com o DagBag (ADR-0013).
   - O job que falha bloqueia o merge.
   - Os erros que já existem entram numa lista de exceções por arquivo, que só pode
     diminuir.
7. **O pre-commit usa o framework `pre-commit`.** Ele roda só nos arquivos do commit,
   com as mesmas ferramentas e versões da CI. O `setup-git-hooks.sh` sai.
8. **O dependabot roda só nos manifestos reais:** uv, GitHub Actions e Docker.

## Alternativas consideradas

**Manter o poetry e versionar o `poetry.lock`.** Resolveria o lock e o cache, mas a
imagem já é montada com uv (`compile-lock.sh`), e o projeto continuaria com duas
ferramentas para a mesma coisa.

**Manter o `requirements.txt` como entrada da imagem.** É o que funciona hoje, mas é
a segunda lista que diverge da primeira, como mostra o `sqlglot`.

**Buildar a imagem em cada ambiente.** Dá autonomia à VM, mas cada ambiente roda uma
imagem diferente, e o que foi testado na CI não é o que sobe.

**Lint só como aviso até zerar os erros.** É o que existe hoje, e os erros só
aumentaram. A lista de exceções que só diminui permite barrar o novo sem exigir
corrigir tudo antes.

## Trade-offs

| Ganha | Paga |
|---|---|
| Uma lista de dependências, com lock versionado | A migração do poetry mexe em Makefile, CI, testes e README de uma vez |
| A imagem testada na CI é a que sobe na VM e no k8s | A VM passa a depender do registry para subir |
| Erro novo de lint, de parse do dbt ou de import de DAG não entra na `main` | PRs ficam vermelhos com mais frequência no começo |
| Hook local rápido e igual à CI | O time instala o `pre-commit` |

## Consequências

- O `pyproject.toml` perde o `[tool.poetry]` e ganha `[project]` e
  `[dependency-groups]`. O `.gitignore` deixa de ignorar `*.lock`.
- O `tests/test_dependencias_sincronizadas.py` passa a comparar o `uv.lock` com o
  lock da imagem.
- O playbook do Airflow e o `values.yaml` do k8s passam a usar a imagem publicada
  pela CI.
- O `--no-verify` deixa de ser necessário.

## Plano de migração

1. Trocar o poetry pelo uv, com o lock versionado e a CI sobre `uv sync --locked`
   (#246).
2. Gerar o lock da imagem a partir do `uv.lock` e trocar o teste de sincronia.
3. Ligar o ruff, o `dbt parse` e o DagBag como jobs blocantes, com a lista de
   exceções (#253), e o `pre-commit` (#254).
4. Imagem única com `.dockerignore`, consumida pela VM e pelo k8s (#255).
5. Dependabot nos manifestos reais (#256).

## Validação operacional

- `uv sync --locked` passa na CI, e o `uv.lock` muda só em PR.
- O lock da imagem e o `uv.lock` batem (teste no CI).
- A tag que roda na VM e no k8s é uma tag publicada pela CI.
- A lista de exceções do lint não cresce entre dois commits da `main`.

## Questões em aberto

1. **Lock e imagem base.** A imagem `apache/airflow` já traz centenas de pacotes
   fixados pelas constraints do Airflow. Falta definir como o `uv.lock` respeita
   essas versões: se o lock é resolvido com as constraints do Airflow como
   restrição, ou se só o grupo `runtime` é conferido contra a imagem. Decidir na
   #246.
