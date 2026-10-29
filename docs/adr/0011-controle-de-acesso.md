# 0011. Controle de acesso

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (usuário do RustFS criado à mão; segredos em Connection, ADR-0013; sem schema `comum`; schema `elementary`)
- **Relacionados:**
  - ADR-0013 (segredos em Connection)
  - ADR-0002 (R0 e a chave do HMAC; R6, consumo só pela ouro), ADR-0005 (raw sem
  remoção), ADR-0007 e ADR-0010 (schema por secretaria, camada no prefixo)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seção 3
  - `infra/playbooks/postgres/README.md`, `infra/playbooks/rustfs/`,
  `infra/playbooks/airflow/`

## Contexto

O deck propõe o princípio do menor privilégio: escrita separada de leitura, um
usuário de leitura por secretaria, papéis no Airflow para cada equipe ver e
disparar só as suas DAGs, e permissões versionadas junto com o código.

Hoje:

| Onde | Como está |
|---|---|
| Postgres | `cidades` é dono do banco e escreve (dbt e DAGs). `usuario_leitura` lê **tudo** que o `cidades` criar, por `ALTER DEFAULT PRIVILEGES`, e serve ao Superset e aos analistas. `duckdb_users` é o papel de quem executa DuckDB. |
| RustFS | O usuário `app-cidades` lê e escreve no bucket, para o pg_duckdb e o Airflow. Ele é criado à mão (`infra/playbooks/postgres/README.md`): o playbook do RustFS não cria usuários nem políticas. |
| Airflow | `SimpleAuthManager`, com um usuário administrador. |
| dbt | Nenhuma configuração de `grants`. |

Com o `usuario_leitura` lendo tudo, nada impede um painel de ler a bronze ou a
prata, o que contraria a R6 do ADR-0002. Com o schema por secretaria (ADR-0010), as
três camadas dividem o mesmo schema, então o schema sozinho não basta para
separar o que cada um pode ler. E o `SimpleAuthManager` é feito para
desenvolvimento, sem papéis.

## Decisão

### 1. Papéis no Postgres

| Papel | Lê | Quem usa |
|---|---|---|
| `cidades` | Tudo, e escreve | dbt, DAGs |
| `leitura_ouro_<sigla>` | Tabelas `ouro_` do schema da secretaria | Superset, planilhas, boletins |
| `leitura_analise_<sigla>` | Tabelas `prata_` e `ouro_` do schema da secretaria | Analistas e ciência de dados da secretaria |

- Tabelas `bronze_` e os schemas técnicos (`lake`, `metadata`, `elementary`) ficam só com o
  `cidades`.
- Usuário de login recebe papel, nunca permissão direta. O usuário do Superset é
  membro dos papéis `leitura_ouro_<sigla>` das secretarias que ele atende.
- O `usuario_leitura` atual vira membro de `leitura_ouro_snh`. Quem precisa de mais
  pede `leitura_analise_snh`.

### 2. Permissões versionadas

- Os papéis são criados pelo playbook do Postgres (`infra/playbooks/postgres/`),
  que já é código.
- As permissões sobre as tabelas são dadas pelo dbt, com `+grants`. Como as três
  camadas dividem o schema, o grant é por tabela, e a pasta de camada dentro de cada
  domínio diz qual papel recebe o quê. O dbt reaplica os grants a cada execução,
  então uma permissão dada à mão some na próxima carga.
- O `USAGE` no schema da secretaria é dado aos dois papéis dela pelo playbook.
- O `ALTER DEFAULT PRIVILEGES` amplo sai do playbook.

```yaml
models:
  mcid:
    far_dbt:
      +schema: snh
      bronze: { +grants: { select: [] } }
      prata:  { +grants: { select: [leitura_analise_snh] } }
      ouro:   { +grants: { select: [leitura_ouro_snh, leitura_analise_snh] } }
```

O formato final depende da estrutura de pastas do `dbt_project.yml`; o princípio é
um grant por camada, dentro de cada domínio, herdado pelos modelos.

### 3. Lake

- O `app-cidades` lê e escreve em `raw/` e `staging/`, mas não apaga em `raw/`
  (ADR-0005). Remoção é com um usuário de manutenção.
- Analistas não acessam o lake. O consumo é pelo warehouse (ADR-0001).

### 4. Airflow

A homologação troca o `SimpleAuthManager` pelo auth manager do FAB, com papéis:

| Papel | Pode |
|---|---|
| `infra` | Administrar o Airflow |
| `engenharia_dados` | Ver, disparar e limpar todas as DAGs |
| `leitura` | Ver DAGs e logs |

Quando houver equipes por secretaria, cada DAG declara `access_control` para o
papel da sua equipe. O ambiente local continua com o `SimpleAuthManager`.

### 5. Segredos

- Senhas e chaves ficam fora do repositório, como já é hoje
  (`~/.clusterlab/credentials/`).
- A `MASKING_HMAC_SECRET` segue as regras do ADR-0002 R0: só no ambiente do
  Airflow, sem aparecer na interface, e com custódia do ministério no ambiente
  dele.
- Segredo do Airflow fica em Connection, nunca em Variable (ADR-0013), e usa o
  mascaramento do próprio Airflow.

## Alternativas consideradas

**Manter um usuário de leitura para tudo.** É o mais simples, mas não impõe a R6 e
não separa secretarias.

**Grant por schema, sem olhar a camada.** Simples com um schema por camada, mas,
com o schema por secretaria, daria ao Superset acesso à bronze e à prata.

**Apache Ranger.** Dá políticas por coluna e por linha e auditoria de cada acesso,
mas só protege o que passa pelo Trino, que o ADR-0010 deixou de fora.

**Grants só no playbook, sem o dbt.** Tabelas recriadas pelo dbt perderiam as
permissões dadas fora dele.

## Trade-offs

| Ganha | Paga |
|---|---|
| A R6 passa a ser imposta pelo banco, não só pela revisão | Painéis que hoje leem prata ou bronze param até serem reapontados |
| Cada secretaria lê só o que é dela | Dois papéis por secretaria para manter |
| Permissão dada à mão não sobrevive, então não se acumula | Toda permissão nova é um PR |
| Airflow com papéis, pronto para mais equipes | Mais um componente (FAB) na homologação |

## Consequências

- Antes de restringir o `usuario_leitura`, é preciso listar os datasets do Superset
  que leem fora da ouro e reapontá-los. Os scripts em `scripts/superset*` já fazem
  esse tipo de reapontamento.
- O playbook do Postgres ganha a criação dos papéis e perde o `ALTER DEFAULT
  PRIVILEGES` amplo.
- O playbook do Airflow passa a configurar o auth manager do FAB na homologação.

## Plano de migração

1. Listar os datasets do Superset fora da ouro e reapontá-los.
2. Criar os papéis da SNH no playbook e os `+grants` no `dbt_project.yml`, junto
   com a mudança para o schema `snh` (ADR-0007).
3. Tornar o `usuario_leitura` membro de `leitura_ouro_snh` e retirar a leitura
   ampla.
4. Passar a criar o `app-cidades` e a política dele pelo playbook do RustFS, sem
   permissão de apagar em `raw/`.
5. Trocar o auth manager da homologação.

## Validação operacional

- Conectado como `leitura_ouro_snh`, um `select` numa tabela `prata_` ou `bronze_`
  falha.
- Nenhuma permissão no banco existe fora das declaradas no playbook e no dbt
  (comparação periódica com `information_schema.role_table_grants`).
- O `app-cidades` recebe erro ao apagar em `raw/`.

## Questões em aberto

1. **Políticas e perfis que o ministério já tem.** O que pode ser reaproveitado
   (pendência da seção 3 do deck). Se o ministério já aplica política equivalente,
   ela prevalece.
