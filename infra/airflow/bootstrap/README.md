# Bootstrap do Airflow local

O serviço `airflow-init` do [compose](../../docker-compose.yml) roda a cada
`make up` e termina antes de o `airflow` subir. Ele migra o banco de metadados e
importa o que está nesta pasta:

| Arquivo | Versionado | Conteúdo |
| --- | --- | --- |
| `pools.json` | sim | pools que as DAGs exigem (`duckdb_historico`) |
| `variables.json` | **não** | Variables com os valores reais |
| `connections.json` | **não** | Connections com as credenciais reais |
| `variables.example.json` | sim | modelo do `variables.json`: tudo o que o código lê |
| `connections.example.json` | sim | modelo do `connections.json` |

Os arquivos reais têm credenciais e estão no `.gitignore`. Para criar os seus,
copie o modelo e preencha:

```bash
cp infra/airflow/bootstrap/variables.example.json infra/airflow/bootstrap/variables.json
cp infra/airflow/bootstrap/connections.example.json infra/airflow/bootstrap/connections.json
```

Sem um deles, o `airflow-init` avisa e segue: o Airflow sobe, só sem aqueles itens.

## Como a importação funciona

- Os arquivos usam o formato de import do próprio Airflow (`airflow variables
  import`, `airflow connections import`, `airflow pools import`).
- O arquivo é a fonte da verdade: o valor dele **sobrescreve** o que foi editado
  na UI, a cada `make up`. Para mudar um valor de vez, mude no arquivo.
- Tirar um item do arquivo não o apaga do Airflow. Para apagar, use a UI ou
  `airflow variables delete <chave>`.
- `make airflow-bootstrap` reaplica tudo sem reiniciar o Airflow.
- Os imports de Variables e Connections saem com código 0 mesmo quando um item
  falha. O `airflow-init` procura a mensagem de falha na saída e para, para o
  Airflow não subir faltando item sem ninguém perceber.

## Por que o example precisa estar completo

As DAGs leem `BACEN_SERIES`, `IBGE_CONFIGURACOES` e `dynamic_schedules` com
`default=`. Quando a Variable falta, nada quebra: a `bacen_sgs_ingest_dag`
termina verde sem ingerir nenhuma série, e a `ibge_ingest_dag` gera zero tasks.
Por isso o `variables.example.json` lista tudo o que o código lê, e o
`tests/test_airflow_bootstrap.py` falha quando uma DAG passa a ler uma Variable,
Connection ou pool que não está declarado aqui.

## Problemas comuns

- **"o uid N não consegue ler"**: o container roda com `AIRFLOW_UID` do `.env`.
  Ele precisa ser o seu uid do host (`id -u`) para ler um arquivo com permissão
  `600`. Confira também se `AIRFLOW_UID` não está exportado no shell, que tem
  prioridade sobre o `.env`.
- **A senha tem `"` ou `\`**: escape no JSON (`\"`, `\\`).

## Produção

Vale só para o compose local. Nas VMs e no Kubernetes, `postgres_default` vem da
variável de ambiente `AIRFLOW_CONN_POSTGRES_DEFAULT`, e Variables e pools são
importados à mão (ver `infra/playbooks/airflow/README.md` e `infra/k8s/README.md`).
