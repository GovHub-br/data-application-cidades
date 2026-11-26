# B5 · Senhas em texto puro na saída do Ansible

> Parte do [guia das refatorações](README.md) · Prioridade P0 · Status: issue aberta · Issue: [#252](https://github.com/GovHub-br/data-application-cidades/issues/252) · PR: —

## Problema

Os `debug` finais dos playbooks imprimem senhas e não têm `no_log`:

- `infra/playbooks/airflow/install-airflow.yaml:232`
- `infra/playbooks/postgres/install-postgres.yaml:401-403`
- `infra/playbooks/rustfs/install-rustfs.yaml:283`
- `infra/playbooks/openmetadata/install-openmetadata.yaml:349`

## Correção proposta

- Colocar `no_log: true` nesses `debug`.
- Indicar o caminho do arquivo de credenciais em vez de imprimir o valor.

## Critério de aceite

`ansible-playbook --check` não mostra nenhuma senha na saída.
