# Glossário MCID

O glossário fica fora de `models/` porque não é um modelo dbt. A raiz é definida
em `mcid.yaml`, e os termos ficam em `mcid.csv`, no mesmo formato de importação
em massa do OpenMetadata.

A DAG `openmetadata_ingestion_dag` executa `sync_mcid_glossary` antes das recipes.
A task faz `PUT` idempotente da raiz e dos termos, primeiro monta a hierarquia e
depois aplica `relatedTerms`. Ela não remove do OpenMetadata termos retirados do
CSV; exclusões devem passar por revisão de governança.

O token da variável Airflow `INGESTION_TOKEN` precisa ter permissão de criar e
editar `Glossary` e `GlossaryTerm`. O cliente de ingestão está fixado em 1.12.1;
o servidor OpenMetadata deve usar uma versão compatível da mesma linha.

Para associar um termo a um modelo dbt, use o FQN em `meta.openmetadata.glossary`:

```yaml
meta:
  openmetadata:
    glossary:
      - MCID.ProgramasHabitacionais.MCMV
```

O mesmo bloco pode ser usado no `meta` de uma coluna. O termo precisa existir no
OpenMetadata antes da recipe `dbt_metadata`, por isso a sincronização vem antes
da ingestão dos artefatos dbt.
