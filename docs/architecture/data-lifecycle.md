# Ciclo de vida de código e dados

## Versionado no Git

- models, macros, testes e YAML do dbt;
- definições de snapshots, sem o resultado materializado;
- contratos, glossário, documentação e ADRs;
- scripts de orquestração e configuração sem segredos.

## Não versionado no Git

- dados Raw, Bronze, Silver ou Gold;
- parquets, tabelas de snapshot e safras;
- `target/`, logs e artefatos dbt;
- credenciais, valores de ambiente e material sensível;
- dados produtivos de seeds.

## Seeds

Seeds são permitidos apenas para referências pequenas, estáticas, não
sensíveis e revisáveis como código. O `boletim_gabarito.csv` é temporário:
será migrado para uma fonte controlada quando houver decisão de retenção e
aprovação de safra.

## Snapshots

Definições de snapshot são código e permanecem versionadas. Tabelas históricas
e sua retenção são dados operacionais; a política de retenção será decidida em
ADR próprio antes de qualquer limpeza ou expansão de snapshots.

Snapshots não são publicados no OpenMetadata, no catálogo semântico ou no
corpus de RAG. A integração exporta exclusivamente modelos dbt elegíveis das
camadas Silver e Gold.
