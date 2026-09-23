# Reforma Casa Brasil

Produto de dados para responder, dentro da cobertura das fontes administrativas,
quais características do programa estão associadas ao acesso e à produção de melhoria
habitacional. O produto separa claramente **caracterização**, **implementação** e
**resultado**; perguntas que exigem pesquisa ou acompanhamento pós-obra aparecem como
lacuna, e não como resultado igual a zero.

## Fluxo automatizado

1. `minio_transform_dag` protege PII na `raw/`, converte os arquivos para Parquet na
   `staging/` e dispara `mcid_cosmos_dag`.
2. `mcid_cosmos_dag` materializa Bronze, Prata e Ouro pelo dbt e, ao terminar, dispara
   `openmetadata_ingestion_dag`.
3. `openmetadata_ingestion_dag` gera `manifest.json`, `catalog.json` e
   `run_results.json`, publica descrições, testes e linhagem no OpenMetadata e reaplica
   as regras de governança.

## Fontes e grãos

| Fonte | Objeto lógico | Grão | Atualização |
|---|---|---|---|
| SFTP/GEFUS | `PMCMV_REFORMAS_MCID_*` | contrato no snapshot | snapshot completo; seleciona o mais recente |
| SFTP/GEFUS/CadÚnico | `ARQ_PESSOA_PBF_12122025_SB8` | pessoa | corte completo |
| SharePoint/CadÚnico | `ARQ_FAMILIA_PBF_12122025_SB8` | família | corte completo |

CPF e NIS são tokens HMAC determinísticos gerados antes da `staging`; nomes, apelidos,
nascimento e endereços são redigidos. A Prata usa CPF HMAC e NIS HMAC como fallback
para ligar contrato → pessoa → família. A Ouro nunca publica esses tokens.

## Camadas

- **Bronze:** cópia tipada como texto dos Parquets de staging e metadados de origem.
- **Prata:** contratos tipados e deduplicados, ponte pseudonimizada de pessoas,
  características domiciliares e integração no grão de contrato.
- **Ouro:** agregados para acesso, implementação e linha de base de resultado; células
  com menos de dez contratos são suprimidas.

O indicador de inadequação observável é uma proxy operacional da linha de base do
CadÚnico. Ele não substitui a metodologia oficial de inadequação habitacional e não
mede causalidade ou efeito pós-obra. A Gold de cobertura explicita quais perguntas
dependem de questionário, medição de obra ou pesquisa longitudinal.
