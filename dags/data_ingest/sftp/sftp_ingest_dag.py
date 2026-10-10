"""SFTP do MCid (acesso fábrica): uma família de arquivo por dataset, em `raw/sftp/`.

Fonte: o SFTP da CAIXA/BB para o MCid, conta `fabrica` (Connection `sftp_mcid`),
pastas `GEFUS/` (com `ANTERIORES/`, `FDS/` e `CadUnico/`) e `Analise_SNH/`. A
pasta do GEAVO pede outra senha e fica de fora (o `CCI_CCA` segue na staging
antiga).

Cada família é um dataset do domínio `sftp`. As famílias vêm da Variable
`SFTP_FAMILIAS` (valor canônico em `infra/airflow/variables/SFTP_FAMILIAS.json`),
lida dentro das tasks:

    {"dataset": "int040_far_caixa_empreendimentos",
     "nome": "INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS_[^/.]*",
     "pacotes_2023": true}

`nome` é a regex do nome do arquivo sem extensão; `pacotes_2023` faz a família
entrar nos pacotes mensais de 2023; `pasta` é `GEFUS` (padrão) ou `Analise_SNH`.


- extração incremental: o manifesto da raw guarda nome, tamanho e data de cada
  arquivo da fonte, e a execução seguinte só baixa o que mudou;
- a mesma entrega em outra pasta ou embrulho (`X.TXT`, `X.TXT.zip`, `X.zip`, em
  `GEFUS/` ou `ANTERIORES/`) pousa uma vez, na precedência csv > txt > xlsx > zip;
- os pacotes mensais de 2023 (`ANTERIORES/202310.zip`, `202311.zip`,
  `202312_CAIXA.zip`), que trazem várias interfaces num zip só, entram nas
  famílias INT; quando trazem um arquivo que também veio solto, fica o solto;
- ficam de fora as versões substituídas (`_substituido`), uploads em curso
  (`.filepart`), temporários do Excel (`~$`), `.7z` vazios e os avulsos que
  não são série (legislação, planilhas soltas, teste de transmissão).

Antes do pouso, no worker: zip e gzip viram os arquivos de dentro (`Unpack`) e o
dado pessoal é mascarado (`MaskPii`, as regras do antigo `mascarar_minio`: HMAC
em CPF/NIS com `MASKING_HMAC_SECRET`, redação de nome, endereço, CEP e
nascimento). A raw nunca guarda PII e não é reescrita depois.

Conversão: texto com encoding e delimitador detectados por arquivo (as entregas
variam) e linha com campos a mais ou a menos descartada e contada; xlsx aba a
aba. A staging guarda o cabeçalho original, e o macro `normalizar_colunas`
entrega às pratas os nomes de sempre.

LoadMode: overwrite em todas. Cada entrega é um retrato completo (a base inteira
do mês, ou a série inteira mais os novos: as contagens se repetem de uma entrega
para a outra), então a nova substitui a anterior. A extração incremental grava
uma ingestão por entrega, na ordem de chegada na fonte; o latest/ fica com a
mais recente e aponta a anterior como antecessora, que continua na partição
dela. Na primeira carga, o histórico entra como uma sequência de ingestões.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec, Prepare
from ingestion.extractors import RemoteFiles, sftp
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps
from ingestion.prepare import MaskPii, Unpack

PASTAS = {"GEFUS": "/home/fabrica/GEFUS", "Analise_SNH": "/home/fabrica/Analise_SNH"}
CAMPOS = {"dataset", "nome", "pacotes_2023", "pasta"}

# Extensões que a conversão lê, e embrulhos que o Unpack abre (X.TXT.zip).
EXTENSOES = r"\.(csv|txt|xlsx|zip|gz)(\.(zip|gz))?$"
PRECEDENCIA = (".csv", ".txt", ".xlsx", ".zip", ".gz")
EXCLUIR = (r"(?i)substitu", r"(^|/)~\$", r"\.filepart$")
PACOTES_2023 = r"^ANTERIORES/\d{6}(_CAIXA)?\.zip$"
MEMBROS_TABULARES = r"(?i)\.(csv|txt|xlsx)$"

# Arquivo sem cabeçalho: as colunas de CPF e NIS vão por posição.
POSICOES_PII = {
    r"^CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112\.TXT$": {2: "cpf", 3: "nis"},
}


def _validar(familia: Mapping[str, Any]) -> None:
    desconhecidos = set(familia) - CAMPOS
    faltando = {"dataset", "nome"} - set(familia)
    if desconhecidos or faltando or familia.get("pasta", "GEFUS") not in PASTAS:
        raise ValueError(
            f"família de SFTP_FAMILIAS inválida: {dict(familia)} "
            f"(campos {sorted(CAMPOS)}, pasta em {sorted(PASTAS)})"
        )


def arquivos(familia: Mapping[str, Any]) -> RemoteFiles:
    """Os arquivos de uma família: o nome dela com as extensões que a conversão lê."""
    _validar(familia)
    return RemoteFiles(
        root=PASTAS[familia.get("pasta", "GEFUS")],
        pattern=rf"(?i)(^|/){familia['nome']}{EXTENSOES}",
        exclude=EXCLUIR,
        prefer_extensions=PRECEDENCIA,
        bundles=PACOTES_2023 if familia.get("pacotes_2023") else None,
    )


def preparo(familia: Mapping[str, Any]) -> tuple[Prepare, ...]:
    """Descompacta (do pacote, só a família; dos outros zips, o que for tabela) e
    mascara a PII antes do pouso."""
    pacotes = bool(familia.get("pacotes_2023"))
    membros = (
        rf"(?i)^{familia['nome']}\.(csv|txt|xlsx)$" if pacotes else MEMBROS_TABULARES
    )
    return (
        Unpack(members=membros, require_match=not pacotes),
        MaskPii(positions=POSICOES_PII),
    )


FAMILIA = DatasetSpec(
    domain="sftp",
    dataset=lambda familia: familia["dataset"],
    extractor=sftp(conn_id="sftp_mcid", remote=arquivos),
    prepare=preparo,
    # As interfaces mandam umas poucas linhas quebradas em arquivos de milhões (a
    # INT039 de 2026-04-30: 2 linhas com 40 campos num layout de 37): elas saem e a
    # contagem vai para o manifesto da staging, como no raw_para_staging.
    converter=ConverterConfig(encoding="auto", delimiter="auto", bad_rows="skip"),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="sftp_ingest_dag",
    # Diário à 01:00: as remessas chegam ao longo do dia, e a carga (PF FGTS,
    # CadÚnico) é pesada para o horário do expediente.
    schedule="0 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Gustavo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["sftp", "mcid", "cidades", "ingestion"],
)
def sftp_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        familias = Variable.get("SFTP_FAMILIAS", deserialize_json=True)
        try:
            return steps.extract_to_raw(FAMILIA, context["dag_run"].run_after, familias)
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, list[str]]:
        familias = Variable.get("SFTP_FAMILIAS", deserialize_json=True)
        return steps.convert_to_staging(FAMILIA, raw, familias)

    convert_to_staging(extract_to_raw())


dag_instance = sftp_ingest_dag()
