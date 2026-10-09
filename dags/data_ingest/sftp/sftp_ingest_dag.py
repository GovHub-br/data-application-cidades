"""SFTP do MCid (acesso fábrica): uma família de arquivo por dataset, em `raw/sftp/`.

Fonte: o SFTP da CAIXA/BB para o MCid, conta `fabrica` (Connection `sftp_mcid`),
pastas `GEFUS/` (com `ANTERIORES/`, `FDS/` e `CadUnico/`) e `Analise_SNH/`. A
pasta do GEAVO pede outra senha e fica de fora (o `CCI_CCA` segue na staging
antiga).

Cada família é um dataset do domínio `sftp` (a lista mora aqui; mudar uma família
é PR):

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
variam) e linha com campos a mais ou a menos descartada e contada; xlsx aba a aba; a staging guarda o cabeçalho original, e o macro
`normalizar_colunas` entrega às pratas os nomes de sempre.

LoadMode: append em todas. Cada arquivo é uma entrega (um retrato do mês, uma
remessa de liberações); a bronze que quer só o último usa `arquivo_mais_recente`.
`overwrite` não serve aqui: com a extração incremental, a última ingestão traz só
o que chegou de novo (na primeira, o histórico inteiro).
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import TaskGroup, dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, RemoteFiles
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps
from ingestion.prepare import MaskPii, Unpack

CONN_ID = "sftp_mcid"
GEFUS = "/home/fabrica/GEFUS"
ANALISE_SNH = "/home/fabrica/Analise_SNH"

# Extensões que a conversão lê, e embrulhos que o Unpack abre (X.TXT.zip).
EXTENSOES = r"\.(csv|txt|xlsx|zip|gz)(\.(zip|gz))?$"
PRECEDENCIA = (".csv", ".txt", ".xlsx", ".zip", ".gz")
EXCLUIR = (r"(?i)substitu", r"(^|/)~\$", r"\.filepart$")
PACOTES_2023 = r"^ANTERIORES/\d{6}(_CAIXA)?\.zip$"
MEMBROS_TABULARES = r"(?i)\.(csv|txt|xlsx)$"

# (dataset, nome do arquivo sem extensão (regex), entra nos pacotes de 2023)
FAMILIAS: tuple[tuple[str, str, bool], ...] = (
    # Dados prioritários da SNH (retratos mensais, `<aaaamm>_` ou `<aaaa>_<mm>_`)
    (
        "snh_dados_prioritarios_af_bb",
        r"\d{4}_?\d{2}_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB(_vs\d+(_correcao)?)?",
        False,
    ),
    (
        "snh_dados_prioritarios_af_caixa",
        r"\d{4}_?\d{2}_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA",
        False,
    ),
    (
        "snh_dados_prioritarios_af_caixa_entregas",
        r"\d{4}_?\d{2}_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA_ENTREGAS",
        False,
    ),
    (
        "snh_dados_prioritarios_da_entrega_da_unidade_af_bb",
        r"\d{4}_?\d{2}_SNH_PMCMV_DADOS_PRIORITARIOS_DA_ENTREGA_DA_UNIDADE_AF_BB",
        False,
    ),
    (
        "dados_prioritarios_contratacoes_semanal",
        r"Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_[^/.]*",
        False,
    ),
    # Interfaces (INT): empreendimentos, liberações e pessoa física
    ("int021_far_bb_pf", r"INT021_MinisterioCidades_FAR_BB_PF_[^/.]*", True),
    ("int039_far_caixa_pf", r"INT039_MinisterioCidades_FAR_CAIXA_PF_[^/.]*", True),
    (
        "int040_far_caixa_empreendimentos",
        r"INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS_[^/.]*",
        True,
    ),
    ("int042_fds_caixa_pf", r"INT042_MinisterioCidades_FDS_CAIXA_PF_[^/.]*", True),
    (
        "int054_far_bb_empreendimentos",
        r"INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS_[^/.]*",
        True,
    ),
    (
        "int055_liberacoes_caixa_bb",
        r"INT055_MinisterioCidades_LIBERACOES_CAIXA_BB_[^/.]*",
        False,
    ),
    (
        "int057_pnhr_bb_empreendimentos",
        r"INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS_[^/.]*",
        True,
    ),
    ("int058_pnhr_bb_pf", r"INT058_MinisterioCidades_PNHR_BB_PF_[^/.]*", True),
    (
        "int059_fds_caixa_empreendimentos",
        r"INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS_[^/.]*",
        True,
    ),
    ("int064_pnhr_caixa_pf", r"INT064_MinisterioCidades_PNHR_CAIXA_PF_[^/.]*", True),
    (
        "int065_pnhr_caixa_empreendimentos",
        r"INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS_[^/.]*",
        True,
    ),
    (
        "int068_solicitacao_liberacao_obra",
        r"INT068_MDR_Solicitacao_Liberacao_Obra_[^/.]*",
        False,
    ),
    # Andamento de obra
    ("bb_af_diemp_andamento_obra", r"BB_AF_DIEMP_ANDAMENTO_OBRA_[^/.]*", False),
    (
        "caixa_af_gehis_andamento_obra",
        r"(CAIXA_AF_GEHIS_ANDAMENTO_OBRA_[^/.]*|\d{6}_ANDAMENTO_OBRA_AF_CAIXA[^/.]*)",
        False,
    ),
    ("base_andamento_obra", r"BASE_ANDAMENTO_OBRA_[^/.]*", False),
    # Outras bases do GEFUS
    (
        "caixa_af_gehis_alienacao_imovel",
        r"CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_[^/.]*",
        False,
    ),
    (
        "caixa_af_gehis_operacao_desenquadrada",
        r"CAIXA_AF_GEHIS_OPERACAO_DESENQUADRADA_[^/.]*",
        False,
    ),
    ("calculo_atuarial_produto", r"CALCULO_ATUARIAL_PRODUTO_[^/.]*", False),
    ("pmcmv_reformas_mcid", r"PMCMV_REFORMAS_MCID_[^/.]*", False),
    ("pmcmv_faixa3_mcid", r"PMCMV_FAIXA3_MCID_[^/.]*", False),
    ("pmcmv_cidades_mcid", r"PMCMV_CIDADES_MCID_[^/.]*", False),
    ("fundo_social", r"FUNDO_SOCIAL_[^/.]*", False),
    ("mont_cont_pf_far", r"MONT_CONT_PF_FAR_[^/.]*", False),
    ("mcmv_cidades_emendas", r"MCMV_CIDADES_EMENDAS_[^/.]*", False),
    # Canal FGTS (as mesmas bases em GEFUS/, GEFUS/FDS/ e ANTERIORES/)
    ("geavo_base_pf_fgts", r"Base_PF_FGTS_[^/.]*", False),
    ("geavo_base_pj_fgts", r"Base_PJ_FGTS_[^/.]*", False),
    # CadÚnico (`.TXT.zip` que é gzip)
    ("cadunico_pessoa_pbf", r"ARQ_PESSOA_PBF_[^/.]*", False),
    ("cadunico_familia_pbf", r"ARQ_FAMILIA_PBF_[^/.]*", False),
)

# Tabelas de validação da Análise SNH, em outra pasta da conta.
FAMILIAS_ANALISE_SNH: tuple[tuple[str, str], ...] = (
    ("analise_snh_tab_validacao_pf", r"(\d{6}_)?tab_validacao_arquivos_pf"),
    ("analise_snh_tab_validacao_pj", r"(\d{6}_)?tab_validacao_arquivos_pj"),
)

# Arquivo sem cabeçalho: as colunas de CPF e NIS vão por posição.
POSICOES_PII = {
    r"^CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112\.TXT$": {2: "cpf", 3: "nis"},
}

# As interfaces mandam umas poucas linhas quebradas em arquivos de milhões (a
# INT039 de 2026-04-30: 2 linhas com 40 campos num layout de 37): elas saem e a
# contagem vai para o manifesto da staging, como no raw_para_staging.
CONVERSAO = ConverterConfig(encoding="auto", delimiter="auto", bad_rows="skip")


def _spec(dataset: str, nome: str, pacotes: bool, root: str = GEFUS) -> DatasetSpec:
    # Do pacote, só a família do dataset; dos outros zips, o que for tabela.
    membros = rf"(?i)^{nome}\.(csv|txt|xlsx)$" if pacotes else MEMBROS_TABULARES
    return DatasetSpec(
        domain="sftp",
        dataset=dataset,
        extractor=ExtractorConfig(
            source="sftp",
            conn_id=CONN_ID,
            remote=RemoteFiles(
                root=root,
                pattern=rf"(?i)(^|/){nome}{EXTENSOES}",
                exclude=EXCLUIR,
                prefer_extensions=PRECEDENCIA,
                bundles=PACOTES_2023 if pacotes else None,
            ),
        ),
        converter=CONVERSAO,
        load_mode=LoadMode.APPEND,
        incremental=True,
        prepare=(
            Unpack(members=membros, require_match=not pacotes),
            MaskPii(positions=POSICOES_PII),
        ),
    )


DATASETS = tuple(_spec(*familia) for familia in FAMILIAS) + tuple(
    _spec(dataset, nome, False, root=ANALISE_SNH)
    for dataset, nome in FAMILIAS_ANALISE_SNH
)


def _pipeline(spec: DatasetSpec) -> None:
    @task(task_id="extract_to_raw")
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(spec, context["dag_run"].run_after)

    @task(task_id="convert_to_staging")
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(spec, raw_prefix)

    convert_to_staging(extract_to_raw())


@dag(
    dag_id="sftp_ingest_dag",
    # Diário à 01:00: as remessas chegam ao longo do dia, e a carga (PF FGTS,
    # CadÚnico) é pesada para o horário do expediente.
    schedule="0 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Disco do worker: cada extração guarda um arquivo e a cópia mascarada.
    max_active_tasks=4,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sftp", "mcid", "cidades", "ingestion"],
)
def sftp_ingest_dag() -> None:
    # Uma família que falha não bloqueia as outras: cada grupo é independente.
    for spec in DATASETS:
        with TaskGroup(group_id=spec.dataset):
            _pipeline(spec)


dag_instance = sftp_ingest_dag()
