"""Dados históricos do MCMV: raw/dados_historicos/ -> staging -> analytics -> cleansed.

A raw é o dump congelado do banco antigo (754 CSVs `;`), subido à mão e completo no MinIO.
Ela é o Asset `dados_historicos_raw`: quem sobe cria um evento do Asset na UI (ou dispara
a DAG direto). Cada camada desta DAG é um Asset também, para quem vier depois (a bronze)
agendar por ele.

Camadas, todas sob `staging/historico_mcmv/` (`convertido/`, `analytics/`, `cleansed/`):
- **staging**: cada CSV vira Parquet, um a um, tudo em texto, sem tocar no conteúdo, menos
  o dado pessoal, que é mascarado aqui (a raw fica como foi subida);
- **analytics**: só os arquivos de conteúdo próprio (fora: vazios, o modelo de 2009 que o
  dump gravou no lugar de 273 tabelas e as cópias exatas), copiados como estão;
- **cleansed**: tratamento estrutural, uma pasta por família. Desfaz a duplicação 2x da
  exportação, arruma o cabeçalho, separa o que não é dado (com registro de cada descarte),
  confere que nada se perdeu e data cada arquivo (competência e data de corte). Depois
  padroniza a escrita dos valores (ainda texto: ponto decimal, sem `.0`, datas AAAA-MM-DD,
  sem espaço nas pontas, nulos escritos vazios, CNPJ com 14 dígitos), conferindo cada
  célula alterada; tira linha idêntica repetida nas famílias com chave; e deixa uma versão
  por conteúdo (as outras vão para `_versoes_substituidas/`, fora da pasta da família);
- **colunas**: evidência para o grafo de nomes de coluna (como cada dado muda de nome ao
  longo do tempo) e a proposta de nome canônico.

O tratamento está em `helpers/historico_mcmv/` (funções puras, sem S3). Esta DAG só lê e
grava no lake, pelo S3Hook, e encadeia as etapas.

Os mapas que dizem como tratar cada arquivo NÃO ficam no repositório: ficam no MinIO, em
`staging/historico_mcmv/config/` (`cabecalhos.csv`: cabeçalho, blocos e colunas coladas
por "|"; `familias.csv`: a família de cada saída).

Conexão: `minio_lake`, do tipo `aws`, com o `endpoint_url` do MinIO no extra, por exemplo
`AIRFLOW_CONN_MINIO_LAKE='{"conn_type": "aws", "login": "<access key>", "password":
"<secret>", "extra": {"endpoint_url": "http://10.0.0.56:9000"}}'`.

Params da staging (disparo manual):
- `forcar`: reconverte mesmo o que já está convertido (por padrão só o que mudou na raw,
  pelo etag);
- `filtro`: regex sobre o nome do arquivo; só os que casam passam pela staging (o
  analytics e o cleansed sempre refazem tudo).
"""

import io
import logging
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, get_current_context, task
from historico_mcmv import analytics, cleansed, colunas, staging, validacao
from historico_mcmv.datas import datar
from historico_mcmv.erros import ErroDadoPessoal, ErroHistorico
from historico_mcmv.layout import (
    ANALYTICS,
    BUCKET,
    CANDIDATOS,
    CLEANSED,
    CONCEITOS_ARQ,
    CONTROLE_CLEANSED,
    DESCARTES_CLEANSED,
    EVOLUCAO,
    GRAFO,
    LOG_ANALYTICS,
    LOG_CLEANSED,
    MANIFESTO,
    MAPA_CABECALHOS,
    MAPA_FAMILIAS,
    PADRONIZACOES,
    RAW,
    STAGING,
)

log = logging.getLogger(__name__)

CONN_ID = "minio_lake"
MB = 1024 * 1024

ASSET_RAW = Asset(name="dados_historicos_raw", uri=f"s3://{BUCKET}/{RAW}")
ASSET_STAGING = Asset(name="dados_historicos_staging", uri=f"s3://{BUCKET}/{STAGING}")
ASSET_ANALYTICS = Asset(name="dados_historicos_analytics", uri=f"s3://{BUCKET}/{ANALYTICS}")
ASSET_CLEANSED = Asset(name="dados_historicos_cleansed", uri=f"s3://{BUCKET}/{CLEANSED}")


# ---------------------------------------------------------------------------------------
# Lake: o S3Hook do provider amazon
# ---------------------------------------------------------------------------------------


def s3_lake() -> S3Hook:
    # o MinIO do lake falha com 10 conexões por arquivo: 2, em partes de 8 MB
    return S3Hook(
        aws_conn_id=CONN_ID,
        transfer_config_args={
            "max_concurrency": 2,
            "multipart_chunksize": 8 * MB,
            "multipart_threshold": 8 * MB,
        },
    )


def _objetos(s3: S3Hook, prefixo: str) -> List[Tuple[str, int, str]]:
    """(key, tamanho, etag) de cada objeto sob o prefixo."""
    return [
        (o["Key"], int(o["Size"]), str(o.get("ETag", "")).strip('"'))
        for o in s3.iter_file_metadata(prefix=prefixo, bucket_name=BUCKET)
    ]


def _baixar(s3: S3Hook, key: str, destino: Path) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    s3.get_conn().download_file(BUCKET, key, str(destino), Config=s3.transfer_config)
    return destino


def _subir(s3: S3Hook, local: Path, key: str) -> None:
    s3.load_file(str(local), key, bucket_name=BUCKET, replace=True)


def _ler_texto(s3: S3Hook, key: str) -> Optional[str]:
    if not s3.check_for_key(key, bucket_name=BUCKET):
        return None
    return s3.read_key(key, bucket_name=BUCKET)


def _ler_csv(s3: S3Hook, key: str) -> pd.DataFrame:
    texto = _ler_texto(s3, key)
    if texto is None:
        raise ErroHistorico(f"{key} não existe no lake")
    return pd.read_csv(io.StringIO(texto), dtype=str, keep_default_na=False)


def _apagar(s3: S3Hook, keys: List[str]) -> None:
    if keys:
        s3.delete_objects(BUCKET, keys)


# ---------------------------------------------------------------------------------------
# 1. raw -> staging
# ---------------------------------------------------------------------------------------


def _converter_um(s3: S3Hook, key: str, etag: str, size: int, colados, tmp: Path) -> dict:
    src, dst = tmp / "entrada.csv", tmp / "saida.parquet"
    destino = staging.chave_staging(key)
    linha = {"raw_key": key, "raw_etag": etag, "raw_bytes": size, "staging_key": destino}
    try:
        for p in (src, dst):
            p.unlink(missing_ok=True)
        _baixar(s3, key, src)
        info = staging.converter_csv(src, dst, source_file=key)
        mascaradas = staging.mascarar_parquet(dst, colados.get(Path(destino).stem))
        _subir(s3, dst, destino)
        linha.update(info, colunas_mascaradas="|".join(mascaradas), status="ok")
    except ErroDadoPessoal:
        raise  # dado pessoal em claro: não segue para lugar nenhum
    except Exception as exc:  # um arquivo ruim não para os outros; o resumo falha no fim
        linha.update(status="erro", erro=f"{type(exc).__name__}: {exc}"[:500])
    linha["convertido_em"] = datetime.now(timezone.utc).isoformat()
    return linha


def etapa_staging(s3: S3Hook, tmp: Path, forcar: bool = False, filtro: str = "") -> Dict[str, int]:
    """Converte a raw para a staging. `filtro` (regex sobre o nome) limita aos arquivos que
    casam; com `forcar`, reconverte esses mesmo sem mudança na raw."""
    anteriores = {
        k: v
        for k, v in staging.ler_manifesto(_ler_texto(s3, MANIFESTO)).items()
        if Path(k).name not in staging.RAW_FORA
    }
    existentes = {k for k, _, _ in _objetos(s3, STAGING)}
    colados = staging.campos_colados(
        validacao.validar_mapa_cabecalhos(_ler_csv(s3, MAPA_CABECALHOS))
    )
    padrao = re.compile(filtro) if filtro else None
    objetos = [
        o
        for o in _objetos(s3, RAW)
        if o[0].lower().endswith(".csv")
        and Path(o[0]).name not in staging.RAW_FORA
        and (not padrao or padrao.search(Path(o[0]).name))
    ]
    resumo = {"ok": 0, "pulado": 0, "erro": 0}
    for i, (key, size, etag) in enumerate(objetos, 1):
        if not forcar and staging.ja_convertido(
            anteriores.get(key), etag, staging.chave_staging(key) in existentes
        ):
            resumo["pulado"] += 1
            continue
        linha = _converter_um(s3, key, etag, size, colados, tmp)
        anteriores[key] = linha
        resumo[linha["status"]] += 1
        if linha["status"] == "erro":
            log.error("%s: %s", key, linha["erro"])
        if i % 50 == 0:
            log.info("[%d/%d] staging", i, len(objetos))
    staging.escrever_manifesto(anteriores.values(), tmp / "_manifesto.csv")
    validacao.validar_manifesto(
        pd.read_csv(tmp / "_manifesto.csv", dtype=str, keep_default_na=False)
    )
    _subir(s3, tmp / "_manifesto.csv", MANIFESTO)
    log.info("staging: %s", resumo)
    if resumo["erro"]:
        raise ErroHistorico(f"{resumo['erro']} arquivo(s) com erro na staging (ver o manifesto)")
    return resumo


def conferir_raw_contra_manifesto(s3: S3Hook) -> Dict[str, int]:
    """A raw é subida à mão: falha se sumiu algum arquivo que já foi convertido (upload
    pela metade). Arquivo novo ou alterado só aparece no resumo."""
    raw = {
        k: e
        for k, _, e in _objetos(s3, RAW)
        if k.lower().endswith(".csv") and Path(k).name not in staging.RAW_FORA
    }
    if not raw:
        raise ErroHistorico(f"{RAW} está vazia")
    anteriores = staging.ler_manifesto(_ler_texto(s3, MANIFESTO))
    faltando = sorted(k for k in anteriores if k not in raw)
    if faltando:
        raise ErroHistorico(
            f"{len(faltando)} arquivo(s) do manifesto sumiram da raw: {faltando[:5]}"
        )
    alterados = sum(1 for k, e in raw.items() if k in anteriores and anteriores[k]["raw_etag"] != e)
    resumo = {
        "arquivos": len(raw),
        "novos": len(set(raw) - set(anteriores)),
        "alterados": alterados,
    }
    log.info("raw: %s", resumo)
    return resumo


# ---------------------------------------------------------------------------------------
# 2. staging -> analytics
# ---------------------------------------------------------------------------------------


def etapa_analytics(s3: S3Hook, tmp: Path) -> Dict[str, int]:
    import duckdb

    con = duckdb.connect()
    gerado_em = datetime.now(timezone.utc).isoformat()
    registros = []
    # a raw é congelada: arquivo da staging com o mesmo etag reaproveita as métricas do log
    anteriores = {}
    if any(k == LOG_ANALYTICS for k, _, _ in _objetos(s3, ANALYTICS)):
        log_ant = pq.read_table(_baixar(s3, LOG_ANALYTICS, tmp / "analytics_log_anterior.parquet"))
        anteriores = {r["staging_key"]: r for r in log_ant.to_pylist() if r.get("staging_etag")}
    objetos = [o for o in _objetos(s3, STAGING) if o[0].endswith(".parquet")]
    reaproveitados = 0
    for i, (key, _, etag) in enumerate(objetos, 1):
        nome = Path(key).stem
        anterior = anteriores.get(key)
        if anterior and anterior["staging_etag"] == etag:
            metricas = {c: anterior[c] for c in analytics.METRICAS_ANALYTICS}
            reaproveitados += 1
        else:
            local = _baixar(s3, key, tmp / f"{nome}.parquet")
            metricas = analytics.metricas_parquet(local, con)
            local.unlink()
        registros.append(
            {
                "arquivo": nome,
                "staging_key": key,
                "staging_etag": etag,
                "periodo": analytics.periodo(nome),
                "gerado_em": gerado_em,
                **metricas,
            }
        )
        if i % 100 == 0:
            log.info("[%d/%d] métricas", i, len(objetos))
    registros = analytics.classificar(registros)
    for r in registros:
        r["analytics_key"] = (
            f"{ANALYTICS}{r['arquivo']}.parquet" if r["status"] == analytics.SELECIONADO else None
        )
    selecionados = {r["analytics_key"] for r in registros if r["analytics_key"]}
    for r in registros:
        if r["analytics_key"]:
            s3.copy_object(
                r["staging_key"],
                r["analytics_key"],
                source_bucket_name=BUCKET,
                dest_bucket_name=BUCKET,
            )
    sobra = [
        k for k, _, _ in _objetos(s3, ANALYTICS)
        if k.endswith(".parquet") and k != LOG_ANALYTICS and k not in selecionados
    ]  # fmt: skip
    _apagar(s3, sobra)
    schema = analytics.SCHEMA_ANALYTICS
    linhas = [{c: r.get(c) for c in schema.names} for r in registros]
    pq.write_table(pa.Table.from_pylist(linhas, schema=schema), tmp / "log.parquet")
    validacao.validar_log_analytics(pd.read_parquet(tmp / "log.parquet"))
    _subir(s3, tmp / "log.parquet", LOG_ANALYTICS)
    resumo: Dict[str, int] = {}
    for r in registros:
        resumo[r["status"]] = resumo.get(r["status"], 0) + 1
    log.info(
        "analytics: %s (%d removido(s) da seleção anterior; %d métricas reaproveitadas)",
        resumo,
        len(sobra),
        reaproveitados,
    )
    return resumo


# ---------------------------------------------------------------------------------------
# 3. analytics -> cleansed
# ---------------------------------------------------------------------------------------


def _tratar_todos(
    s3: S3Hook, registros: List[dict], tmp: Path
) -> Tuple[List[pd.DataFrame], List[dict]]:
    """Trata cada arquivo em `tmp/saida/`. Tenta todos; se algum falhar, erro no fim."""
    por_arquivo: Dict[str, List[dict]] = {}
    for r in registros:
        if r["status"] == cleansed.TRATADO:
            por_arquivo.setdefault(r["arquivo"], []).append(r)
    familias = {r["saida"]: r["familia"] for r in registros}
    chaves = {r["saida"]: list(r["chave_familia"] or []) for r in registros}
    descartes, padronizacoes, erros = [], [], []
    for i, (arquivo, regs) in enumerate(por_arquivo.items(), 1):
        local = _baixar(s3, regs[0]["analytics_key"], tmp / "entrada" / f"{arquivo}.parquet")
        try:
            campos, desc, pads = cleansed.tratar_parquet(
                local, cleansed.regras_do_registro(regs), tmp / "saida", familias, chaves
            )
        except ErroHistorico as exc:
            log.error("[%d/%d] %s: %s", i, len(por_arquivo), arquivo, exc)
            erros.append(arquivo)
            continue
        finally:
            local.unlink()
        for r in regs:
            r.update(campos[r["bloco"] or ""])
            r["cleansed_key"] = f"{CLEANSED}{r['familia']}/{r['saida']}.parquet"
        descartes.append(desc)
        padronizacoes += pads
        if i % 50 == 0:
            log.info("[%d/%d] tratados", i, len(por_arquivo))
    if erros:
        raise ErroHistorico(f"{len(erros)} arquivo(s) falharam no tratamento: {erros[:5]}")
    return descartes, padronizacoes


def etapa_cleansed(s3: S3Hook, tmp: Path) -> Dict[str, object]:
    """Tudo ou nada: só publica se todos os arquivos passaram pela conferência."""
    _baixar(s3, LOG_ANALYTICS, tmp / "analytics_log.parquet")
    log_analytics = pq.read_table(tmp / "analytics_log.parquet").to_pylist()
    selecionados = [s for s in log_analytics if s["status"] == analytics.SELECIONADO]
    mapa = cleansed.ler_mapa(validacao.validar_mapa_cabecalhos(_ler_csv(s3, MAPA_CABECALHOS)))
    familias = cleansed.ler_familias(validacao.validar_mapa_familias(_ler_csv(s3, MAPA_FAMILIAS)))
    registros = cleansed.planejar(selecionados, mapa, familias)
    gerado_em = datetime.now(timezone.utc).isoformat()
    for r in registros:
        r["gerado_em"] = gerado_em
    descartes, padronizacoes = _tratar_todos(s3, registros, tmp)
    datar(registros)
    cleansed.escolher_versoes(registros)
    publicadas = set()
    for r in registros:
        if r["status"] in (cleansed.TRATADO, cleansed.SUBSTITUIDA):
            _subir(s3, tmp / "saida" / r["familia"] / f"{r['saida']}.parquet", r["cleansed_key"])
            publicadas.add(r["cleansed_key"])
    # o controle fica; a saída que não é desta execução sai
    sobra = [
        k for k, _, _ in _objetos(s3, CLEANSED)
        if k.endswith(".parquet") and k not in publicadas and k not in CONTROLE_CLEANSED
    ]  # fmt: skip
    _apagar(s3, sobra)
    cleansed.escrever_log_cleansed(registros, tmp / "log.parquet")
    validacao.validar_log_cleansed(pd.read_parquet(tmp / "log.parquet"))
    _subir(s3, tmp / "log.parquet", LOG_CLEANSED)
    cleansed.escrever_descartes(descartes, tmp / "descartes.parquet")
    validacao.validar_descartes(pd.read_parquet(tmp / "descartes.parquet"))
    _subir(s3, tmp / "descartes.parquet", DESCARTES_CLEANSED)
    tabela = pa.Table.from_pylist(padronizacoes, schema=cleansed.SCHEMA_PADRONIZACOES)
    pq.write_table(tabela, tmp / "padronizacoes.parquet")
    _subir(s3, tmp / "padronizacoes.parquet", PADRONIZACOES)
    resumo = cleansed.resumo_cleansed(registros)
    log.info("cleansed: %s (%d removido(s) da execução anterior)", resumo, len(sobra))
    return resumo


# ---------------------------------------------------------------------------------------
# 4. colunas
# ---------------------------------------------------------------------------------------


def etapa_colunas(s3: S3Hook, tmp: Path) -> Dict[str, int]:
    _baixar(s3, LOG_CLEANSED, tmp / "log.parquet")
    log_cleansed = pd.read_parquet(tmp / "log.parquet")
    evolucao = colunas.evolucao_colunas(log_cleansed)
    trocas = colunas.trocas_de_layout(log_cleansed)
    baixados: Dict[str, Path] = {}

    def ler_coluna(reg, coluna) -> pd.Series:
        if reg.saida not in baixados:
            baixados[reg.saida] = _baixar(
                s3, reg.cleansed_key, tmp / "arquivos" / f"{reg.saida}.parquet"
            )
        return pq.read_table(baixados[reg.saida], columns=[coluna]).column(0).to_pandas()

    candidatos = colunas.candidatos_renomeacao(trocas, ler_coluna)
    conceitos_df = colunas.conceitos_colunas(evolucao, candidatos)
    for nome, df, key in (
        ("evolucao", evolucao, EVOLUCAO),
        ("candidatos", candidatos, CANDIDATOS),
        ("conceitos", conceitos_df, CONCEITOS_ARQ),
    ):
        df.to_parquet(tmp / f"{nome}.parquet", index=False)
        _subir(s3, tmp / f"{nome}.parquet", key)
    (tmp / "grafo.md").write_text(colunas.grafo_mermaid(conceitos_df), encoding="utf-8")
    _subir(s3, tmp / "grafo.md", GRAFO)
    resumo = {
        "familias": int(evolucao.familia.nunique()),
        "colunas": len(evolucao),
        "trocas_de_layout": len(trocas),
        "pares_provaveis": int(candidatos.provavel.sum()) if len(candidatos) else 0,
        "nomes_canonicos": int(conceitos_df.nome_canonico.nunique()),
    }
    log.info("colunas: %s", resumo)
    return resumo


# ---------------------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------------------


@dag(
    dag_id="dados_historicos_dag",
    schedule=[ASSET_RAW],
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"forcar": False, "filtro": ""},
    default_args={"owner": "Lucas", "retries": 0, "execution_timeout": timedelta(hours=3)},
    tags=["dados_historicos", "mcmv", "lake"],
    doc_md=__doc__,
)
def dados_historicos_dag() -> None:
    @task
    def conferir_raw() -> dict:
        """Inventário da raw contra o manifesto da staging."""
        return conferir_raw_contra_manifesto(s3_lake())

    @task(outlets=[ASSET_STAGING])
    def raw_para_staging() -> dict:
        params = get_current_context()["params"]
        forcar, filtro = bool(params.get("forcar", False)), str(params.get("filtro") or "")
        with tempfile.TemporaryDirectory(prefix="historico-staging-") as tmp:
            return etapa_staging(s3_lake(), Path(tmp), forcar, filtro)

    @task(outlets=[ASSET_ANALYTICS])
    def staging_para_analytics() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-analytics-") as tmp:
            return etapa_analytics(s3_lake(), Path(tmp))

    @task(outlets=[ASSET_CLEANSED])
    def analytics_para_cleansed() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-cleansed-") as tmp:
            return etapa_cleansed(s3_lake(), Path(tmp))

    @task
    def mapear_colunas() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-colunas-") as tmp:
            return etapa_colunas(s3_lake(), Path(tmp))

    (
        conferir_raw()
        >> raw_para_staging()
        >> staging_para_analytics()
        >> analytics_para_cleansed()
        >> mapear_colunas()
    )


dag_instance = dados_historicos_dag()
