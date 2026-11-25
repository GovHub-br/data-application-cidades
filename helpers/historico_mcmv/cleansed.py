"""analytics -> cleansed: plano por arquivo, famílias, log, descartes e escolha de versão."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from historico_mcmv.datas import evidencias
from historico_mcmv.erros import ErroHistorico
from historico_mcmv.estrutura import (
    COMPOR,
    DIVIDIR,
    EXCLUIR,
    LINHA_ORIGEM,
    MANTER,
    NOMEAR,
    PROMOVER,
    Regra,
    tratar_blocos,
)
from historico_mcmv.layout import SUBSTITUIDAS
from historico_mcmv.padronizacao import hash_conteudo, padronizar, remover_duplicadas
from historico_mcmv.validacao import validar_saida

log = logging.getLogger(__name__)


LIMITE_UNNAMED = 0.5  # a partir daqui, arquivo fora do mapa está com o cabeçalho errado
TRATADO, EXCLUIDO, SUBSTITUIDA = "tratado", "excluido_formatado", "versao_substituida"
CONTEUDO_IDENTICO, VERSAO_CORRIGIDA = "conteudo_identico", "versao_corrigida"
SCHEMA_CLEANSED = pa.schema(
    [
        ("arquivo", pa.string()),
        ("bloco", pa.string()),
        ("saida", pa.string()),
        ("familia", pa.string()),
        ("chave_familia", pa.list_(pa.string())),
        ("analytics_key", pa.string()),
        ("cleansed_key", pa.string()),
        ("status", pa.string()),
        ("periodo", pa.string()),
        ("competencia", pa.string()),
        ("competencia_fonte", pa.string()),
        ("data_corte", pa.date32()),
        ("data_corte_fonte", pa.string()),
        ("alertas_data", pa.list_(pa.string())),
        ("data_nome", pa.date32()),
        ("data_nome_precisao", pa.string()),
        ("lote", pa.string()),
        ("contrato_coluna", pa.string()),
        ("contrato_mais_novo", pa.date32()),
        ("data_declarada_coluna", pa.string()),
        ("data_declarada", pa.string()),
        ("cabecalho_acao", pa.string()),
        ("cabecalho_linha", pa.int64()),
        ("bloco_fim", pa.int64()),
        ("cabecalho_colunas", pa.list_(pa.string())),
        ("cabecalho_motivo", pa.string()),
        ("descartar_zeradas", pa.bool_()),
        ("linhas_total", pa.list_(pa.int64())),
        ("linhas_entrada", pa.int64()),
        ("linhas_reais", pa.int64()),
        ("linhas_descartadas", pa.int64()),
        ("descartes_por_motivo", pa.map_(pa.string(), pa.int64())),
        ("colunas_entrada", pa.list_(pa.string())),
        ("colunas_removidas", pa.list_(pa.string())),
        ("colunas_vazias", pa.list_(pa.string())),
        ("n_colunas", pa.int64()),
        ("colunas", pa.list_(pa.string())),
        ("padronizacoes", pa.map_(pa.string(), pa.int64())),  # células alteradas por regra
        ("hash_conteudo", pa.string()),  # do conteúdo padronizado
        ("substituida_por", pa.string()),
        ("motivo_substituicao", pa.string()),
        ("gerado_em", pa.string()),
    ]
)
SCHEMA_PADRONIZACOES = pa.schema(
    [
        ("saida", pa.string()),
        ("coluna", pa.string()),
        ("regra", pa.string()),
        ("celulas", pa.int64()),
        ("antes", pa.string()),  # um exemplo
        ("depois", pa.string()),
    ]
)
SCHEMA_DESCARTES = pa.schema(
    [
        ("arquivo", pa.string()),
        ("bloco", pa.string()),
        (LINHA_ORIGEM, pa.int64()),
        ("motivo", pa.string()),
        ("valores", pa.list_(pa.string())),
    ]
)


def ler_mapa(mapa: pd.DataFrame) -> Dict[str, List[Regra]]:
    """`cabecalhos.csv`: arquivo, bloco, acao, linha, fim, colunas ("|"), motivo,
    linhas_zeradas ("descartar" descarta as linhas-modelo zeradas do arquivo) e linhas_total
    (`_linha_origem` de totais sem rótulo, "|"; a linha sai como total). A ação vazia mantém
    o cabeçalho (serve para pedir só esses descartes)."""
    regras: Dict[str, List[Regra]] = {}
    for r in mapa.itertuples():
        if r.acao not in (PROMOVER, COMPOR, NOMEAR, DIVIDIR, EXCLUIR, MANTER):
            raise ErroHistorico(f"mapa de cabeçalhos: ação inválida {r.acao!r} para {r.arquivo}")
        colunas = r.colunas.split("|") if r.colunas else []
        if r.acao in (NOMEAR, DIVIDIR) and not colunas:
            raise ErroHistorico(f"mapa de cabeçalhos: {r.acao} sem colunas para {r.arquivo}")
        regra = Regra(
            r.acao,
            int(r.linha) if r.linha else 0,
            r.motivo,
            r.bloco,
            int(r.fim) if r.fim else None,
            colunas,
            getattr(r, "linhas_zeradas", "") == "descartar",
            [int(n) for n in str(getattr(r, "linhas_total", "") or "").split("|") if n],
        )
        regras.setdefault(r.arquivo, []).append(regra)
    for arquivo, rs in regras.items():
        blocos = [r.bloco for r in rs]
        if len(rs) > 1 and (not all(blocos) or len(set(blocos)) != len(blocos)):
            raise ErroHistorico(f"mapa de cabeçalhos: {arquivo} tem blocos sem nome ou repetidos")
    return regras


def ler_familias(familias: pd.DataFrame) -> List[Tuple[str, re.Pattern, List[str]]]:
    """`familias.csv`: familia, padrao (regex sobre a saída), chave (colunas que identificam a
    linha, separadas por "|"; vazia quando a família não tem chave), fonte, descricao."""
    return [
        (r.familia, re.compile(r.padrao), [c for c in getattr(r, "chave", "").split("|") if c])
        for r in familias.itertuples()
    ]


def chaves_da_familia(familia: Optional[str], familias) -> List[str]:
    return next((chaves for nome, _, chaves in familias if nome == familia), [])


def familia_de(saida: str, familias) -> Optional[str]:
    """A família da saída; erro se casar com mais de uma; None se com nenhuma."""
    achadas = [nome for nome, padrao, _ in familias if padrao.search(saida)]
    if len(achadas) > 1:
        raise ErroHistorico(f"{saida} casa com mais de uma família: {achadas}")
    return achadas[0] if achadas else None


def nome_saida(arquivo: str, bloco: str) -> str:
    return f"{arquivo}__{bloco}" if bloco else arquivo


def _fracao_unnamed(colunas: List[str]) -> float:
    return sum(c.startswith("unnamed") for c in colunas) / max(len(colunas), 1)


def _registro(s: dict, regra: Regra, familias) -> dict:
    saida = nome_saida(s["arquivo"], regra.bloco)
    familia = familia_de(saida, familias)
    return {
        "arquivo": s["arquivo"],
        "bloco": regra.bloco or None,
        "saida": saida,
        "familia": familia,
        "chave_familia": chaves_da_familia(familia, familias) or None,
        "descartar_zeradas": regra.zeradas,
        "linhas_total": regra.totais or None,
        "analytics_key": s["analytics_key"],
        "periodo": s.get("periodo"),
        "colunas_entrada": list(s["colunas"]),
        "status": EXCLUIDO if regra.acao == EXCLUIR else TRATADO,
        "cabecalho_acao": regra.acao,
        "cabecalho_linha": regra.linha if regra.acao in (PROMOVER, COMPOR) else None,
        "cabecalho_colunas": regra.colunas or None,
        "bloco_fim": regra.fim,
        "cabecalho_motivo": regra.motivo or None,
    }


def planejar(selecionados: List[dict], mapa: Dict[str, List[Regra]], familias) -> List[dict]:
    """Um registro por saída (arquivo, ou arquivo + bloco). Falha se um arquivo fora do
    mapa tem o cabeçalho quase todo `unnamed_*`, ou se uma saída tratada não tem família."""
    regs, sem_regra = [], []
    for s in selecionados:
        regras = mapa.get(s["arquivo"], [Regra(MANTER)])
        regs += [_registro(s, regra, familias) for regra in regras]
        if regras[0].acao == MANTER and _fracao_unnamed(list(s["colunas"])) >= LIMITE_UNNAMED:
            sem_regra.append(s["arquivo"])
    if sem_regra:
        raise ErroHistorico(
            f"{len(sem_regra)} arquivo(s) com cabeçalho quase todo unnamed_* e fora do mapa "
            f"de cabeçalhos: {sem_regra[:5]}"
        )
    sem_familia = [r["saida"] for r in regs if r["status"] == TRATADO and not r["familia"]]
    if sem_familia:
        raise ErroHistorico(f"{len(sem_familia)} saída(s) sem família: {sem_familia[:5]}")
    for arquivo in sorted(set(mapa) - {s["arquivo"] for s in selecionados}):
        log.warning("mapa de cabeçalhos: %s não está no analytics", arquivo)
    return regs


def _gravar_saida(dados: pd.DataFrame, linhagem: dict, local: Path) -> None:
    saida = pa.Table.from_pandas(dados, preserve_index=False)
    for c, v in linhagem.items():
        saida = saida.append_column(c, pa.array([v] * saida.num_rows, pa.string()))
    local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(saida, local, compression="snappy")


def _somar_regras(padronizacoes: List[dict]) -> Dict[str, int]:
    total: Dict[str, int] = {}
    for p in padronizacoes:
        total[p["regra"]] = total.get(p["regra"], 0) + p["celulas"]
    return total


def tratar_parquet(
    path: Path,
    regras: List[Regra],
    destino: Optional[Path],
    familias: Dict[str, str],
    chaves: Optional[Dict[str, List[str]]] = None,
) -> Tuple[Dict[str, dict], pd.DataFrame, List[dict]]:
    """Trata um Parquet do analytics: estrutura, padronização e, em família com chave,
    linhas duplicadas. Grava cada bloco em `destino/<familia>/<saida>.parquet` (se dado).
    Devolve ({bloco: campos do log}, descartes, padronizações por coluna)."""
    tabela = pq.read_table(path)
    linhagem = {c: tabela.column(c)[0].as_py() for c in ("_source_file", "_source_hash")}
    dados = tabela.drop([c for c in tabela.column_names if c.startswith("_")]).to_pandas()
    campos, descartes, padronizacoes = {}, [], []
    for bloco, res in tratar_blocos(dados, regras):
        saida_nome = nome_saida(path.stem, bloco)
        ev = evidencias(res.dados)  # a data do arquivo sai do valor original
        padronizado, pads = padronizar(res.dados)
        desc = res.descartes
        if any(c in padronizado.columns for c in (chaves or {}).get(saida_nome, [])):
            padronizado, duplicadas = remover_duplicadas(padronizado)
            desc = pd.concat([desc, duplicadas]).sort_values(LINHA_ORIGEM).reset_index(drop=True)
        validar_saida(padronizado, saida_nome)
        if destino:
            _gravar_saida(
                padronizado, linhagem, destino / familias[saida_nome] / f"{saida_nome}.parquet"
            )
        colunas = [c for c in padronizado.columns if c != LINHA_ORIGEM]
        por_motivo = desc.motivo.value_counts().to_dict() if len(desc) else {}
        campos[bloco] = {
            "linhas_entrada": res.linhas_entrada,
            "linhas_reais": len(padronizado),
            "linhas_descartadas": len(desc),
            "descartes_por_motivo": por_motivo,
            "colunas_removidas": res.colunas_removidas,
            "colunas_vazias": res.colunas_vazias,
            "n_colunas": len(colunas),
            "colunas": colunas,
            "padronizacoes": _somar_regras(pads),
            "hash_conteudo": hash_conteudo(padronizado),
            **ev,
        }
        descartes.append(desc.assign(arquivo=path.stem, bloco=bloco or None))
        padronizacoes += [dict(saida=saida_nome, **p) for p in pads]
    return campos, pd.concat(descartes, ignore_index=True), padronizacoes


def escrever_log_cleansed(registros: List[dict], path: Path) -> None:
    linhas = [{c: r.get(c) for c in SCHEMA_CLEANSED.names} for r in registros]
    for linha in linhas:
        for campo in ("descartes_por_motivo", "padronizacoes"):
            if linha[campo] is not None:
                linha[campo] = [(k, int(v)) for k, v in linha[campo].items()]
    pq.write_table(pa.Table.from_pylist(linhas, schema=SCHEMA_CLEANSED), path)


def escrever_descartes(descartes: List[pd.DataFrame], path: Path) -> None:
    if descartes:
        df = pd.concat(descartes, ignore_index=True)[SCHEMA_DESCARTES.names]
        tabela = pa.Table.from_pandas(df, schema=SCHEMA_DESCARTES, preserve_index=False)
    else:
        tabela = SCHEMA_DESCARTES.empty_table()
    pq.write_table(tabela, path)


def regras_do_registro(regs: List[dict]) -> List[Regra]:
    return [
        Regra(
            r["cabecalho_acao"],
            r["cabecalho_linha"] or 0,
            bloco=r["bloco"] or "",
            fim=r["bloco_fim"],
            colunas=list(r["cabecalho_colunas"] or []),
            zeradas=bool(r.get("descartar_zeradas")),
            totais=list(r.get("linhas_total") or []),
        )
        for r in regs
    ]


def rank_versao(saida: str) -> int:
    """Quanto maior, mais corrigida a versão: correção/ajuste > v3 > v2 > original > antigo
    ou a 2ª versão com a acentuação quebrada (`min_cidades_pj_AAAA_MM`)."""
    if re.search(r"correcao|ajustada", saida):
        return 30
    m = re.search(r"(?:^|_|\d)vs?0?(\d)(?:$|_)", saida)
    if m:
        return 10 + int(m.group(1))
    if re.search(r"_antigo$|min_cidades_pj_\d{4}_\d{2}$", saida):
        return 0
    return 10


def _substituir(r: dict, por: dict, motivo: str) -> None:
    r.update(status=SUBSTITUIDA, substituida_por=por["saida"], motivo_substituicao=motivo)
    r["cleansed_key"] = f"{SUBSTITUIDAS}{r['familia']}/{r['saida']}.parquet"


def escolher_versoes(registros: List[dict]) -> int:
    """Uma saída por conteúdo e por versão, dentro da família. Conteúdo idêntico depois da
    padronização: fica o de menor competência (e, no mesmo mês, a versão mais corrigida).
    Mesmo mês, mesmo corte e mesmas colunas com conteúdo diferente: fica a versão mais
    corrigida, se o nome disser qual é. As outras vão para `_versoes_substituidas/`."""
    tratados = [r for r in registros if r["status"] == TRATADO]
    ordem = lambda r: (r["competencia"] or "9999", -rank_versao(r["saida"]), r["saida"])  # noqa: E731
    grupos: Dict[tuple, List[dict]] = {}
    for r in tratados:
        grupos.setdefault((r["familia"], r["hash_conteudo"]), []).append(r)
    for g in grupos.values():
        fica, *saem = sorted(g, key=ordem)
        for r in saem:
            _substituir(r, fica, CONTEUDO_IDENTICO)
    grupos = {}
    for r in (r for r in tratados if r["status"] == TRATADO):
        chave = (r["familia"], r["competencia"], str(r["data_corte"]), tuple(sorted(r["colunas"])))
        grupos.setdefault(chave, []).append(r)
    for g in (g for g in grupos.values() if len(g) > 1):
        g = sorted(g, key=lambda r: (-rank_versao(r["saida"]), r["saida"]))
        if rank_versao(g[0]["saida"]) == rank_versao(g[1]["saida"]):
            log.warning("versões sem critério de escolha, ficam todas: %s", [r["saida"] for r in g])
            continue
        for r in g[1:]:
            _substituir(r, g[0], VERSAO_CORRIGIDA)
    return sum(r["status"] == SUBSTITUIDA for r in registros)


def resumo_cleansed(registros: List[dict]) -> Dict[str, object]:
    tratados = [r for r in registros if r["status"] == TRATADO]
    motivos: Dict[str, int] = {}
    alertas: Dict[str, int] = {}
    regras: Dict[str, int] = {}
    for r in tratados:
        for k, v in r["descartes_por_motivo"].items():
            motivos[k] = motivos.get(k, 0) + v
        for k, v in r["padronizacoes"].items():
            regras[k] = regras.get(k, 0) + v
    for r in registros:
        for a in r["alertas_data"]:
            alertas[a] = alertas.get(a, 0) + 1
    return {
        "saidas": len(tratados),
        "familias": len({r["familia"] for r in tratados}),
        "linhas_reais": sum(r["linhas_reais"] for r in tratados),
        "descartes": motivos,
        "padronizacoes": dict(sorted(regras.items())),
        "versoes_substituidas": sum(r["status"] == SUBSTITUIDA for r in registros),
        "com_competencia": sum(r["competencia"] is not None for r in registros),
        "com_data_corte": sum(r["data_corte"] is not None for r in registros),
        "alertas_data": dict(sorted(alertas.items())),
    }
