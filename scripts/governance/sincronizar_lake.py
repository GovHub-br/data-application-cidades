#!/usr/bin/env python3
"""Publica o data lake no OpenMetadata e liga o parquet à camada Bronze.

Sem isto o grafo de linhagem começa na Bronze e o parquet aparece do nada: o
consumidor vê o número, mas não vê de onde ele veio. O caminho já está
declarado no repo há tempo — `meta.caminho` em `sources.yml`, resolvido pela
macro `fonte_lake` —, só nunca tinha sido publicado.

O que é publicado: existência e topologia — bucket, camada, prefixo, arquivo,
formato e a aresta arquivo -> tabela Bronze. Nunca conteúdo de objeto,
contagem de linha ou amostra. O serviço é registrado SEM credencial: o lake
entra para catálogo e linhagem, não para ingestão nativa.

De onde sai cada coisa:

    governance/servicos.yml   semântica do serviço, do bucket e dos prefixos
    models/*/sources.yml      bucket, caminho e descrição de cada arquivo
    models/*/bronze/*.sql     qual Bronze lê qual arquivo, pela chamada a
                              `fonte_lake('<fonte>')` — é declaração, não SQL
                              compilado, e por isso pode ser lida aqui

Uso:
    poetry run python scripts/governance/sincronizar_lake.py
    poetry run python scripts/governance/sincronizar_lake.py --confirmar
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field

import yaml

from governanca_comum import (
    RAIZ,
    ClienteOM,
    Proprietarios,
    ambiente,
    carregar,
    etiqueta,
    operacoes_de_diferenca,
    referencia,
    texto,
)

MODELOS = RAIZ / "dbt" / "mcid" / "models"
CATALOGO = RAIZ / "docs-conjuntura" / "openmetadata_semantic_catalog.json"
#: A macro registra a dependência do model bronze na fonte do lake. Ler a
#: chamada é ler a declaração — não há SQL compilado nem caminho de dado aqui.
CHAMADA_FONTE = re.compile(r"fonte_lake\(\s*['\"]([^'\"]+)['\"]")


@dataclass
class Arquivo:
    """Um objeto do lake e a tabela Bronze que o lê."""

    fonte: str
    caminho: str
    bucket: str
    descricao: str
    dag: str | None = None
    bronze: list[str] = field(default_factory=list)

    @property
    def partes(self) -> list[str]:
        return self.caminho.split("/")


def carregar_arquivos() -> list[Arquivo]:
    """Lê as fontes do lake declaradas nos `sources.yml` do projeto."""
    arquivos: dict[str, Arquivo] = {}
    for caminho_yml in sorted(MODELOS.rglob("sources.yml")):
        documento = yaml.safe_load(caminho_yml.read_text(encoding="utf-8")) or {}
        for fonte in documento.get("sources") or []:
            bucket = (fonte.get("meta") or {}).get("bucket")
            if not bucket:
                continue
            for tabela in fonte.get("tables") or []:
                caminho = (tabela.get("meta") or {}).get("caminho")
                if not caminho:
                    continue
                arquivos[tabela["name"]] = Arquivo(
                    fonte=tabela["name"],
                    caminho=caminho,
                    bucket=bucket,
                    descricao=texto(tabela.get("description")),
                    dag=(tabela.get("meta") or {}).get("dag"),
                )
    for sql in sorted(MODELOS.rglob("bronze/*.sql")):
        for nome in CHAMADA_FONTE.findall(sql.read_text(encoding="utf-8")):
            if nome in arquivos:
                arquivos[nome].bronze.append(sql.stem)
    return list(arquivos.values())


def arvore(arquivos: list[Arquivo], declarado: dict) -> list[dict]:
    """Monta os containers do raiz para a folha.

    A ordem importa: o OpenMetadata exige que o pai exista antes do filho.
    """
    prefixos = declarado.get("prefixos") or {}
    camadas = declarado.get("camadas") or {}
    buckets = declarado.get("buckets") or {}
    nos: dict[tuple[str, ...], dict] = {}

    for arquivo in arquivos:
        trilha: tuple[str, ...] = (arquivo.bucket,)
        nos.setdefault(
            trilha,
            {
                "chave": trilha,
                "name": arquivo.bucket,
                "description": texto(
                    (buckets.get(arquivo.bucket) or {}).get("description")
                ),
                "folha": False,
            },
        )
        for indice, parte in enumerate(arquivo.partes):
            trilha = trilha + (parte,)
            folha = indice == len(arquivo.partes) - 1
            if folha:
                descricao = arquivo.descricao
            elif indice == 0:
                descricao = texto((camadas.get(parte) or {}).get("description"))
            else:
                descricao = texto(prefixos.get(parte, ""))
            nos.setdefault(
                trilha,
                {
                    "chave": trilha,
                    "name": parte,
                    "description": descricao,
                    "folha": folha,
                    "prefix": "/" + "/".join(trilha[1:]),
                    "arquivo": arquivo if folha else None,
                },
            )
    # do mais raso ao mais profundo
    return [nos[chave] for chave in sorted(nos, key=lambda c: (len(c), c))]


def fqn_do_container(servico: str, chave: tuple[str, ...]) -> str:
    """Monta o FQN citando o segmento que contém ponto.

    O OpenMetadata usa ponto como separador de FQN e cita o segmento que tenha
    um: `...staging.infomoney."acoes_imob.parquet"`. Montando sem as aspas, a
    busca por FQN nunca encontra as 33 folhas e o sync as recria a cada
    execução — sem duplicar, porque o PUT é upsert, mas sem nunca convergir.
    """
    return ".".join([_segmento(parte) for parte in (servico, *chave)])


def _segmento(nome: str) -> str:
    return f'"{nome}"' if "." in nome else nome


def schema_por_modelo() -> dict[str, str]:
    """Em que schema cada model foi materializado, lido do catálogo seguro."""
    if not CATALOGO.exists():
        return {}
    catalogo = json.loads(CATALOGO.read_text(encoding="utf-8"))
    return {m["name"]: m["schema"] for m in catalogo.get("models", [])}


def sincronizar_servico(om: ClienteOM, donos: Proprietarios, declarado: dict) -> None:
    """Registra o serviço de armazenamento, sem credencial."""
    print("Serviço de armazenamento")
    nome = declarado["name"]
    atual = om.existe("services/storageServices", nome, campos="owners,domains,tags")
    desejado: dict = {"description": texto(declarado.get("description"))}
    dono = donos.resolver(declarado.get("owner_key"))
    if dono:
        desejado["owners"] = dono
    if declarado.get("domain"):
        dominio = om.existe("domains", declarado["domain"])
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
    if declarado.get("tags"):
        desejado["tags"] = [etiqueta(t) for t in declarado["tags"]]
    if not atual:
        # Criação MÍNIMA. Esta instância recusa `owners`, `domains` e `tags` no
        # payload de criação — devolve 400 "Invalid request format", sem dizer
        # qual campo. O `MEMORY.md` já registrava o mesmo para tabelas. A
        # governança entra logo abaixo, por patch.
        #
        # E sem `connection` de propósito: o lake entra para catálogo e
        # linhagem, não para ingestão nativa. Credencial de MinIO não entra em
        # instância compartilhada.
        criado = om.criar(
            "services/storageServices",
            {
                "name": nome,
                "serviceType": declarado.get("service_type", "S3"),
                "description": desejado["description"],
            },
            nome,
        )
        if not criado:
            return
        atual = criado
    om.patch(
        "services/storageServices",
        atual["id"],
        operacoes_de_diferenca(atual, desejado),
        nome,
    )


#: Identificador usado no lugar do id real quando nada é escrito. Existe para
#: que a simulação percorra a árvore inteira: sem ele, o pai "não criado" faz
#: todos os 40 filhos virarem falha e o plano fica ilegível.
ID_SIMULADO = "(simulado)"


def sincronizar_containers(
    om: ClienteOM, declarado: dict, arquivos: list[Arquivo]
) -> dict[tuple[str, ...], str]:
    """Cria bucket, camada, prefixo e arquivo, do raiz para a folha."""
    nome_servico = declarado["name"]
    print(f"Containers ({len(arquivos)} arquivos declarados)")
    identificadores: dict[tuple[str, ...], str] = {}
    for no in arvore(arquivos, declarado):
        fqn = fqn_do_container(nome_servico, no["chave"])
        atual = om.existe("containers", fqn)
        corpo: dict = {
            "name": no["name"],
            "service": nome_servico,
            "description": no["description"],
        }
        if len(no["chave"]) > 1:
            pai = identificadores.get(no["chave"][:-1])
            if not pai:
                om.falhas.append(f"container pai ausente para {fqn}")
                continue
            if pai != ID_SIMULADO:
                corpo["parent"] = {"id": pai, "type": "container"}
        if no.get("prefix"):
            corpo["prefix"] = no["prefix"]
        if no["folha"]:
            corpo["fileFormats"] = ["parquet"]
        if atual:
            identificadores[no["chave"]] = atual["id"]
            om.patch(
                "containers",
                atual["id"],
                operacoes_de_diferenca(atual, {"description": no["description"]}),
                fqn,
            )
            continue
        criado = om.criar("containers", corpo, fqn)
        identificadores[no["chave"]] = criado["id"] if criado else ID_SIMULADO
    return identificadores


def anotar_dag_de_origem(om: ClienteOM, tabela: dict, arquivo: Arquivo) -> None:
    """Grava na tabela qual DAG produziu o arquivo que ela lê.

    A linhagem já responde isso, mas exige percorrer o grafo. Como propriedade,
    a resposta aparece na própria página da tabela. Só grava o que está
    declarado em `meta.dag`; as 19 sem declaração ficam sem a propriedade, e
    não com um valor inventado.
    """
    if not arquivo.dag:
        return
    atual = (tabela.get("extension") or {}).get("mcidDagDeOrigem")
    if atual == arquivo.dag:
        om.conformes += 1
        return
    om.patch(
        "tables",
        tabela["id"],
        [
            {
                "op": "add",
                "path": "/extension",
                "value": {
                    **(tabela.get("extension") or {}),
                    "mcidDagDeOrigem": arquivo.dag,
                },
            }
        ],
        f"DAG de origem de {tabela['name']}",
    )


def sincronizar_linhagem(
    om: ClienteOM,
    arquivos: list[Arquivo],
    identificadores: dict[tuple[str, ...], str],
    servico_banco: str,
    banco: str,
) -> None:
    """Liga cada parquet à tabela Bronze que o lê."""
    print("Linhagem arquivo -> Bronze")
    schemas = schema_por_modelo()
    ligadas = 0
    for arquivo in arquivos:
        origem = identificadores.get((arquivo.bucket, *arquivo.partes))
        for modelo in arquivo.bronze:
            if not om.confirmar:
                print(f"  [{om.modo}] ligaria {arquivo.caminho} -> {modelo}")
                om.atualizados += 1
                continue
            schema = schemas.get(modelo)
            tabela = (
                om.existe(
                    "tables",
                    f"{servico_banco}.{banco}.{schema}.{modelo}",
                    campos="extension",
                )
                if schema
                else None
            )
            if not (origem and tabela):
                faltou = "container" if not origem else "tabela Bronze"
                om.falhas.append(
                    f"não consegui ligar {arquivo.caminho} a {modelo}: "
                    f"{faltou} não encontrado"
                )
                continue
            anotar_dag_de_origem(om, tabela, arquivo)
            if origem in om.montante("table", tabela["id"]):
                om.conformes += 1
                continue
            om.ligar(
                origem, "container", tabela["id"], "table", "Ingestão declarada no dbt."
            )
            ligadas += 1
    if ligadas:
        print(f"  {ligadas} arestas do lake para a Bronze")


def _governanca_declarada(om: ClienteOM, donos: Proprietarios, declarado: dict) -> dict:
    """Dono, domínio e etiquetas prontos para o patch, a partir do YAML."""
    desejado: dict = {}
    dono = donos.resolver(declarado.get("owner_key"))
    if dono:
        desejado["owners"] = dono
    if declarado.get("domain"):
        dominio = om.existe("domains", declarado["domain"])
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
    if declarado.get("tags"):
        desejado["tags"] = [etiqueta(t) for t in declarado["tags"]]
    return desejado


def _limpar_marcacao_do_mcid(om: ClienteOM, donos: Proprietarios, atual: dict) -> dict:
    """Tira a titularidade do MCid de um serviço que não é nosso.

    Remove só o que é NOSSO — o time, o domínio `MCid` e as etiquetas
    `dbtTags`. Marcação de outro órgão, se houver, fica: numa instância
    compartilhada não se apaga o que não se declarou.
    """
    resultado: dict = {}
    nosso_time = {r["id"] for r in (donos.resolver("mcid_data_engineering") or [])}
    atuais_donos = atual.get("owners") or []
    restantes = [o for o in atuais_donos if o.get("id") not in nosso_time]
    if len(restantes) != len(atuais_donos):
        resultado["owners"] = restantes
    atuais_dominios = atual.get("domains") or []
    dominios = [
        d
        for d in atuais_dominios
        if not str(d.get("fullyQualifiedName", "")).startswith("MCid")
    ]
    if len(dominios) != len(atuais_dominios):
        resultado["domains"] = dominios
    atuais_tags = atual.get("tags") or []
    etiquetas = [
        t for t in atuais_tags if not str(t.get("tagFQN", "")).startswith("dbtTags.")
    ]
    if len(etiquetas) != len(atuais_tags):
        resultado["tags"] = etiquetas
    return resultado


def _documentar_dag(
    om: ClienteOM, servico: str, arquivo: Arquivo, governanca: dict, destino: str | None
) -> None:
    """Documenta uma DAG de ingestão e a liga ao arquivo que ela produz."""
    fqn = f"{servico}.{arquivo.dag}"
    pipeline = om.existe("pipelines", fqn, campos="owners,domains,tags")
    if not pipeline:
        # A DAG existe no Airflow mas pode não ter sido ingerida no catálogo:
        # 55 das nossas 69 estavam nesse caso. Sem criá-la, o grafo fica sem
        # justamente a ponta que responde "quem trouxe esse dado".
        pipeline = om.criar("pipelines", {"name": arquivo.dag, "service": servico}, fqn)
        if not pipeline:
            return
    om.patch(
        "pipelines", pipeline["id"], operacoes_de_diferenca(pipeline, governanca), fqn
    )
    if not destino:
        return
    if om.confirmar and pipeline["id"] in om.montante("container", destino):
        om.conformes += 1
        return
    om.ligar(
        pipeline["id"], "pipeline", destino, "container", "Ingestão declarada no dbt."
    )


def sincronizar_orquestracao(
    om: ClienteOM,
    donos: Proprietarios,
    arquivos: list[Arquivo],
    identificadores: dict[tuple[str, ...], str],
) -> None:
    """Documenta o serviço de orquestração e as DAGs que alimentam o lake.

    O serviço já existe e é nosso — o `airflow` traz as DAGs deste repo, e o
    MinC tem o dele em separado. O que se preenche aqui é o que estava vazio.
    """
    declarado = carregar("servicos.yml").get("orquestracao") or {}
    if not declarado:
        return
    servico = declarado["name"]
    print("Orquestração")
    atual = om.existe("services/pipelineServices", servico, campos="owners,domains,tags")
    if not atual:
        if not om.offline:
            om.falhas.append(f"serviço de pipeline '{servico}' não existe na instância")
        return
    if declarado.get("compartilhado"):
        # Serviço que não é nosso: só a descrição é, e a marcação do MCid é
        # RETIRADA se estiver lá. Aqui estava — foi aplicada por engano numa
        # infraestrutura que hospeda 86 DAGs, das quais 22 são deste repositório.
        governanca = _limpar_marcacao_do_mcid(om, donos, atual)
    else:
        governanca = _governanca_declarada(om, donos, declarado)
    om.patch(
        "services/pipelineServices",
        atual["id"],
        operacoes_de_diferenca(
            atual, {"description": texto(declarado.get("description")), **governanca}
        ),
        servico,
    )

    com_dag = [a for a in arquivos if a.dag]
    print(
        f"  {len(com_dag)} arquivos com DAG declarada, {len(arquivos) - len(com_dag)} sem"
    )
    # A governança das DAGs é OUTRA que a do serviço: o serviço é compartilhado,
    # as DAGs deste repositório são nossas. Reaproveitar a do serviço aqui
    # apagaria a marcação das nossas junto com a dele.
    governanca_das_dags = _governanca_declarada(om, donos, declarado.get("dags") or {})
    for arquivo in com_dag:
        _documentar_dag(
            om,
            servico,
            arquivo,
            governanca_das_dags,
            identificadores.get((arquivo.bucket, *arquivo.partes)),
        )


def sincronizar(
    om: ClienteOM, donos: Proprietarios, servico_banco: str, banco: str
) -> None:
    declarado = carregar("servicos.yml").get("lake") or {}
    if not declarado:
        raise SystemExit("Seção `lake` ausente em governance/servicos.yml")
    sincronizar_servico(om, donos, declarado)
    arquivos = carregar_arquivos()
    identificadores = sincronizar_containers(om, declarado, arquivos)
    sincronizar_linhagem(om, arquivos, identificadores, servico_banco, banco)
    sincronizar_orquestracao(om, donos, arquivos, identificadores)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirmar", action="store_true", help="escreve de fato")
    args = parser.parse_args()

    config = ambiente(exigir_acesso=args.confirmar)
    om = ClienteOM(config["url"], config["token"], args.confirmar)
    om.cabecalho()
    donos = Proprietarios(om, carregar("dominios.yml"))
    sincronizar(om, donos, config["servico"], config["banco"])
    return om.resumo()


if __name__ == "__main__":
    raise SystemExit(main())
