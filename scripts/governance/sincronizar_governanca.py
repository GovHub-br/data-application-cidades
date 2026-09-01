#!/usr/bin/env python3
"""Aplica no OpenMetadata a governança declarada no dbt.

`sincronizar_openmetadata.py` cuida da estrutura: schema, tabela, coluna e
linhagem. Este cuida de tudo que diz de QUEM é o dado, o QUE ele significa e
se pode ser usado — e é o que faltava para o nosso schema ter as tabelas
etiquetadas e certificadas como o schema de referência já tem.

Lê o que está declarado em:

    governance/servicos.yml     descrição do serviço de banco e do database
    governance/schemas.yml      descrição, nome de exibição e camada do schema
    governance/dominios.yml     domínio, produto, proprietário, classificação,
                                etiqueta, tier, certificação e permissão de uso
    governance/termos_mcid.yml  termos do glossário e a que modelos/colunas se
                                aplicam

Nada é criado pela interface. O que está nos arquivos é o que existe no
catálogo; quem editar na tela perde a alteração no próximo sync.

É idempotente: rodar duas vezes seguidas deve terminar com `atualizados=0`.
Por padrão não escreve — sem `--confirmar` apenas relata o que faria. Sem
URL/token no ambiente, roda offline e imprime só o que está declarado.

Uso:
    poetry run python scripts/governance/sincronizar_governanca.py
    poetry run python scripts/governance/sincronizar_governanca.py --confirmar
"""

from __future__ import annotations

import argparse
import json

from governanca_comum import (
    RAIZ,
    ClienteOM,
    Proprietarios,
    ambiente,
    carregar,
    etiqueta,
    operacoes_de_diferenca,
    referencia,
    relacao_de_termo,
    texto,
)

CATALOGO = RAIZ / "docs-conjuntura" / "openmetadata_semantic_catalog.json"

#: Classificações cujo conteúdo é nosso. Numa tabela, as etiquetas destas
#: classificações são substituídas pelo que o YAML declara; as de qualquer
#: outra (PII, por exemplo) são preservadas. É o que torna o sync declarativo
#: sem que ele apague vocabulário que não é dele.
CLASSIFICACOES_GERENCIADAS = {"dbtTags", "Uso", "Tier", "Certification"}


# ── classificações e etiquetas ──────────────────────────────────────────────
def sincronizar_classificacoes(om: ClienteOM, dominios: dict) -> None:
    """Cria as classificações e etiquetas antes de qualquer coisa usá-las.

    Sem isto, o PATCH de etiqueta era recusado tabela a tabela: o sync pedia
    `Uso.NaoConsumivel` e `dbtTags.mcid` sem que nenhuma das duas existisse na
    instância.
    """
    print("Classificações")
    for declarada in dominios.get("classificacoes") or []:
        nome = declarada["name"]
        # Numa instância compartilhada entre ministérios, uma classificação que
        # já existe pode ser de outro órgão — `dbtTags` carrega as etiquetas do
        # MinC ao lado das nossas. Nesse caso só acrescentamos o que falta.
        compartilhada = bool(declarada.get("compartilhada"))
        desejado = {
            "name": nome,
            "displayName": declarada.get("display_name", nome),
            "description": texto(declarada.get("description")),
            "mutuallyExclusive": bool(declarada.get("mutuamente_exclusivas")),
        }
        atual = om.existe("classifications", nome)
        if atual and compartilhada:
            om.conformes += 1
        elif atual:
            # `mutuallyExclusive` é imutável depois de criada: incluí-lo no
            # patch faria a API recusar toda atualização da classificação.
            om.patch(
                "classifications",
                atual["id"],
                operacoes_de_diferenca(
                    atual, {k: v for k, v in desejado.items() if k != "mutuallyExclusive"}
                ),
                f"classificação {nome}",
            )
        else:
            om.criar("classifications", desejado, nome)

        for tag in declarada.get("etiquetas") or []:
            fqn = f"{nome}.{tag['name']}"
            desejada = {
                "name": tag["name"],
                "displayName": tag.get("display_name", tag["name"]),
                "description": texto(tag.get("description")),
                "classification": nome,
            }
            existente = om.existe("tags", fqn)
            if existente and compartilhada:
                om.conformes += 1
            elif existente:
                om.patch(
                    "tags",
                    existente["id"],
                    operacoes_de_diferenca(
                        existente,
                        {k: v for k, v in desejada.items() if k != "classification"},
                    ),
                    f"etiqueta {fqn}",
                )
            else:
                om.criar("tags", desejada, fqn)

    for nativa in dominios.get("classificacoes_nativas") or []:
        if om.offline:
            print(f"  [offline] conferiria a classificação nativa {nativa}")
        elif om.existe("classifications", nativa):
            om.conformes += 1
        else:
            om.falhas.append(
                f"classificação nativa {nativa} não existe na instância; "
                "a certificação e o tier não poderão ser aplicados"
            )
            print(f"  FALHA classificação nativa ausente: {nativa}")


def sincronizar_propriedades_customizadas(om: ClienteOM, dominios: dict) -> None:
    """Declara as propriedades customizadas do MCID nos tipos de entidade.

    O tipo de entidade é GLOBAL: uma propriedade criada em `table` aparece para
    todos os ministérios da instância. Daí o prefixo `mcid`, que já era a
    convenção das duas que existiam. Só criamos o que falta — nunca removemos,
    porque as outras podem não ser nossas.
    """
    declaradas = dominios.get("propriedades_customizadas") or []
    if not declaradas:
        return
    print("Propriedades customizadas")
    tipos_de_campo = {
        t["name"]: t["id"]
        for t in om.listar("metadata/types", {"category": "field", "limit": 100})
    }
    for propriedade in declaradas:
        # `customProperties` só vem se for pedido — sem isso a lista chega
        # vazia, a propriedade parece faltar e é recriada a cada execução.
        entidade = om.existe(
            "metadata/types", propriedade["entidade"], campos="customProperties"
        )
        if not entidade:
            om.falhas.append(f"tipo de entidade {propriedade['entidade']} não existe")
            continue
        existentes = {c["name"] for c in (entidade.get("customProperties") or [])}
        if propriedade["name"] in existentes:
            om.conformes += 1
            continue
        tipo = tipos_de_campo.get(propriedade["tipo"])
        if not tipo:
            om.falhas.append(f"tipo de campo '{propriedade['tipo']}' não existe")
            continue
        om.criar(
            f"metadata/types/{entidade['id']}",
            {
                "name": propriedade["name"],
                "description": texto(propriedade.get("description")),
                "propertyType": {"id": tipo, "type": "type"},
            },
            f"{propriedade['entidade']}.{propriedade['name']}",
        )


def sincronizar_glossario(om: ClienteOM, donos: Proprietarios, termos: dict) -> None:
    """Dono e revisores do glossário — a entidade, não os termos dentro dele."""
    declarado = termos.get("glossario") or {}
    if not declarado:
        return
    print("Glossário")
    atual = om.existe("glossaries", declarado["name"], campos="owners,reviewers")
    if not atual:
        om.falhas.append(f"glossário {declarado['name']} não existe na instância")
        return
    desejado: dict = {}
    dono = donos.resolver(declarado.get("owner_key"))
    if dono:
        desejado["owners"] = dono
    revisores = []
    for nome in declarado.get("revisores") or []:
        usuario = om.existe("users", nome)
        if usuario:
            revisores.append(referencia(usuario, "user"))
        else:
            om.falhas.append(f"revisor '{nome}' não existe como usuário")
    if revisores:
        desejado["reviewers"] = revisores
    om.patch(
        "glossaries",
        atual["id"],
        operacoes_de_diferenca(atual, desejado),
        declarado["name"],
    )


# ── serviço e banco ─────────────────────────────────────────────────────────
def _documentar_entidade_de_servico(
    om: ClienteOM, donos: Proprietarios, rota: str, fqn: str, decl: dict
) -> None:
    """Preenche descrição, dono, domínio e etiqueta de uma entidade existente.

    Nunca cria e nunca toca em conexão: serviço e database são de quem
    administra a instância. O nome de exibição vai em patch SEPARADO porque o
    bot de ingestão é proibido de alterá-lo, e JSON Patch é tudo ou nada — indo
    junto, o 403 derrubava descrição, dono, domínio e etiqueta com ele.
    """
    atual = om.existe(rota, fqn, campos="owners,domains,tags")
    if not atual:
        if om.offline:
            print(f"  [offline] documentaria {rota}: {fqn}")
            om.atualizados += 1
        else:
            om.falhas.append(
                f"{rota} '{fqn}' não existe na instância. "
                "Ele é criado por quem administra o OpenMetadata, não por nós."
            )
            print(f"  FALHA {rota} ausente: {fqn}")
        return
    desejado: dict = {"description": texto(decl.get("description"))}
    dono = donos.resolver(decl.get("owner_key"))
    if dono:
        desejado["owners"] = dono
    if decl.get("domain"):
        dominio = om.existe("domains", decl["domain"])
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
    if decl.get("tags"):
        desejado["tags"] = mesclar_etiquetas(
            atual.get("tags") or [], [etiqueta(t) for t in decl["tags"]]
        )
    om.patch(rota, atual["id"], operacoes_de_diferenca(atual, desejado), fqn)
    if decl.get("display_name"):
        om.patch(
            rota,
            atual["id"],
            operacoes_de_diferenca(atual, {"displayName": decl["display_name"]}),
            f"nome de exibição de {fqn} "
            f"({atual.get('displayName')!r} -> {decl['display_name']!r})",
        )


def sincronizar_servico_e_banco(
    om: ClienteOM, donos: Proprietarios, servico: str, banco: str
) -> None:
    """Documenta serviço de banco, database e serviço de consumo."""
    print("Serviços, database e consumo")
    declarado = carregar("servicos.yml")
    alvos = [
        # o serviço vive sob `services/`, e não na raiz da API como as demais
        # entidades — procurá-lo em `databaseServices` devolve 404 e faz o
        # serviço parecer inexistente.
        ("services/databaseServices", servico, declarado.get("servico") or {}),
        ("databases", f"{servico}.{banco}", declarado.get("banco") or {}),
    ]
    consumo = declarado.get("consumo") or {}
    if consumo:
        alvos.append(("services/dashboardServices", consumo["name"], consumo))
    for rota, fqn, decl in alvos:
        if decl:
            _documentar_entidade_de_servico(om, donos, rota, fqn, decl)


# ── domínios e produtos ─────────────────────────────────────────────────────
def sincronizar_dominios(om: ClienteOM, donos: Proprietarios, dominios: dict) -> None:
    print("Domínios")
    for d in dominios["dominios"]:
        # o FQN do subdomínio é `Pai.Filho`; referenciar pelo nome curto dá 404
        fqn = f"{d['parent']}.{d['name']}" if d.get("parent") else d["name"]
        desejado: dict = {
            "displayName": d.get("display_name", d["name"]),
            "description": texto(d.get("description")),
        }
        dono = donos.resolver(d.get("owner_key"))
        if dono:
            desejado["owners"] = dono
        atual = om.existe("domains", fqn, campos="owners")
        if atual:
            # antes o patch cobria só `/description`: mudar nome de exibição ou
            # dono no YAML não mudava nada no catálogo.
            om.patch("domains", atual["id"], operacoes_de_diferenca(atual, desejado), fqn)
        else:
            corpo = {
                "name": d["name"],
                "domainType": d.get("domain_type", "Aggregate"),
                **desejado,
            }
            if d.get("parent"):
                corpo["parent"] = d["parent"]
            om.criar("domains", corpo, fqn)


def sincronizar_produtos(om: ClienteOM, donos: Proprietarios, dominios: dict) -> None:
    print("Produtos de dados")
    for p in dominios["produtos"]:
        desejado: dict = {
            "displayName": p.get("display_name", p["name"]),
            "description": texto(p.get("description")),
        }
        dono = donos.resolver(p.get("owner_key"))
        if dono:
            desejado["owners"] = dono
        dominio = om.existe("domains", p["domain"])
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
        marcas = etiquetas_do_produto(dominios, p["name"])
        marcas += [etiqueta(fqn, "Glossary") for fqn in p.get("termos") or []]
        desejado["tags"] = marcas
        peritos = [
            referencia(usuario, "user")
            for usuario in (om.existe("users", nome) for nome in p.get("experts") or [])
            if usuario
        ]
        if peritos:
            desejado["experts"] = peritos
        reconciliacoes = resumo_de_reconciliacoes(p["name"])
        if reconciliacoes:
            desejado["extension"] = {"mcidReconciliacoes": reconciliacoes}
        atual = om.existe(
            "dataProducts", p["name"], campos="owners,domains,tags,experts,extension"
        )
        if atual:
            om.patch(
                "dataProducts",
                atual["id"],
                operacoes_de_diferenca(atual, desejado),
                f"produto {p['name']}",
            )
        else:
            om.criar(
                "dataProducts",
                {"name": p["name"], "domains": [p["domain"]], **desejado},
                p["name"],
            )


# ── glossário ───────────────────────────────────────────────────────────────
def sincronizar_termos(om: ClienteOM, termos: dict) -> None:
    """Cria e enriquece os termos. Relações vêm num segundo passe.

    `relatedTerms` referencia outros termos por id; só dá para ligá-los depois
    que todos existirem, senão o primeiro termo do arquivo tentaria apontar
    para um que ainda não foi criado.
    """
    print("Termos de glossário")
    declarados = termos.get("termos") or []
    for t in declarados:
        _criar_ou_atualizar_termo(om, t)
    reciprocas = _relacoes_reciprocas(declarados)
    for t in declarados:
        _relacionar_termo(om, t, reciprocas.get(t["fqn"], []))


def _relacoes_reciprocas(declarados: list[dict]) -> dict[str, list[str]]:
    """Fecha as relações nos dois sentidos antes de aplicar.

    A relação entre termos é BIDIRECIONAL no OpenMetadata: ligar Bronze a
    Silver aparece também em Silver. Como o patch substitui a lista inteira,
    declarar só um sentido fazia o segundo termo apagar o vínculo do primeiro —
    Bronze perdia Silver no momento em que Silver recebia Gold. Declarando
    `A -> B` no YAML, aplicamos `A -> B` e `B -> A`.
    """
    mapa: dict[str, set[str]] = {}
    for t in declarados:
        for outro in t.get("related_terms") or []:
            mapa.setdefault(t["fqn"], set()).add(outro)
            mapa.setdefault(outro, set()).add(t["fqn"])
    return {fqn: sorted(alvos) for fqn, alvos in mapa.items()}


def _criar_ou_atualizar_termo(om: ClienteOM, t: dict) -> None:
    partes = t["fqn"].split(".")
    desejado: dict = {
        "displayName": t.get("display_name", t["name"]),
        "description": texto(t.get("description")),
    }
    if t.get("synonyms"):
        desejado["synonyms"] = t["synonyms"]
    if t.get("references"):
        desejado["references"] = [
            {"name": r["name"], "endpoint": r["endpoint"]} for r in t["references"]
        ]
    atual = om.existe("glossaryTerms", t["fqn"])
    if atual:
        om.patch(
            "glossaryTerms",
            atual["id"],
            operacoes_de_diferenca(atual, desejado),
            f"termo {t['fqn']}",
        )
        return
    corpo = {"name": t["name"], "glossary": partes[0], **desejado}
    # o eixo do termo é o pai; sem ele, criar `MCID.Governanca.Safra` devolve 404
    pai = t.get("parent") or (".".join(partes[:-1]) if len(partes) > 2 else None)
    if pai:
        corpo["parent"] = pai
    om.criar("glossaryTerms", corpo, t["fqn"])


def _relacionar_termo(om: ClienteOM, t: dict, relacionados: list[str]) -> None:
    if not relacionados:
        return
    atual = om.existe("glossaryTerms", t["fqn"], campos="relatedTerms")
    if not atual:
        if om.offline:
            print(f"  [offline] relacionaria {t['fqn']} a {len(relacionados)} termos")
        return
    referencias = []
    for outro in relacionados:
        entidade = om.existe("glossaryTerms", outro)
        if entidade:
            referencias.append(relacao_de_termo(entidade))
        else:
            om.falhas.append(f"termo relacionado inexistente: {outro} (em {t['fqn']})")
    if referencias:
        om.patch(
            "glossaryTerms",
            atual["id"],
            operacoes_de_diferenca(atual, {"relatedTerms": referencias}),
            f"relações de {t['fqn']}",
        )


#: Colunas do contrato de dimensão temporal (macro `dimensao_temporal.sql`).
#: São o que torna duas séries do conjuntura comparáveis entre si — e o que
#: responde "com o que eu posso cruzar esta tabela".
GRANULARIDADE_TEMPORAL = (
    "data_referencia",
    "periodo",
    "ano",
    "mes",
    "trimestre",
    "edicao",
)

#: Camadas que recebem relações semânticas. A Bronze fica de fora: ela é cópia
#: fiel da origem, e o que se publica dela é a topologia, não o significado.
CAMADAS_COM_RELACOES = {"silver", "gold"}


def vizinhanca_do_modelo(catalogo: dict) -> dict[str, dict[str, list[str]]]:
    """De quem cada model lê e quem lê dele, direto do grafo do dbt."""
    por_id = {m["id"]: m["name"] for m in catalogo.get("models", [])}
    acima: dict[str, list[str]] = {}
    abaixo: dict[str, list[str]] = {}
    for modelo in catalogo.get("models", []):
        nome = modelo["name"]
        for pai in modelo.get("depends_on") or []:
            if pai in por_id:
                acima.setdefault(nome, []).append(por_id[pai])
                abaixo.setdefault(por_id[pai], []).append(nome)
    return {
        m["name"]: {
            "le_de": sorted(acima.get(m["name"], [])),
            "lido_por": sorted(abaixo.get(m["name"], [])),
        }
        for m in catalogo.get("models", [])
    }


def markdown_das_relacoes(modelo: dict, vizinhos: dict[str, list[str]]) -> str:
    """Texto que descreve com o que a tabela se relaciona e por qual chave.

    Sai do grafo do dbt e do contrato de dimensão temporal — nada é inferido
    por semelhança de nome. Não cria FK física nem aresta de linhagem: é
    auxílio de descoberta, para quem procura "com o que posso cruzar isto".
    """
    colunas = {c["name"] for c in modelo.get("columns") or []}
    grao = [c for c in GRANULARIDADE_TEMPORAL if c in colunas]
    linhas = []
    if grao:
        linhas.append(
            "**Granularidade temporal:** `" + "`, `".join(grao) + "`. "
            "Séries que compartilham estas colunas são comparáveis entre si "
            "no mesmo período."
        )
    if vizinhos["le_de"]:
        linhas.append("**Lê de:** " + ", ".join(f"`{n}`" for n in vizinhos["le_de"]))
    if vizinhos["lido_por"]:
        linhas.append(
            "**É lido por:** " + ", ".join(f"`{n}`" for n in vizinhos["lido_por"])
        )
    if not linhas:
        return ""
    linhas.append(
        "_Derivado do grafo do dbt e do contrato de dimensão temporal. "
        "Não constitui chave estrangeira nem aresta de linhagem._"
    )
    return "\n\n".join(linhas)


def _referencias_dos_vizinhos(
    om: ClienteOM, catalogo: dict, vizinhos: dict, servico: str, banco: str
) -> list[dict]:
    """Atalhos navegáveis para as tabelas vizinhas no grafo."""
    por_nome = {m["name"]: m for m in catalogo.get("models", [])}
    referencias = []
    for vizinho in vizinhos["le_de"] + vizinhos["lido_por"]:
        outro = por_nome.get(vizinho)
        if not outro:
            continue
        entidade = om.existe(
            "tables", f"{servico}.{banco}.{outro['schema']}.{outro['name']}"
        )
        if entidade:
            # `id` e `type` sozinhos não bastam: a interface usa `name` e
            # `fullyQualifiedName` como rótulo do link, e sem eles a lista
            # aparece com entradas em branco.
            referencias.append(referencia(entidade, "table"))
    return referencias


def sincronizar_relacoes_semanticas(om: ClienteOM, servico: str, banco: str) -> None:
    """Publica vizinhança e granularidade nas tabelas Silver e Gold."""
    if not CATALOGO.exists():
        return
    print("Relações semânticas")
    catalogo = json.loads(CATALOGO.read_text(encoding="utf-8"))
    vizinhanca = vizinhanca_do_modelo(catalogo)
    escritas = 0
    for modelo in catalogo.get("models", []):
        if modelo.get("layer") not in CAMADAS_COM_RELACOES:
            continue
        texto_md = markdown_das_relacoes(modelo, vizinhanca[modelo["name"]])
        if not texto_md:
            continue
        fqn = f"{servico}.{banco}.{modelo['schema']}.{modelo['name']}"
        if not om.confirmar:
            om.atualizados += 1
            continue
        tabela = om.existe("tables", fqn, campos="extension")
        if not tabela:
            continue
        relacionadas = _referencias_dos_vizinhos(
            om, catalogo, vizinhanca[modelo["name"]], servico, banco
        )
        desejado = {
            **(tabela.get("extension") or {}),
            "mcidSemanticRelationships": texto_md,
        }
        if relacionadas:
            desejado["mcidRelatedTables"] = relacionadas
        if not operacoes_de_diferenca(tabela, {"extension": desejado}):
            om.conformes += 1
            continue
        om.patch(
            "tables",
            tabela["id"],
            [{"op": "add", "path": "/extension", "value": desejado}],
            f"relações de {modelo['name']}",
        )
        escritas += 1
    print(f"  {escritas} tabelas Silver/Gold com relações publicadas")


# ── catalogação de schemas, tabelas e colunas ───────────────────────────────
def vocabulario_declarado(termos: dict) -> set[str]:
    """FQNs de glossário que este repo declara — e portanto governa.

    Um termo que não está aqui foi pendurado por uma pessoa e não é nosso para
    tirar. O schema `conjuntura_continuo_mart` tinha `MCID.IndicadoresConjunturais`
    aplicado na mão, e o sync o removia a cada execução: nunca convergia, e
    desfazia curadoria alheia de quebra.
    """
    declarados = {t["fqn"] for t in (termos.get("termos") or [])}
    declarados |= set(termos.get("aplicacao_de_termos_existentes") or {})
    declarados |= set(termos.get("aplicacao_em_colunas") or {})
    return declarados


def mesclar_etiquetas(
    atuais: list[dict], desejadas: list[dict], glossario_nosso: set[str] | None = None
) -> list[dict]:
    """Substitui as etiquetas que governamos e preserva as que não são nossas.

    Trocar a lista inteira apagaria classificações de terceiros na instância
    compartilhada; somar sem trocar deixaria etiqueta obsoleta para sempre.
    """
    nosso = glossario_nosso or set()
    preservadas = [
        tag
        for tag in atuais
        if str(tag.get("tagFQN", "")).split(".")[0] not in CLASSIFICACOES_GERENCIADAS
        and not (tag.get("source") == "Glossary" and tag.get("tagFQN") in nosso)
    ]
    vistas = set()
    resultado = []
    for tag in preservadas + desejadas:
        if tag["tagFQN"] not in vistas:
            vistas.add(tag["tagFQN"])
            resultado.append(tag)
    return resultado


def camadas_por_modelo() -> dict[str, str]:
    """Camada real de cada modelo, lida do catálogo semântico.

    A camada do SCHEMA não serve para etiquetar tabela: `empreendimento_far` e
    `entidades_fds` guardam bronze, silver e gold no mesmo schema, declarado
    como `mixed`. Usar a camada do schema daria Tier1 e `Uso.Consumivel` às
    bronzes desses dois produtos — exatamente o contrário do que a camada diz.
    """
    if not CATALOGO.exists():
        return {}
    catalogo = json.loads(CATALOGO.read_text(encoding="utf-8"))
    return {m["name"]: m["layer"] for m in catalogo.get("models", []) if m.get("layer")}


def etiquetas_do_produto(dominios: dict, produto: str) -> list[dict]:
    """Organização e produto — a camada não se aplica a um produto inteiro."""
    automaticas = dominios.get("etiquetas_automaticas") or {}
    fqns = [
        f
        for f in (
            automaticas.get("organizacao"),
            (automaticas.get("por_produto") or {}).get(produto),
        )
        if f
    ]
    return [etiqueta(f) for f in fqns]


def resumo_de_reconciliacoes(produto: str) -> str:
    """Texto em markdown com o que foi conferido e o que não é comparável.

    Sai de `reconciliacoes.yml`. Publicar o BLOQUEIO importa tanto quanto o
    cruzamento ativo: sem ele, a ausência de uma conferência parece
    esquecimento, e alguém refaz a análise que o time já sabe que não fecha.
    """
    entradas = [
        r
        for r in (carregar("reconciliacoes.yml").get("reconciliations") or [])
        if r.get("product") == produto
    ]
    if not entradas:
        return ""
    rotulos = {
        "active": "Conferido",
        "blocked_not_equivalent": "Não comparável",
        "frozen_edition_required": "Exige safra congelada",
    }
    linhas = []
    for r in entradas:
        rotulo = rotulos.get(r.get("status", ""), r.get("status", ""))
        linhas.append(f"- **{r['id']}** — {rotulo}. {texto(r.get('rationale'))}")
    return "\n".join(linhas)


def etiquetas_da_camada(dominios: dict, produto: str, camada: str) -> list[dict]:
    """Etiqueta, tier, certificação e permissão de uso de uma tabela."""
    automaticas = dominios.get("etiquetas_automaticas") or {}
    fqns = []
    if automaticas.get("organizacao"):
        fqns.append(automaticas["organizacao"])
    if automaticas.get("por_produto", {}).get(produto):
        fqns.append(automaticas["por_produto"][produto])
    if automaticas.get("por_camada", {}).get(camada):
        fqns.append(automaticas["por_camada"][camada])
    tier = (dominios.get("certificacao_por_camada") or {}).get(camada)
    if tier:
        fqns.append(f"Tier.{tier}")
    uso = (dominios.get("permissao_de_uso_por_camada") or {}).get(camada)
    if uso:
        fqns.append(uso)
    # a certificação de curadoria NÃO entra aqui: ver `certificacao_da_camada`
    return [etiqueta(fqn) for fqn in fqns]


def certificacao_da_camada(dominios: dict, camada: str) -> str | None:
    """Certificação de curadoria do ativo, que não é uma etiqueta comum.

    Nesta instância a certificação é campo próprio da entidade
    (`/certification`), e não um item de `/tags`. Enviá-la junto das etiquetas
    faz o OpenMetadata **descartá-la em silêncio**: o patch volta 200 e a
    certificação simplesmente não aparece. Foi o que aconteceu na primeira
    aplicação — as tabelas saíram com Tier e permissão de uso, sem certificação.
    """
    valor = (dominios.get("certificacao_de_curadoria_por_camada") or {}).get(camada)
    return str(valor) if valor else None


def aplicar_certificacao(
    om: ClienteOM, rota: str, entidade: dict, camada_fqn: str | None, rotulo: str
) -> None:
    """Aplica a certificação no campo próprio, se ainda não for a declarada.

    O OpenMetadata preenche `appliedDate` e `expiryDate` sozinho, com validade
    de 30 dias — é o sync recorrente que renova.
    """
    if not camada_fqn:
        return
    atual = ((entidade.get("certification") or {}).get("tagLabel") or {}).get("tagFQN")
    if atual == camada_fqn:
        om.conformes += 1
        return
    om.patch(
        rota,
        entidade["id"],
        [
            {
                "op": "add",
                "path": "/certification",
                "value": {"tagLabel": etiqueta(camada_fqn)},
            }
        ],
        f"certificação de {rotulo}",
    )


def termos_por_modelo(termos: dict) -> dict[str, list[str]]:
    mapa: dict[str, list[str]] = {}
    for termo in termos.get("termos") or []:
        for modelo in termo.get("aplica_a") or []:
            mapa.setdefault(modelo, []).append(termo["fqn"])
    for fqn, modelos in (termos.get("aplicacao_de_termos_existentes") or {}).items():
        for modelo in modelos:
            mapa.setdefault(modelo, []).append(fqn)
    return mapa


def termos_por_coluna(termos: dict) -> dict[str, list[str]]:
    """Termos declarados para nomes de coluna, e não para modelos inteiros."""
    mapa: dict[str, list[str]] = {}
    for termo in termos.get("termos") or []:
        for coluna in termo.get("aplica_a_colunas") or []:
            mapa.setdefault(coluna, []).append(termo["fqn"])
    for fqn, colunas in (termos.get("aplicacao_em_colunas") or {}).items():
        for coluna in colunas:
            mapa.setdefault(coluna, []).append(fqn)
    return mapa


def catalogar_schemas(
    om: ClienteOM,
    donos: Proprietarios,
    dominios: dict,
    nosso_glossario: set[str],
    servico: str,
    banco: str,
) -> None:
    """Dono, domínio e etiqueta de camada nos schemas."""
    print("Catalogação dos schemas")
    declarados = {s["name"]: s for s in carregar("schemas.yml")["schemas"]}
    for produto in dominios["produtos"]:
        dominio = om.existe("domains", produto["domain"])
        for nome in produto["schemas"]:
            decl = declarados.get(nome)
            if not decl:
                om.falhas.append(f"schema '{nome}' do produto sem entrada em schemas.yml")
                continue
            fqn = f"{servico}.{banco}.{nome}"
            atual = om.existe(
                "databaseSchemas", fqn, campos="owners,domains,tags,certification"
            )
            if not atual:
                if om.offline:
                    print(f"  [offline] catalogaria schema {nome}")
                    om.atualizados += 1
                continue
            desejado: dict = {}
            dono = donos.resolver(decl.get("owner_key"))
            if dono:
                desejado["owners"] = dono
            if dominio:
                desejado["domains"] = [referencia(dominio, "domain")]
            desejado["tags"] = mesclar_etiquetas(
                atual.get("tags") or [],
                etiquetas_da_camada(dominios, produto["name"], decl.get("layer", "gold")),
                nosso_glossario,
            )
            om.patch(
                "databaseSchemas",
                atual["id"],
                operacoes_de_diferenca(atual, desejado),
                fqn,
            )
            aplicar_certificacao(
                om,
                "databaseSchemas",
                atual,
                certificacao_da_camada(dominios, decl.get("layer", "gold")),
                fqn,
            )


def catalogar_tabelas(
    om: ClienteOM,
    donos: Proprietarios,
    dominios: dict,
    termos: dict,
    servico: str,
    banco: str,
) -> None:
    """Aplica domínio, produto, dono, etiqueta, certificação e glossário.

    A ordem importa: o OpenMetadata recusa vincular tabela a produto de dados
    se ela ainda não pertence ao domínio do produto — a regra chama-se
    `Data Product Domain Validation`. Domínio primeiro, produto depois.
    """
    print("Catalogação das tabelas e colunas")
    declarados = {s["name"]: s for s in carregar("schemas.yml")["schemas"]}
    camadas = camadas_por_modelo()
    por_modelo = termos_por_modelo(termos)
    por_coluna = termos_por_coluna(termos)
    nosso_glossario = vocabulario_declarado(termos)

    for produto in dominios["produtos"]:
        dominio = om.existe("domains", produto["domain"])
        prod = om.existe("dataProducts", produto["name"])
        dono = donos.resolver(produto.get("owner_key"))
        if not om.offline and not (dominio and prod):
            om.falhas.append(
                f"domínio ou produto ausente para {produto['name']}; "
                "tabelas não catalogadas"
            )
            print(f"  domínio ou produto ausente para {produto['name']}; pulando")
            continue

        for schema in produto["schemas"]:
            camada_do_schema = declarados.get(schema, {}).get("layer", "gold")
            fqn_schema = f"{servico}.{banco}.{schema}"
            tabelas = om.listar(
                "tables",
                {
                    "databaseSchema": fqn_schema,
                    "limit": 1000,
                    "fields": "tags,domains,owners,columns,certification,dataProducts",
                },
            )
            if om.offline:
                print(
                    f"  [offline] {schema}: catalogaria as tabelas do produto "
                    f"{produto['name']} com domínio {produto['domain']}, dono "
                    f"{produto.get('owner_key')} e as etiquetas da camada de cada modelo"
                )
                om.atualizados += 1
                continue
            print(f"  {schema}: {len(tabelas)} tabelas")

            for tab in tabelas:
                rotulo = f"{schema}.{tab['name']}"
                # camada do MODELO; a do schema é só o recurso de última hora
                camada = camadas.get(tab["name"], camada_do_schema)

                if dominio:
                    om.patch(
                        "tables",
                        tab["id"],
                        operacoes_de_diferenca(
                            tab, {"domains": [referencia(dominio, "domain")]}
                        ),
                        rotulo,
                    )
                desejado: dict = {}
                if prod:
                    desejado["dataProducts"] = [referencia(prod, "dataProduct")]
                if dono:
                    desejado["owners"] = dono
                marcas = etiquetas_da_camada(dominios, produto["name"], camada)
                marcas += [
                    etiqueta(fqn, "Glossary") for fqn in por_modelo.get(tab["name"], [])
                ]
                desejado["tags"] = mesclar_etiquetas(
                    tab.get("tags") or [], marcas, nosso_glossario
                )
                om.patch(
                    "tables", tab["id"], operacoes_de_diferenca(tab, desejado), rotulo
                )
                aplicar_certificacao(
                    om, "tables", tab, certificacao_da_camada(dominios, camada), rotulo
                )
                catalogar_colunas(om, tab, por_coluna, nosso_glossario, rotulo)


def catalogar_colunas(
    om: ClienteOM,
    tabela: dict,
    por_coluna: dict[str, list[str]],
    nosso_glossario: set[str],
    rotulo: str,
) -> None:
    """Pendura termo de glossário na coluna, não só na tabela.

    O patch endereça a coluna pela posição no array da entidade
    (`/columns/<i>/tags`), então o índice sai da resposta da API e nunca de uma
    ordem presumida.
    """
    for indice, coluna in enumerate(tabela.get("columns") or []):
        fqns = por_coluna.get(coluna["name"])
        if not fqns:
            continue
        desejadas = mesclar_etiquetas(
            coluna.get("tags") or [],
            [etiqueta(fqn, "Glossary") for fqn in fqns],
            nosso_glossario,
        )
        if not operacoes_de_diferenca(coluna, {"tags": desejadas}):
            om.conformes += 1
            continue
        om.patch(
            "tables",
            tabela["id"],
            [{"op": "add", "path": f"/columns/{indice}/tags", "value": desejadas}],
            f"{rotulo}.{coluna['name']}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirmar", action="store_true", help="escreve de fato")
    args = parser.parse_args()

    config = ambiente(exigir_acesso=args.confirmar)
    om = ClienteOM(config["url"], config["token"], args.confirmar)
    om.cabecalho()

    dominios = carregar("dominios.yml")
    termos = carregar("termos_mcid.yml")
    donos = Proprietarios(om, dominios)

    sincronizar_classificacoes(om, dominios)
    sincronizar_propriedades_customizadas(om, dominios)
    sincronizar_dominios(om, donos, dominios)
    sincronizar_servico_e_banco(om, donos, config["servico"], config["banco"])
    sincronizar_produtos(om, donos, dominios)
    sincronizar_termos(om, termos)
    sincronizar_glossario(om, donos, termos)
    nosso_glossario = vocabulario_declarado(termos)
    catalogar_schemas(
        om, donos, dominios, nosso_glossario, config["servico"], config["banco"]
    )
    catalogar_tabelas(om, donos, dominios, termos, config["servico"], config["banco"])
    sincronizar_relacoes_semanticas(om, config["servico"], config["banco"])

    return om.resumo()


if __name__ == "__main__":
    raise SystemExit(main())
