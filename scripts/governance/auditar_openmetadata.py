#!/usr/bin/env python3
"""Confere o que está NA INSTÂNCIA contra o que está declarado no repo.

`auditar_metadados.py` audita o YAML do dbt: se a documentação foi escrita.
Este audita o outro lado: se ela chegou ao catálogo. São perguntas diferentes,
e a distância entre as duas já foi grande — a documentação do dbt esteve 100%
completa enquanto o OpenMetadata mostrava "Modelo da camada gold do produto
conjuntura" em 85 tabelas, sem nenhum erro em lugar nenhum.

O que ele pega e a conferência manual não pegava de graça:

- campo que a API aceita com 200 e descarta em silêncio (foi o caso da
  certificação enviada como etiqueta);
- descrição que diverge para sempre por causa da escapada HTML da instância;
- ativo que existe na instância dentro dos nossos schemas e não está declarado
  em lugar nenhum — tabela órfã ou edição feita pela tela.

Uso:
    poetry run python scripts/governance/auditar_openmetadata.py
    poetry run python scripts/governance/auditar_openmetadata.py --strict
"""

from __future__ import annotations

import argparse
import json
import pathlib
from collections import Counter

import yaml

import sincronizar_governanca as gov
from governanca_comum import (
    ClienteOM,
    ambiente,
    carregar,
    mesmo_texto,
    texto,
)
from sincronizar_openmetadata import build_payload, colunas_divergem

RAIZ_MODELOS = pathlib.Path(gov.CATALOGO).parents[1] / "dbt" / "mcid" / "models"


class Auditoria:
    """Acumula o que foi conferido e o que ficou faltando."""

    def __init__(self) -> None:
        self.contagens: dict[str, Counter] = {}
        self.pendencias: list[str] = []

    def conferir(self, grupo: str, criterio: str, ok: bool) -> None:
        contador = self.contagens.setdefault(grupo, Counter())
        contador[f"{criterio}.total"] += 1
        if ok:
            contador[f"{criterio}.ok"] += 1

    def pendencia(self, mensagem: str) -> None:
        self.pendencias.append(mensagem)

    def percentual(self, grupo: str, criterio: str) -> tuple[int, int]:
        contador = self.contagens.get(grupo, Counter())
        return contador[f"{criterio}.ok"], contador[f"{criterio}.total"]


CRITERIOS_DE_TABELA = (
    ("descrição", "descricao"),
    ("colunas", "colunas"),
    ("domínio", "dominio"),
    ("produto", "produto"),
    ("dono", "dono"),
    ("tier", "tier"),
    ("certif.", "certificacao"),
    ("uso", "uso"),
    ("etiquetas", "etiquetas"),
)


def tem_etiqueta(entidade: dict, prefixo: str) -> bool:
    return any(
        str(t.get("tagFQN", "")).startswith(prefixo) for t in (entidade.get("tags") or [])
    )


def auditar_servico_e_banco(
    om: ClienteOM, aud: Auditoria, servico: str, banco: str
) -> None:
    declarado = carregar("servicos.yml")
    alvos = [
        ("services/databaseServices", servico, declarado.get("servico") or {}),
        ("databases", f"{servico}.{banco}", declarado.get("banco") or {}),
    ]
    for rota, fqn, decl in alvos:
        atual = om.existe(rota, fqn, campos="owners,domains,tags")
        if not atual:
            aud.pendencia(f"{rota} '{fqn}' não existe na instância")
            continue
        aud.conferir(
            "serviço e banco",
            "descricao",
            mesmo_texto(atual.get("description"), texto(decl.get("description"))),
        )
        aud.conferir("serviço e banco", "dono", bool(atual.get("owners")))
        aud.conferir("serviço e banco", "dominio", bool(atual.get("domains")))
        if not mesmo_texto(atual.get("description"), texto(decl.get("description"))):
            aud.pendencia(f"descrição de {fqn} diverge do servicos.yml")
        if not atual.get("owners"):
            aud.pendencia(f"{fqn} sem proprietário")
        # nome de exibição é conferido mas não cobrado: o bot não pode alterá-lo
        if decl.get("display_name") and atual.get("displayName") != decl["display_name"]:
            aud.pendencia(
                f"nome de exibição de {fqn} é {atual.get('displayName')!r} e deveria "
                f"ser {decl['display_name']!r} (exige administrador, o bot não pode)"
            )


def auditar_classificacoes(om: ClienteOM, aud: Auditoria, dominios: dict) -> None:
    for declarada in dominios.get("classificacoes") or []:
        existe = bool(om.existe("classifications", declarada["name"]))
        aud.conferir("vocabulário", "classificacao", existe)
        if not existe:
            aud.pendencia(f"classificação {declarada['name']} não existe")
            continue
        for tag in declarada.get("etiquetas") or []:
            fqn = f"{declarada['name']}.{tag['name']}"
            tem = bool(om.existe("tags", fqn))
            aud.conferir("vocabulário", "etiqueta", tem)
            if not tem:
                aud.pendencia(f"etiqueta {fqn} não existe")
    for nativa in dominios.get("classificacoes_nativas") or []:
        existe = bool(om.existe("classifications", nativa))
        aud.conferir("vocabulário", "classificacao", existe)
        if not existe:
            aud.pendencia(f"classificação nativa {nativa} não existe")


def testes_declarados_no_dbt() -> set[str]:
    """Nomes de test case que os testes de coluna do dbt geram.

    O dbt nomeia `<teste>_<model>_<coluna>`, e é com esse nome que o conector
    do OpenMetadata cria o test case. Comparar por esse nome é o que permite
    dizer se a ingestão está em dia sem duplicar nada.
    """
    modelos = RAIZ_MODELOS
    declarados: set[str] = set()
    for caminho in modelos.rglob("*.yml"):
        try:
            documento = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        for modelo in documento.get("models") or []:
            for coluna in modelo.get("columns") or []:
                testes = coluna.get("tests") or coluna.get("data_tests") or []
                for teste in testes:
                    nome = teste if isinstance(teste, str) else next(iter(teste), "")
                    if nome in ("not_null", "unique"):
                        declarados.add(f"{nome}_{modelo['name']}_{coluna['name']}")
    return declarados


def auditar_qualidade(om: ClienteOM, aud: Auditoria) -> None:
    """Confere se os testes do dbt viraram test case na instância.

    Quem cria o test case é o conector dbt do OpenMetadata, não nós — escrever
    à mão duplicaria o trabalho dele e divergiria no nome. O que a auditoria
    faz é dizer quando a ingestão ficou para trás, que foi como se descobriu
    que ela não roda desde 2026-07-23.
    """
    declarados = testes_declarados_no_dbt()
    if not declarados:
        return
    existentes = {c["name"] for c in om.listar("dataQuality/testCases", {"limit": 1000})}
    for nome in sorted(declarados):
        presente = nome in existentes
        aud.conferir("qualidade", "test_case", presente)
        if not presente:
            aud.pendencia(
                f"teste '{nome}' está declarado no dbt e não existe como test case; "
                "a ingestão dbt do OpenMetadata está defasada"
            )


def auditar_glossario(om: ClienteOM, aud: Auditoria, termos: dict) -> None:
    for t in termos.get("termos") or []:
        atual = om.existe("glossaryTerms", t["fqn"], campos="relatedTerms")
        aud.conferir("glossário", "termo", bool(atual))
        if not atual:
            aud.pendencia(f"termo {t['fqn']} não existe na instância")
            continue
        aud.conferir(
            "glossário",
            "descricao",
            mesmo_texto(atual.get("description"), texto(t.get("description"))),
        )
        if t.get("synonyms"):
            ok = set(atual.get("synonyms") or []) >= set(t["synonyms"])
            aud.conferir("glossário", "sinonimo", ok)
            if not ok:
                aud.pendencia(f"sinônimos de {t['fqn']} não aplicados")
        if t.get("related_terms"):
            aplicados = {
                (r.get("term") or {}).get("fullyQualifiedName")
                for r in (atual.get("relatedTerms") or [])
            }
            ok = aplicados >= set(t["related_terms"])
            aud.conferir("glossário", "relacao", ok)
            if not ok:
                faltando = set(t["related_terms"]) - aplicados
                aud.pendencia(f"relações de {t['fqn']} faltando: {sorted(faltando)}")


def auditar_tabelas(
    om: ClienteOM, aud: Auditoria, dominios: dict, termos: dict, servico: str, banco: str
) -> dict[str, Counter]:
    """Confere tabela e coluna contra o catálogo semântico e a governança."""
    catalogo = json.loads(gov.CATALOGO.read_text(encoding="utf-8"))
    payload = build_payload(catalogo, servico, banco)
    porfqn = {f"{t['databaseSchema']}.{t['name']}": t for t in payload["tables"]}
    camadas = gov.camadas_por_modelo()
    esquemas = {s["name"]: s for s in carregar("schemas.yml")["schemas"]}
    por_coluna = gov.termos_por_coluna(termos)
    por_modelo = gov.termos_por_modelo(termos)
    por_produto: dict[str, Counter] = {}

    for produto in dominios["produtos"]:
        contador: Counter = Counter()
        for schema in produto["schemas"]:
            fqn_schema = f"{servico}.{banco}.{schema}"
            atual_schema = om.existe(
                "databaseSchemas", fqn_schema, campos="owners,domains,tags,certification"
            )
            if atual_schema:
                decl = esquemas.get(schema, {})
                aud.conferir(
                    "schemas",
                    "descricao",
                    mesmo_texto(atual_schema.get("description"), decl.get("description")),
                )
                aud.conferir("schemas", "dono", bool(atual_schema.get("owners")))
                aud.conferir(
                    "schemas", "certificacao", bool(atual_schema.get("certification"))
                )
            else:
                aud.pendencia(f"schema {fqn_schema} não existe na instância")
                continue

            tabelas = om.listar(
                "tables",
                {
                    "databaseSchema": fqn_schema,
                    "limit": 1000,
                    "fields": "tags,domains,owners,dataProducts,certification,columns",
                },
            )
            declaradas = {
                t["name"] for t in porfqn.values() if t["databaseSchema"] == fqn_schema
            }
            for extra in sorted({t["name"] for t in tabelas} - declaradas):
                # existe na instância e não sai do dbt: tabela órfã ou criada
                # pela tela. O catálogo não deveria ter nada que o repo não diga.
                aud.pendencia(
                    f"{schema}.{extra} está na instância e não é declarada no repo"
                )

            for tab in tabelas:
                alvo = porfqn.get(tab["fullyQualifiedName"])
                if not alvo:
                    continue
                contador["tabelas"] += 1
                camada = camadas.get(
                    tab["name"], esquemas.get(schema, {}).get("layer", "gold")
                )
                conferir_tabela(
                    aud, contador, dominios, produto["name"], camada, tab, alvo, schema
                )
                conferir_glossario_da_tabela(aud, por_modelo, tab, schema)
                conferir_colunas(aud, contador, por_coluna, tab, schema)
        por_produto[produto["name"]] = contador
    return por_produto


def conferir_tabela(
    aud: Auditoria,
    contador: Counter,
    dominios: dict,
    produto: str,
    camada: str,
    tab: dict,
    alvo: dict,
    schema: str,
) -> None:
    """Confere a tabela contra o catálogo do dbt e a governança declarada."""
    esperadas = {e["tagFQN"] for e in gov.etiquetas_da_camada(dominios, produto, camada)}
    aplicadas = {t["tagFQN"] for t in (tab.get("tags") or [])}
    certificacao = ((tab.get("certification") or {}).get("tagLabel") or {}).get("tagFQN")
    checagens = {
        "descricao": mesmo_texto(tab.get("description"), alvo["description"]),
        "colunas": not colunas_divergem(tab.get("columns") or [], alvo["columns"]),
        "dominio": bool(tab.get("domains")),
        "produto": bool(tab.get("dataProducts")),
        "dono": bool(tab.get("owners")),
        "tier": tem_etiqueta(tab, "Tier."),
        "certificacao": certificacao == gov.certificacao_da_camada(dominios, camada),
        "uso": tem_etiqueta(tab, "Uso."),
        "etiquetas": esperadas <= aplicadas,
    }
    for criterio, ok in checagens.items():
        aud.conferir("tabelas", criterio, ok)
        if ok:
            contador[criterio] += 1
        else:
            aud.pendencia(f"{schema}.{tab['name']}: {criterio} não conforme")


def conferir_glossario_da_tabela(
    aud: Auditoria, por_modelo: dict[str, list[str]], tab: dict, schema: str
) -> None:
    if not por_modelo.get(tab["name"]):
        return
    esperados = set(por_modelo[tab["name"]])
    tem = {t["tagFQN"] for t in (tab.get("tags") or []) if t.get("source") == "Glossary"}
    ok = esperados <= tem
    aud.conferir("tabelas", "glossario", ok)
    if not ok:
        aud.pendencia(
            f"{schema}.{tab['name']}: termos faltando {sorted(esperados - tem)}"
        )


def conferir_colunas(
    aud: Auditoria,
    contador: Counter,
    por_coluna: dict[str, list[str]],
    tab: dict,
    schema: str,
) -> None:
    for coluna in tab.get("columns") or []:
        aud.conferir("colunas", "descricao", bool(coluna.get("description")))
        if not coluna.get("description"):
            contador["colunas_sem_descricao"] += 1
        if not por_coluna.get(coluna["name"]):
            continue
        esperados = set(por_coluna[coluna["name"]])
        tem = {
            t["tagFQN"]
            for t in (coluna.get("tags") or [])
            if t.get("source") == "Glossary"
        }
        ok = esperados <= tem
        aud.conferir("colunas", "glossario", ok)
        if not ok:
            aud.pendencia(
                f"{schema}.{tab['name']}.{coluna['name']}: "
                f"termos faltando {sorted(esperados - tem)}"
            )


def relatar(aud: Auditoria, por_produto: dict[str, Counter]) -> None:
    print("Cobertura por produto de dados\n")
    cabecalho = f"{'produto':22}{'tabelas':>9}" + "".join(
        f"{rotulo:>11}" for rotulo, _ in CRITERIOS_DE_TABELA
    )
    print(cabecalho)
    print("-" * len(cabecalho))
    for nome, contador in por_produto.items():
        total = contador["tabelas"]
        linha = f"{nome:22}{total:>9}"
        for _, chave in CRITERIOS_DE_TABELA:
            linha += f"{contador[chave]:>7}/{total:<3}"
        print(linha)

    print("\nCobertura por grupo\n")
    for grupo, contador in aud.contagens.items():
        criterios = sorted({c.rsplit(".", 1)[0] for c in contador})
        partes = []
        for criterio in criterios:
            ok, total = aud.percentual(grupo, criterio)
            marca = "✓" if ok == total else "✗"
            partes.append(f"{marca} {criterio} {ok}/{total}")
        print(f"  {grupo:16} " + "   ".join(partes))

    sem_descricao = sum(c["colunas_sem_descricao"] for c in por_produto.values())
    if sem_descricao:
        print(f"\n  colunas sem descrição: {sem_descricao}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict", action="store_true", help="sai com erro se houver pendência"
    )
    parser.add_argument(
        "--listar", type=int, default=25, help="quantas pendências listar"
    )
    args = parser.parse_args()

    config = ambiente(exigir_acesso=True)
    om = ClienteOM(config["url"], config["token"], confirmar=False)
    dominios = carregar("dominios.yml")
    termos = carregar("termos_mcid.yml")

    aud = Auditoria()
    auditar_servico_e_banco(om, aud, config["servico"], config["banco"])
    auditar_classificacoes(om, aud, dominios)
    auditar_qualidade(om, aud)
    auditar_glossario(om, aud, termos)
    por_produto = auditar_tabelas(
        om, aud, dominios, termos, config["servico"], config["banco"]
    )

    relatar(aud, por_produto)

    if aud.pendencias:
        print(f"\n{len(aud.pendencias)} pendências:")
        for mensagem in aud.pendencias[: args.listar]:
            print(f"  - {mensagem}")
        if len(aud.pendencias) > args.listar:
            print(f"  ... e mais {len(aud.pendencias) - args.listar}")
    else:
        print("\nNenhuma pendência: a instância corresponde ao que o repo declara.")

    return 1 if args.strict and aud.pendencias else 0


if __name__ == "__main__":
    raise SystemExit(main())
