#!/usr/bin/env python3
"""Base compartilhada pelos sincronizadores de governança do OpenMetadata.

Existe porque os dois scripts carregavam o `.env` por conta própria, falavam
com a API cada um do seu jeito e — o pior — resolviam o proprietário por
caminhos diferentes: um punha o usuário `admin` nas tabelas enquanto o outro
punha o time `mcid-data-engineering` nos produtos. Dono é declaração de
governança, não detalhe de implementação de script; agora sai de um lugar só.

Três modos de execução:

``offline``    sem URL/token no ambiente. Não fala com a instância; imprime o
               que está declarado. Serve para revisar a declaração sem
               credencial na mão.
``simulacao``  com credencial, sem ``--confirmar``. Lê a instância, compara com
               o declarado e relata a diferença. Não escreve.
``confirmar``  escreve.
"""

from __future__ import annotations

import html
import os
import pathlib
from typing import Any
from urllib.parse import quote

import requests
import yaml

RAIZ = pathlib.Path(__file__).resolve().parents[2]
GOV = RAIZ / "dbt" / "mcid" / "governance"

#: Variáveis que nomeiam entidades e por isso são exigidas em qualquer modo:
#: sem elas não se monta um FQN, e sem FQN não há o que planejar.
VARS_DE_NOME = ("OPENMETADATA_DATABASE_SERVICE", "OPENMETADATA_DATABASE_NAME")
#: Variáveis de acesso. Só fazem falta quando se vai falar com a instância.
VARS_DE_ACESSO = ("OPENMETADATA_URL", "OPENMETADATA_JWT_TOKEN")


class ConfiguracaoAusente(SystemExit):
    """Erro de configuração que diz qual variável falta e em que arquivo."""

    def __init__(self, faltando: list[str]) -> None:
        super().__init__(
            "Faltam variáveis do OpenMetadata em "
            f"{RAIZ / '.env'}: {', '.join(faltando)}.\n"
            f"O modelo com a explicação de cada uma está em "
            f"{RAIZ / 'infra' / 'env' / '.env.example'}."
        )


def carregar_env() -> None:
    """Carrega o `.env` da raiz sem sobrescrever o que já veio do ambiente."""
    arquivo = RAIZ / ".env"
    if not arquivo.exists():
        return
    for linha in arquivo.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, _, valor = linha.partition("=")
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def ambiente(exigir_acesso: bool) -> dict[str, str | None]:
    """Devolve a configuração do OpenMetadata.

    ``exigir_acesso`` é verdadeiro quando o comando vai escrever: aí URL e
    token deixam de ser opcionais.
    """
    carregar_env()
    faltando = [nome for nome in VARS_DE_NOME if not os.environ.get(nome)]
    if exigir_acesso:
        faltando += [nome for nome in VARS_DE_ACESSO if not os.environ.get(nome)]
    if faltando:
        raise ConfiguracaoAusente(faltando)
    url = os.environ.get("OPENMETADATA_URL")
    return {
        "url": url.rstrip("/") if url else None,
        "token": os.environ.get("OPENMETADATA_JWT_TOKEN"),
        "servico": os.environ["OPENMETADATA_DATABASE_SERVICE"],
        "banco": os.environ["OPENMETADATA_DATABASE_NAME"],
    }


def carregar(nome: str) -> dict:
    """Lê um YAML de governança pelo nome do arquivo."""
    caminho = GOV / nome
    if not caminho.exists():
        raise SystemExit(f"Arquivo de governança ausente: {caminho}")
    return yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}


def texto(valor: str | None) -> str:
    """Normaliza descrição de YAML multilinha para uma linha só."""
    return " ".join((valor or "").split())


class ClienteOM:
    """Cliente da API do OpenMetadata ciente dos três modos de execução."""

    def __init__(self, url: str | None, token: str | None, confirmar: bool) -> None:
        self.offline = not (url and token)
        self.confirmar = confirmar and not self.offline
        self.base = f"{url}/api/v1" if url else ""
        self.sessao = requests.Session()
        if token:
            self.sessao.headers.update(
                {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
        self.criados = 0
        self.atualizados = 0
        self.conformes = 0
        self.falhas: list[str] = []

    @property
    def modo(self) -> str:
        if self.offline:
            return "offline"
        return "confirmar" if self.confirmar else "simulacao"

    def cabecalho(self) -> None:
        if self.offline:
            print(
                "MODO OFFLINE — sem URL/token no ambiente. A instância não é\n"
                "consultada nem escrita; o que segue é o que está declarado.\n"
            )
        elif not self.confirmar:
            print("MODO SIMULAÇÃO — nada é escrito. Use --confirmar para aplicar.\n")

    # ── leitura ──────────────────────────────────────────────────────────
    def existe(self, rota: str, fqn: str, campos: str | None = None) -> dict | None:
        """Busca uma entidade pelo FQN. Em offline, ninguém existe ainda."""
        if self.offline:
            return None
        parametros = {"fields": campos} if campos else None
        resposta = self.sessao.get(
            f"{self.base}/{rota}/name/{quote(fqn, safe='')}",
            params=parametros,
            timeout=60,
        )
        if not resposta.ok:
            return None
        entidade: dict = resposta.json()
        return entidade

    def listar(self, rota: str, parametros: dict) -> list[dict]:
        if self.offline:
            return []
        resposta = self.sessao.get(f"{self.base}/{rota}", params=parametros, timeout=90)
        if not resposta.ok:
            self.falha(f"listar {rota}", resposta)
            return []
        dados: list[dict] = resposta.json().get("data", [])
        return dados

    # ── escrita ──────────────────────────────────────────────────────────
    def criar(self, rota: str, corpo: dict, rotulo: str) -> dict | None:
        if not self.confirmar:
            print(f"  [{self.modo}] criaria {rota}: {rotulo}")
            self.criados += 1
            return None
        resposta = self.sessao.put(f"{self.base}/{rota}", json=corpo, timeout=90)
        if not resposta.ok:
            self.falha(f"criar {rota} {rotulo}", resposta)
            return None
        self.criados += 1
        criada: dict = resposta.json()
        return criada

    def patch(self, rota: str, ident: str, operacoes: list[dict], rotulo: str) -> None:
        """Aplica um JSON Patch. Lista vazia significa que já está conforme."""
        if not operacoes:
            self.conformes += 1
            return
        if not self.confirmar:
            campos = ", ".join(sorted({o["path"].strip("/") for o in operacoes}))
            print(f"  [{self.modo}] ajustaria {rotulo}: {campos}")
            self.atualizados += 1
            return
        resposta = self.sessao.patch(
            f"{self.base}/{rota}/{ident}",
            headers={"Content-Type": "application/json-patch+json"},
            json=operacoes,
            timeout=90,
        )
        if not resposta.ok:
            self.falha(f"ajustar {rotulo}", resposta)
            return
        self.atualizados += 1

    def montante(self, tipo: str, ident: str) -> set[str]:
        """Ids que já apontam para esta entidade.

        Sem consultar antes, cada execução reescrevia as 32 arestas do lake: a
        API aceita o PUT repetido sem duplicar, mas o sync nunca chegava a
        `atualizados=0` e não dava para distinguir "liguei agora" de "já
        estava ligado".
        """
        if self.offline:
            return set()
        resposta = self.sessao.get(
            f"{self.base}/lineage/{tipo}/{ident}",
            params={"upstreamDepth": 1, "downstreamDepth": 0},
            timeout=90,
        )
        if not resposta.ok:
            return set()
        corpo = resposta.json()
        return {no["id"] for no in corpo.get("nodes", []) if no.get("id")}

    def ligar(
        self, origem: str, tipo_origem: str, destino: str, tipo_destino: str, nota: str
    ) -> None:
        """Cria uma aresta de linhagem. É idempotente do lado da API."""
        if not self.confirmar:
            print(f"  [{self.modo}] ligaria {tipo_origem} -> {tipo_destino}")
            self.atualizados += 1
            return
        resposta = self.sessao.put(
            f"{self.base}/lineage",
            json={
                "edge": {
                    "fromEntity": {"id": origem, "type": tipo_origem},
                    "toEntity": {"id": destino, "type": tipo_destino},
                    "description": nota,
                }
            },
            timeout=90,
        )
        if not resposta.ok:
            self.falha(f"ligar {tipo_origem} a {tipo_destino}", resposta)
            return
        self.atualizados += 1

    def falha(self, contexto: str, resposta: requests.Response) -> None:
        mensagem = f"{contexto}: {self._explicar(resposta)}"
        self.falhas.append(mensagem)
        print(f"  FALHA {mensagem}")

    @staticmethod
    def _explicar(resposta: requests.Response) -> str:
        """Traduz as recusas que já custaram diagnóstico errado uma vez."""
        corpo = resposta.text
        if resposta.status_code == 403 and "DisplayName-Deny" in corpo:
            return (
                "HTTP 403 — o bot de ingestão é proibido de alterar nome de "
                "exibição (IngestionBotRole / DefaultBotPolicy). Só quem tem "
                "perfil de administrador troca, pela interface."
            )
        if resposta.status_code == 403:
            return f"HTTP 403 — permissão negada ao bot. {corpo[:160]}"
        return f"HTTP {resposta.status_code} {corpo[:200]}"

    def resumo(self) -> int:
        print(
            f"\ncriados={self.criados}  atualizados={self.atualizados}  "
            f"já conformes={self.conformes}  falhas={len(self.falhas)}"
        )
        if self.falhas:
            print("\nFalhas:")
            for mensagem in self.falhas:
                print(f"  - {mensagem}")
            return 1
        return 0


def operacoes_de_diferenca(atual: dict, desejado: dict[str, Any]) -> list[dict]:
    """Monta o JSON Patch só dos campos que realmente diferem.

    É o que faz o sync ser idempotente de verdade: rodar duas vezes seguidas
    tem de terminar com `atualizados=0`. Antes o patch era emitido sempre, e
    não havia como distinguir "mudou" de "reescrevi igual".
    """
    operacoes = []
    for campo, valor in desejado.items():
        if valor is None:
            continue
        if _equivalente(atual.get(campo), valor):
            continue
        operacoes.append({"op": "add", "path": f"/{campo}", "value": valor})
    return operacoes


def _equivalente(atual: Any, desejado: Any) -> bool:
    """Compara ignorando o que a API acrescenta por conta própria.

    A resposta do OpenMetadata devolve as referências completas (id, href,
    displayName, deleted…). Comparar o dicionário inteiro faria todo campo
    parecer diferente e o sync nunca convergiria.
    """
    if isinstance(desejado, list) and isinstance(atual, list):
        return _chaves(atual) == _chaves(desejado)
    if isinstance(desejado, dict) and isinstance(atual, dict):
        return all(atual.get(chave) == valor for chave, valor in desejado.items())
    if isinstance(desejado, str) and isinstance(atual, str):
        return mesmo_texto(atual, desejado)
    return bool(atual == desejado)


def mesmo_texto(atual: str | None, desejado: str | None) -> bool:
    """Compara texto tratando a escapada que o OpenMetadata aplica ao gravar.

    A instância grava `=`, `'` e `"` como entidade HTML (`&#61;`, `&#39;`,
    `&#34;`). Comparando cru, toda descrição que tenha um desses caracteres
    parece divergente para sempre: o sync reescreve, a instância reescapa, e na
    execução seguinte a diferença está lá de novo. Nunca converge.
    """
    return html.unescape(atual or "").strip() == html.unescape(desejado or "").strip()


def _chaves(itens: list) -> set:
    """Reduz uma lista de referências ao que a identifica."""
    reduzidas = set()
    for item in itens:
        if not isinstance(item, dict):
            reduzidas.add(item)
            continue
        # `relatedTerms` do glossário não é uma referência solta: é um
        # `TermRelation`, que embrulha a referência em `term` e acrescenta o
        # tipo da relação. Sem tratar esse caso, os dois lados da comparação
        # reduziriam a conjunto vazio e o patch nunca seria emitido.
        if isinstance(item.get("term"), dict):
            alvo = _chaves([item["term"]])
            reduzidas.add(f"{item.get('relationType', '')}:{'|'.join(sorted(alvo))}")
            continue
        for chave in ("tagFQN", "fullyQualifiedName", "id", "name"):
            if item.get(chave):
                reduzidas.add(item[chave])
                break
    return reduzidas


#: O OpenMetadata modela a relação entre termos com um tipo. Usamos o genérico:
#: os nossos vínculos são "veja também" (LCI e Funding, FIPE e FipeZap), não
#: hierarquia — nenhum é mais amplo nem sinônimo do outro.
RELACAO_ENTRE_TERMOS = "relatedTo"


def relacao_de_termo(entidade: dict) -> dict:
    """Monta o `TermRelation` que a API exige em `relatedTerms`."""
    return {
        "term": referencia(entidade, "glossaryTerm"),
        "relationType": RELACAO_ENTRE_TERMOS,
    }


def referencia(entidade: dict, tipo: str) -> dict:
    """Referência de entidade no formato que a API espera em um patch."""
    return {
        "id": entidade["id"],
        "type": tipo,
        "name": entidade["name"],
        "fullyQualifiedName": entidade["fullyQualifiedName"],
    }


def etiqueta(fqn: str, origem: str = "Classification") -> dict:
    """Etiqueta aplicada por automação, não por decisão manual na tela."""
    return {
        "tagFQN": fqn,
        "source": origem,
        "labelType": "Automated",
        "state": "Confirmed",
    }


class Proprietarios:
    """Resolve a chave declarada no YAML para a entidade do OpenMetadata.

    A chave (`mcid_data_engineering`) é o que aparece em `dominios.yml`,
    `schemas.yml`, `servicos.yml` e no `meta.governance` de cada modelo dbt. O
    mapa `proprietarios:` de `dominios.yml` diz se ela é time ou pessoa. Nenhum
    script decide dono por conta própria.
    """

    def __init__(self, om: ClienteOM, dominios: dict) -> None:
        self.om = om
        self.mapa = dominios.get("proprietarios") or {}
        self.cache: dict[str, list[dict] | None] = {}

    def resolver(self, chave: str | None) -> list[dict] | None:
        """Devolve `owners` pronto para o patch, ou None se não der para resolver."""
        if not chave:
            return None
        if chave in self.cache:
            return self.cache[chave]
        declarado = self.mapa.get(chave)
        if not declarado:
            print(f"  proprietário '{chave}' não está em dominios.yml; ignorado")
            self.cache[chave] = None
            return None
        tipo = declarado.get("type", "team")
        rota = "teams" if tipo == "team" else "users"
        entidade = self.om.existe(rota, declarado["name"])
        if not entidade:
            if not self.om.offline:
                print(
                    f"  proprietário '{declarado['name']}' não existe como {tipo} "
                    "na instância; dono não será aplicado"
                )
            self.cache[chave] = None
            return None
        self.cache[chave] = [referencia(entidade, tipo)]
        return self.cache[chave]
