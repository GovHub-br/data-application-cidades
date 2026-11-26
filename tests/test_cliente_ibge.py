"""Testes de ClienteIBGE: __init__, get_dados_agregados e transformar_resposta."""

import http
import logging
from datetime import datetime
from unittest.mock import patch

import pytest

from cliente_ibge import ClienteIBGE

OK = http.HTTPStatus.OK


@pytest.fixture
def cliente():
    return ClienteIBGE.__new__(ClienteIBGE)


def chamar(cliente, retorno=(OK, []), **kwargs):
    kwargs.setdefault("agregado", 5932)
    kwargs.setdefault("variaveis", "6564|6563")
    with patch.object(ClienteIBGE, "request", return_value=retorno) as req:
        resultado = cliente.get_dados_agregados(**kwargs)
    return resultado, req


def variavel(valores, classificacoes=None, id_=1, nome="PIB", unidade="%"):
    resultado = {"series": [{"localidade": {"id": "1", "nome": "Brasil"}, "serie": valores}]}
    if classificacoes is not None:
        resultado["classificacoes"] = classificacoes
    return {
        "id": id_,
        "variavel": nome,
        "unidade": unidade,
        "resultados": [resultado],
    }


# caixa-branca | cobertura de instruções do __init__
def test_init_repassa_base_url_e_registra_log(caplog):
    with patch("cliente_base.ClienteBase.__init__", return_value=None) as base:
        with caplog.at_level(logging.INFO):
            ClienteIBGE()
    base.assert_called_once_with(base_url=ClienteIBGE.BASE_URL)
    assert ClienteIBGE.BASE_URL in caplog.text


# caixa-branca | MC/DC de D1: as duas condições verdadeiras
def test_classificacao_e_categoria_informadas_adicionam_param_classificacao(cliente):
    _, req = chamar(cliente, classificacao_id=11255, categoria=90694)
    assert req.call_args.kwargs["params"]["classificacao"] == "11255[90694]"


# caixa-branca | MC/DC de D1: classificacao_id None
def test_classificacao_id_none_nao_adiciona_param_classificacao(cliente):
    _, req = chamar(cliente, classificacao_id=None, categoria=90694)
    assert "classificacao" not in req.call_args.kwargs["params"]


# caixa-branca | MC/DC de D1: categoria None
def test_categoria_none_nao_adiciona_param_classificacao(cliente):
    _, req = chamar(cliente, classificacao_id=11255, categoria=None)
    assert "classificacao" not in req.call_args.kwargs["params"]


# caixa-branca | cobertura de condições de D1: as duas condições falsas
def test_sem_classificacao_nem_categoria_nao_adiciona_param_classificacao(cliente):
    _, req = chamar(cliente)
    assert "classificacao" not in req.call_args.kwargs["params"]


# caixa-branca | MC/DC de D2: as duas condições verdadeiras
def test_status_ok_com_lista_retorna_os_dados(cliente):
    dados = [{"id": 1}]
    resultado, _ = chamar(cliente, retorno=(OK, dados))
    assert resultado is dados


# caixa-branca | MC/DC de D2: status diferente de OK
def test_status_diferente_de_ok_retorna_none(cliente):
    resultado, _ = chamar(cliente, retorno=(http.HTTPStatus.NOT_FOUND, [{"id": 1}]))
    assert resultado is None


# caixa-branca | MC/DC de D2: dados que não são lista
def test_resposta_que_nao_e_lista_retorna_none(cliente):
    resultado, _ = chamar(cliente, retorno=(OK, {"erro": "x"}))
    assert resultado is None


# caixa-branca | cobertura de condições de D2: as duas condições falsas
def test_status_de_erro_sem_dados_retorna_none(cliente):
    resultado, _ = chamar(cliente, retorno=(http.HTTPStatus.INTERNAL_SERVER_ERROR, None))
    assert resultado is None


# caixa-branca | cobertura de instruções: ramo verdadeiro de D2
def test_sucesso_registra_log_info(cliente, caplog):
    with caplog.at_level(logging.INFO):
        chamar(cliente, retorno=(OK, []))
    assert "Successfully fetched" in caplog.text


# caixa-branca | cobertura de instruções: ramo falso de D2
def test_falha_registra_warning_com_o_status(cliente, caplog):
    with caplog.at_level(logging.WARNING):
        chamar(cliente, retorno=(http.HTTPStatus.BAD_REQUEST, None))
    assert "Failed to fetch" in caplog.text
    assert "400" in caplog.text


# caixa-preta | classe de equivalência: parâmetros padrão
def test_valores_padrao_montam_path_e_params(cliente):
    _, req = chamar(cliente)
    assert req.call_args.args == (
        http.HTTPMethod.GET,
        "/agregados/5932/periodos/-20/variaveis/6564|6563",
    )
    assert req.call_args.kwargs["params"] == {"localidades": "N1[1]"}


# caixa-preta | classe de equivalência: parâmetros informados
def test_parametros_customizados_montam_path_e_localidade(cliente):
    _, req = chamar(
        cliente,
        agregado=2296,
        variaveis="1",
        periodos="202401|202402",
        nivel="N3",
        localidade="53",
    )
    assert req.call_args.args[1] == "/agregados/2296/periodos/202401|202402/variaveis/1"
    assert req.call_args.kwargs["params"]["localidades"] == "N3[53]"


# caixa-preta | partição de equivalência: formato da categoria
@pytest.mark.parametrize(
    "categoria, esperado",
    [
        ("47946|47949", "11255[47946,47949]"),
        ("47946,47949", "11255[47946,47949]"),
        ("47946", "11255[47946]"),
        (47946, "11255[47946]"),
        ("1|2|3", "11255[1,2,3]"),
    ],
    ids=["pipe", "virgula", "unica_str", "unica_int", "tres_pipes"],
)
def test_categoria_normaliza_separador_para_virgula(cliente, categoria, esperado):
    _, req = chamar(cliente, classificacao_id=11255, categoria=categoria)
    assert req.call_args.kwargs["params"]["classificacao"] == esperado


# caixa-preta | classe de equivalência: classificacao_id como string
def test_classificacao_id_aceita_string(cliente):
    _, req = chamar(cliente, classificacao_id="11255", categoria="1|2")
    assert req.call_args.kwargs["params"]["classificacao"] == "11255[1,2]"


# caixa-preta | valor limite: status HTTP vizinhos de 200
@pytest.mark.parametrize(
    "status",
    [201, 204, 301, 400, 404, 500],
)
def test_status_vizinhos_de_200_retornam_none(cliente, status):
    resultado, _ = chamar(cliente, retorno=(status, [{"id": 1}]))
    assert resultado is None


# caixa-preta | valor limite: lista vazia com status OK
def test_lista_vazia_com_status_ok_e_retornada(cliente):
    resultado, _ = chamar(cliente, retorno=(OK, []))
    assert resultado == []


# caixa-branca | cobertura de laço L1: zero iterações
def test_transformar_lista_vazia_retorna_vazio():
    assert ClienteIBGE.transformar_resposta([]) == []


# caixa-branca | cobertura de laço L2: zero iterações
def test_variavel_sem_resultados_nao_gera_registros():
    dados = [{"id": 1, "variavel": "PIB", "unidade": "%", "resultados": []}]
    assert ClienteIBGE.transformar_resposta(dados) == []


# caixa-branca | cobertura de laço L5: zero iterações
def test_lista_de_series_vazia_nao_gera_registros():
    dados = [{"id": 1, "variavel": "PIB", "unidade": "%", "resultados": [{"series": []}]}]
    assert ClienteIBGE.transformar_resposta(dados) == []


# caixa-branca | cobertura de laço L6: zero iterações
def test_serie_sem_periodos_nao_gera_registros():
    assert ClienteIBGE.transformar_resposta([variavel({})]) == []


# caixa-branca | laço L3 sem iterações e ramo falso de T1..T4: chave classificacoes ausente
def test_sem_chave_classificacoes_usa_valores_padrao():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"})])[0]
    assert reg["classificacao_id"] == "0"
    assert reg["classificacao_nome"] == ""
    assert reg["categoria_id"] == "0"
    assert reg["categoria_nome"] == ""


# caixa-branca | laço L3 sem iterações: lista classificacoes vazia
def test_classificacoes_vazia_usa_valores_padrao():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, classificacoes=[])])[0]
    assert reg["classificacao_id"] == "0"
    assert reg["categoria_id"] == "0"


# caixa-branca | laços L3 e L4 com uma iteração e ramo verdadeiro de T1..T4
def test_uma_classificacao_com_uma_categoria_preenche_ids_e_nomes():
    cls = [{"id": "11255", "nome": "Setor", "categoria": {"90694": "Total"}}]
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
    assert reg["classificacao_id"] == "11255"
    assert reg["classificacao_nome"] == "Setor"
    assert reg["categoria_id"] == "90694"
    assert reg["categoria_nome"] == "Total"


# caixa-branca | laços L3 e L4 com várias iterações
def test_varias_classificacoes_e_categorias_usam_separadores_distintos():
    cls = [
        {"id": "1", "nome": "A", "categoria": {"10": "X", "11": "Y"}},
        {"id": "2", "nome": "B", "categoria": {"20": "Z"}},
    ]
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
    assert reg["classificacao_id"] == "1|2"
    assert reg["classificacao_nome"] == "A | B"
    assert reg["categoria_id"] == "10|11|20"
    assert reg["categoria_nome"] == "X | Y | Z"


# caixa-branca | ramo falso de T3: classificação sem chave categoria
def test_classificacao_sem_chave_categoria_usa_categoria_padrao():
    cls = [{"id": "11255", "nome": "Setor"}]
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
    assert reg["classificacao_id"] == "11255"
    assert reg["categoria_id"] == "0"
    assert reg["categoria_nome"] == ""


# caixa-branca | ramo falso de T3: classificação com categoria vazia
def test_classificacao_com_categoria_vazia_usa_categoria_padrao():
    cls = [{"id": "11255", "nome": "Setor", "categoria": {}}]
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
    assert reg["classificacao_id"] == "11255"
    assert reg["categoria_id"] == "0"


# caixa-branca | MC/DC de D3: as duas condições verdadeiras
def test_valor_valido_e_mantido():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "123.4"})])[0]
    assert reg["valor"] == "123.4"


# caixa-branca | MC/DC de D3: valor falsy
def test_valor_string_vazia_vira_none():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": ""})])[0]
    assert reg["valor"] is None


# caixa-branca | MC/DC de D3: valor igual a "..."
def test_valor_reticencias_vira_none():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "..."})])[0]
    assert reg["valor"] is None


# caixa-branca | cobertura de condições de D3: valor None
def test_valor_none_vira_none():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": None})])[0]
    assert reg["valor"] is None


# caixa-preta | classe de equivalência: registro válido com todos os campos
def test_registro_tem_todos_os_campos_esperados():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "5"}, id_=6564)])[0]
    assert set(reg) == {
        "variavel_id",
        "variavel_nome",
        "localidade_id",
        "localidade_nome",
        "classificacao_id",
        "classificacao_nome",
        "categoria_id",
        "categoria_nome",
        "unidade",
        "periodo",
        "valor",
        "dt_ingest",
    }
    assert reg["variavel_id"] == "6564"
    assert reg["variavel_nome"] == "PIB"
    assert reg["unidade"] == "%"
    assert reg["localidade_id"] == "1"
    assert reg["localidade_nome"] == "Brasil"
    assert reg["periodo"] == "202401"


# caixa-preta | classe de equivalência: ids numéricos
def test_ids_numericos_viram_string():
    dados = [variavel({202401: "5"}, id_=6564)]
    dados[0]["resultados"][0]["series"][0]["localidade"]["id"] = 1
    reg = ClienteIBGE.transformar_resposta(dados)[0]
    assert reg["variavel_id"] == "6564"
    assert reg["localidade_id"] == "1"
    assert reg["periodo"] == "202401"


# caixa-preta | formato da saída: dt_ingest em ISO 8601
def test_dt_ingest_e_data_iso_valida():
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": "5"})])[0]
    assert isinstance(datetime.fromisoformat(reg["dt_ingest"]), datetime)


# caixa-preta | classe de equivalência: vários períodos
def test_gera_um_registro_por_periodo():
    regs = ClienteIBGE.transformar_resposta(
        [variavel({"202401": "1", "202402": "2", "202403": "3"})]
    )
    assert [r["periodo"] for r in regs] == ["202401", "202402", "202403"]
    assert [r["valor"] for r in regs] == ["1", "2", "3"]


# caixa-preta | classe de equivalência: várias variáveis, séries e localidades
def test_varias_variaveis_e_localidades_geram_um_registro_por_combinacao():
    v1 = variavel({"202401": "1", "202402": "2"}, id_=1)
    v2 = variavel({"202401": "9"}, id_=2)
    v2["resultados"][0]["series"].append(
        {"localidade": {"id": "2", "nome": "Norte"}, "serie": {"202401": "7"}}
    )
    regs = ClienteIBGE.transformar_resposta([v1, v2])
    assert len(regs) == 2 + 1 + 1
    assert {(r["variavel_id"], r["localidade_id"]) for r in regs} == {
        ("1", "1"),
        ("2", "1"),
        ("2", "2"),
    }


# caixa-preta | classe de equivalência: códigos especiais do IBGE
# caracterização: comportamento atual do código, pode mudar
@pytest.mark.parametrize(
    "valor, esperado",
    [("-", "-"), ("X", "X"), ("..", ".."), ("0", "0"), (0, None)],
    ids=["hifen", "X", "dois_pontos", "zero_str", "zero_int"],
)
def test_entre_codigos_especiais_do_ibge_so_reticencias_e_tratado(valor, esperado):
    reg = ClienteIBGE.transformar_resposta([variavel({"202401": valor})])[0]
    assert reg["valor"] == esperado


# caixa-preta | classe de equivalência inválida: variável sem chave obrigatória
# caracterização: comportamento atual do código, pode mudar
@pytest.mark.parametrize("chave", ["id", "variavel", "unidade", "resultados"])
def test_variavel_sem_chave_obrigatoria_levanta_keyerror(chave):
    dados = variavel({"202401": "1"})
    del dados[chave]
    with pytest.raises(KeyError):
        ClienteIBGE.transformar_resposta([dados])


# caixa-preta | classe de equivalência inválida: resultado sem series
# caracterização: comportamento atual do código, pode mudar
def test_resultado_sem_chave_series_levanta_keyerror():
    dados = variavel({"202401": "1"})
    del dados["resultados"][0]["series"]
    with pytest.raises(KeyError):
        ClienteIBGE.transformar_resposta([dados])


# caixa-preta | classe de equivalência inválida: série sem localidade
# caracterização: comportamento atual do código, pode mudar
def test_serie_sem_chave_localidade_levanta_keyerror():
    dados = variavel({"202401": "1"})
    del dados["resultados"][0]["series"][0]["localidade"]
    with pytest.raises(KeyError):
        ClienteIBGE.transformar_resposta([dados])
