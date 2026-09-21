"""
Testes unitários para airflow_lappis/plugins/cliente_infomoney.py

"""

import sys
import os
from http import HTTPStatus

import httpx
import pytest
import respx

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "airflow_lappis", "plugins")
)

from cliente_infomoney import ClienteInfomoney  # noqa: E402

BASE_URL = "https://www.alphavantage.co"
API_KEY = "fake-api-key"
SYMBOL = "PETR4"


@pytest.fixture
def cliente() -> ClienteInfomoney:
    return ClienteInfomoney(api_key=API_KEY)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Evita que os testes de retry/erro fiquem lentos de verdade."""
    monkeypatch.setattr("cliente_base.time.sleep", lambda *_args, **_kwargs: None)


# ---------------------------------------------------------------------------
# Casos de sucesso
# ---------------------------------------------------------------------------

@respx.mock
def test_get_daily_series_sucesso_retorna_series_temporal(cliente):
    payload = {
        "Meta Data": {"2. Symbol": SYMBOL},
        "Time Series (Daily)": {
            "2024-01-02": {
                "1. open": "10.00",
                "2. high": "10.50",
                "3. low": "9.80",
                "4. close": "10.20",
                "5. volume": "1000",
            },
            "2024-01-01": {
                "1. open": "9.50",
                "2. high": "9.90",
                "3. low": "9.40",
                "4. close": "9.80",
                "5. volume": "800",
            },
        },
    }

    route = respx.get(f"{BASE_URL}/query").mock(
        return_value=httpx.Response(200, json=payload)
    )

    resultado = cliente.get_daily_series(SYMBOL)

    assert route.called
    assert route.call_count == 1
    assert resultado == payload["Time Series (Daily)"]


@respx.mock
def test_get_daily_series_envia_parametros_corretos(cliente):
    payload = {"Time Series (Daily)": {}}
    route = respx.get(f"{BASE_URL}/query").mock(
        return_value=httpx.Response(200, json=payload)
    )

    cliente.get_daily_series(SYMBOL)

    request_enviada = route.calls.last.request
    query = httpx.QueryParams(request_enviada.url.query)
    assert query["function"] == "TIME_SERIES_DAILY"
    assert query["symbol"] == SYMBOL
    assert query["apikey"] == API_KEY
    assert query["outputsize"] == "compact"


@respx.mock
def test_get_daily_series_retorna_lista_vazia_quando_payload_vazio(cliente):
    respx.get(f"{BASE_URL}/query").mock(return_value=httpx.Response(200, json={}))

    resultado = cliente.get_daily_series(SYMBOL)

    assert resultado == []


# ---------------------------------------------------------------------------
# Casos de erro / limite de API
# ---------------------------------------------------------------------------

@respx.mock
def test_get_daily_series_retorna_vazio_quando_limite_de_api_atingido(cliente):
    """
    A API Alpha Vantage retorna HTTP 200 com uma mensagem de "Note"/"Information"
    em vez da chave "Time Series (Daily)" quando o limite de chamadas é
    atingido. Nesse caso, get_daily_series deve retornar [].
    """
    payload = {
        "Information": "Thank you for using Alpha Vantage! "
        "Our standard API call frequency is 25 requests per day."
    }
    respx.get(f"{BASE_URL}/query").mock(return_value=httpx.Response(200, json=payload))

    resultado = cliente.get_daily_series(SYMBOL)

    assert resultado == []


@respx.mock
def test_get_daily_series_retorna_vazio_quando_symbol_invalido(cliente):
    payload = {
        "Error Message": "Invalid API call. Please retry or visit the "
        "documentation."
    }
    respx.get(f"{BASE_URL}/query").mock(return_value=httpx.Response(200, json=payload))

    resultado = cliente.get_daily_series("SYMBOL_INEXISTENTE")

    assert resultado == []


@respx.mock
def test_get_daily_series_propaga_excecao_apos_esgotar_retries_em_erro_http(cliente):
    """
    ClienteBase.request relança exceção quando todas as tentativas falham
    (ex.: erro 500 persistente). get_daily_series não trata essa exceção,
    então o comportamento esperado é a propagação.
    """
    route = respx.get(f"{BASE_URL}/query").mock(
        return_value=httpx.Response(500, json={"error": "internal error"})
    )

    with pytest.raises(Exception, match="API failed after the maximum number of attempts"):
        cliente.get_daily_series(SYMBOL)

    assert route.call_count == ClienteInfomoney.DEFAULT_MAX_RETRIES


@respx.mock
def test_get_daily_series_sucesso_apos_falha_transitoria(cliente):
    """
    Simula uma falha (500) na primeira tentativa e sucesso na segunda,
    validando a lógica de retry herdada de ClienteBase.
    """
    payload = {"Time Series (Daily)": {"2024-01-01": {"4. close": "10.00"}}}

    route = respx.get(f"{BASE_URL}/query").mock(
        side_effect=[
            httpx.Response(500, json={"error": "temp"}),
            httpx.Response(200, json=payload),
        ]
    )

    resultado = cliente.get_daily_series(SYMBOL)

    assert route.call_count == 2
    assert resultado == payload["Time Series (Daily)"]


@respx.mock
def test_get_daily_series_propaga_excecao_em_timeout(cliente):
    respx.get(f"{BASE_URL}/query").mock(side_effect=httpx.TimeoutException("timeout"))

    with pytest.raises(Exception, match="API failed after the maximum number of attempts"):
        cliente.get_daily_series(SYMBOL)


# ---------------------------------------------------------------------------
# Inicialização do cliente
# ---------------------------------------------------------------------------

def test_init_configura_base_url_e_api_key():
    cliente = ClienteInfomoney(api_key="minha-chave")

    assert cliente.api_key == "minha-chave"
    assert cliente.base_url == BASE_URL
    assert str(cliente.client.base_url) == BASE_URL