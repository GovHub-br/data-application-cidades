"""Testes unitários de ClienteIBGE.

Abordagem: caixa-branca primeiro (CB), complementada por caixa-preta (CP).

IDs de rastreabilidade
  Método get_dados_agregados
    D1  decisão composta: classificacao_id is not None and categoria is not None
    D2  decisão composta: status == OK and isinstance(data, list)
  Método transformar_resposta
    L1  for variavel      L2  for resultado     L3  for classificacoes
    L4  for categoria     L5  for series        L6  for periodo/valor
    T1..T4  ternários de classificacao_id/nome e categoria_id/nome
    D3  decisão composta: valor and valor != "..."
"""

import http
import logging
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "plugins"))

from cliente_ibge import ClienteIBGE  # noqa: E402

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
    resultado = {
        "series": [{"localidade": {"id": "1", "nome": "Brasil"}, "serie": valores}]
    }
    if classificacoes is not None:
        resultado["classificacoes"] = classificacoes
    return {
        "id": id_,
        "variavel": nome,
        "unidade": unidade,
        "resultados": [resultado],
    }


class TestInit:
    def test_cb_init_repassa_base_url_e_registra_log(self, caplog):
        with patch("cliente_base.ClienteBase.__init__", return_value=None) as base:
            with caplog.at_level(logging.INFO):
                ClienteIBGE()
        base.assert_called_once_with(base_url=ClienteIBGE.BASE_URL)
        assert ClienteIBGE.BASE_URL in caplog.text


class TestGetDadosAgregadosCaixaBranca:
    def test_cb_d1_ambos_informados_adiciona_classificacao(self, cliente):
        _, req = chamar(cliente, classificacao_id=11255, categoria=90694)
        assert req.call_args.kwargs["params"]["classificacao"] == "11255[90694]"

    def test_cb_d1_mcdc_classificacao_none_nao_adiciona(self, cliente):
        _, req = chamar(cliente, classificacao_id=None, categoria=90694)
        assert "classificacao" not in req.call_args.kwargs["params"]

    def test_cb_d1_mcdc_categoria_none_nao_adiciona(self, cliente):
        _, req = chamar(cliente, classificacao_id=11255, categoria=None)
        assert "classificacao" not in req.call_args.kwargs["params"]

    def test_cb_d1_ambos_none_nao_adiciona(self, cliente):
        _, req = chamar(cliente)
        assert "classificacao" not in req.call_args.kwargs["params"]

    def test_cb_d2_ok_e_lista_retorna_dados(self, cliente):
        dados = [{"id": 1}]
        resultado, _ = chamar(cliente, retorno=(OK, dados))
        assert resultado is dados

    def test_cb_d2_mcdc_status_diferente_de_ok_retorna_none(self, cliente):
        resultado, _ = chamar(cliente, retorno=(http.HTTPStatus.NOT_FOUND, [{"id": 1}]))
        assert resultado is None

    def test_cb_d2_mcdc_dados_nao_lista_retorna_none(self, cliente):
        resultado, _ = chamar(cliente, retorno=(OK, {"erro": "x"}))
        assert resultado is None

    def test_cb_d2_ambos_falsos_retorna_none(self, cliente):
        resultado, _ = chamar(
            cliente, retorno=(http.HTTPStatus.INTERNAL_SERVER_ERROR, None)
        )
        assert resultado is None

    def test_cb_log_info_no_sucesso(self, cliente, caplog):
        with caplog.at_level(logging.INFO):
            chamar(cliente, retorno=(OK, []))
        assert "Successfully fetched" in caplog.text

    def test_cb_log_warning_na_falha_inclui_status(self, cliente, caplog):
        with caplog.at_level(logging.WARNING):
            chamar(cliente, retorno=(http.HTTPStatus.BAD_REQUEST, None))
        assert "Failed to fetch" in caplog.text
        assert "400" in caplog.text


class TestGetDadosAgregadosCaixaPreta:
    def test_cp_valores_padrao_montam_path_e_params(self, cliente):
        _, req = chamar(cliente)
        assert req.call_args.args == (
            http.HTTPMethod.GET,
            "/agregados/5932/periodos/-20/variaveis/6564|6563",
        )
        assert req.call_args.kwargs["params"] == {"localidades": "N1[1]"}

    def test_cp_parametros_customizados(self, cliente):
        _, req = chamar(
            cliente,
            agregado=2296,
            variaveis="1",
            periodos="202401|202402",
            nivel="N3",
            localidade="53",
        )
        assert (
            req.call_args.args[1] == "/agregados/2296/periodos/202401|202402/variaveis/1"
        )
        assert req.call_args.kwargs["params"]["localidades"] == "N3[53]"

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
    def test_cp_particionamento_categoria_normaliza_separador(
        self, cliente, categoria, esperado
    ):
        _, req = chamar(cliente, classificacao_id=11255, categoria=categoria)
        assert req.call_args.kwargs["params"]["classificacao"] == esperado

    def test_cp_classificacao_id_como_string(self, cliente):
        _, req = chamar(cliente, classificacao_id="11255", categoria="1|2")
        assert req.call_args.kwargs["params"]["classificacao"] == "11255[1,2]"

    @pytest.mark.parametrize(
        "status",
        [201, 204, 301, 400, 404, 500],
    )
    def test_cp_valor_limite_status_vizinhos_de_200_retornam_none(self, cliente, status):
        resultado, _ = chamar(cliente, retorno=(status, [{"id": 1}]))
        assert resultado is None

    def test_cp_lista_vazia_com_ok_e_retornada(self, cliente):
        resultado, _ = chamar(cliente, retorno=(OK, []))
        assert resultado == []


class TestTransformarRespostaCaixaBranca:
    def test_cb_l1_lista_vazia_retorna_vazio(self):
        assert ClienteIBGE.transformar_resposta([]) == []

    def test_cb_l2_variavel_sem_resultados_retorna_vazio(self):
        dados = [{"id": 1, "variavel": "PIB", "unidade": "%", "resultados": []}]
        assert ClienteIBGE.transformar_resposta(dados) == []

    def test_cb_l5_resultado_sem_series_retorna_vazio(self):
        dados = [
            {"id": 1, "variavel": "PIB", "unidade": "%", "resultados": [{"series": []}]}
        ]
        assert ClienteIBGE.transformar_resposta(dados) == []

    def test_cb_l6_serie_vazia_retorna_vazio(self):
        assert ClienteIBGE.transformar_resposta([variavel({})]) == []

    def test_cb_l3_t1_t4_sem_chave_classificacoes_usa_padrao(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"})])[0]
        assert reg["classificacao_id"] == "0"
        assert reg["classificacao_nome"] == ""
        assert reg["categoria_id"] == "0"
        assert reg["categoria_nome"] == ""

    def test_cb_l3_classificacoes_lista_vazia_usa_padrao(self):
        reg = ClienteIBGE.transformar_resposta(
            [variavel({"202401": "1"}, classificacoes=[])]
        )[0]
        assert reg["classificacao_id"] == "0"
        assert reg["categoria_id"] == "0"

    def test_cb_l3_l4_uma_classificacao_com_categoria(self):
        cls = [{"id": "11255", "nome": "Setor", "categoria": {"90694": "Total"}}]
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
        assert reg["classificacao_id"] == "11255"
        assert reg["classificacao_nome"] == "Setor"
        assert reg["categoria_id"] == "90694"
        assert reg["categoria_nome"] == "Total"

    def test_cb_l3_l4_varias_classificacoes_e_categorias_usam_separadores_distintos(self):
        cls = [
            {"id": "1", "nome": "A", "categoria": {"10": "X", "11": "Y"}},
            {"id": "2", "nome": "B", "categoria": {"20": "Z"}},
        ]
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
        assert reg["classificacao_id"] == "1|2"
        assert reg["classificacao_nome"] == "A | B"
        assert reg["categoria_id"] == "10|11|20"
        assert reg["categoria_nome"] == "X | Y | Z"

    def test_cb_t3_classificacao_sem_chave_categoria(self):
        cls = [{"id": "11255", "nome": "Setor"}]
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
        assert reg["classificacao_id"] == "11255"
        assert reg["categoria_id"] == "0"
        assert reg["categoria_nome"] == ""

    def test_cb_t3_classificacao_com_categoria_vazia(self):
        cls = [{"id": "11255", "nome": "Setor", "categoria": {}}]
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "1"}, cls)])[0]
        assert reg["classificacao_id"] == "11255"
        assert reg["categoria_id"] == "0"

    def test_cb_d3_valor_valido_e_mantido(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "123.4"})])[0]
        assert reg["valor"] == "123.4"

    def test_cb_d3_mcdc_valor_falsy_vira_none(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": ""})])[0]
        assert reg["valor"] is None

    def test_cb_d3_mcdc_valor_reticencias_vira_none(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "..."})])[0]
        assert reg["valor"] is None

    def test_cb_d3_valor_none_vira_none(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": None})])[0]
        assert reg["valor"] is None


class TestTransformarRespostaCaixaPreta:
    def test_cp_campos_do_registro(self):
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

    def test_cp_ids_numericos_viram_string(self):
        dados = [variavel({202401: "5"}, id_=6564)]
        dados[0]["resultados"][0]["series"][0]["localidade"]["id"] = 1
        reg = ClienteIBGE.transformar_resposta(dados)[0]
        assert reg["variavel_id"] == "6564"
        assert reg["localidade_id"] == "1"
        assert reg["periodo"] == "202401"

    def test_cp_dt_ingest_e_iso_valido(self):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": "5"})])[0]
        assert isinstance(datetime.fromisoformat(reg["dt_ingest"]), datetime)

    def test_cp_multiplos_periodos_gera_um_registro_por_periodo(self):
        regs = ClienteIBGE.transformar_resposta(
            [variavel({"202401": "1", "202402": "2", "202403": "3"})]
        )
        assert [r["periodo"] for r in regs] == ["202401", "202402", "202403"]
        assert [r["valor"] for r in regs] == ["1", "2", "3"]

    def test_cp_multiplas_variaveis_series_e_localidades(self):
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

    @pytest.mark.parametrize(
        "valor, esperado",
        [("-", "-"), ("X", "X"), ("..", ".."), ("0", "0"), (0, None)],
        ids=["hifen", "X", "dois_pontos", "zero_str", "zero_int"],
    )
    def test_cp_codigos_especiais_do_ibge_so_reticencias_e_tratado(self, valor, esperado):
        reg = ClienteIBGE.transformar_resposta([variavel({"202401": valor})])[0]
        assert reg["valor"] == esperado

    @pytest.mark.parametrize("chave", ["id", "variavel", "unidade", "resultados"])
    def test_cp_variavel_sem_chave_obrigatoria_levanta_keyerror(self, chave):
        dados = variavel({"202401": "1"})
        del dados[chave]
        with pytest.raises(KeyError):
            ClienteIBGE.transformar_resposta([dados])

    def test_cp_resultado_sem_series_levanta_keyerror(self):
        dados = variavel({"202401": "1"})
        del dados["resultados"][0]["series"]
        with pytest.raises(KeyError):
            ClienteIBGE.transformar_resposta([dados])

    def test_cp_serie_sem_localidade_levanta_keyerror(self):
        dados = variavel({"202401": "1"})
        del dados["resultados"][0]["series"][0]["localidade"]
        with pytest.raises(KeyError):
            ClienteIBGE.transformar_resposta([dados])
