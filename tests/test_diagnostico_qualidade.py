"""main() do diagnóstico de qualidade: as seções saem na ordem, com os argumentos certos."""

import sys

import pytest

import diagnostico_qualidade as dq


class _Conexao:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self


def _rodar(monkeypatch, capsys, *argv):
    monkeypatch.setattr(dq, "conectar", _Conexao)
    for nome in [n for n in dir(dq) if n.startswith("secao_")]:
        monkeypatch.setattr(
            dq, nome, lambda _cur, *args, _n=nome: [f"{_n}{args!r}", "linha 2", "linha 3"]
        )
    monkeypatch.setattr(sys, "argv", ["diagnostico_qualidade.py", *argv])
    dq.main()
    return capsys.readouterr().out


def test_main_imprime_as_secoes_na_ordem(monkeypatch, capsys):
    saida = _rodar(monkeypatch, capsys, "--limiar-nulo", "40", "--tolerancia-divergencia", "2")
    titulos = [linha for linha in saida.splitlines() if linha.startswith("#")]
    assert titulos == [
        "# Diagnóstico de qualidade — domínio rural",
        "## 1. Mojibake residual (bronze)",
        f"## 2a. Mojibake residual — silver ({dq.DOMINIOS['rural']['schema_silver']})",
        f"## 2b. Mojibake residual — gold ({dq.DOMINIOS['rural']['schema_gold']})",
        "## 3. Colunas com >= 40% de nulo/vazio",
        "## 4. Duplicidade na chave natural",
        "## 5. Cobertura dos joins",
        "## 6. Valores reais dos campos de corte",
        "## 7. Divergência entre fontes da mesma medida",
        "## 8. Atualidade e procedência das medições",
    ]
    # o cabeçalho da tabela de nulos da gold não se repete: só a da silver tem
    assert saida.count("secao_nulos") == 1 and saida.count("linha 2") == 9
    assert "2.0)" in saida  # a tolerância chega à seção de divergência


def test_main_e_estavel(monkeypatch, capsys):
    """A saída inteira, com os argumentos de cada seção, não muda na refatoração."""
    saida = _rodar(monkeypatch, capsys, "--schema-bronze", "outro")
    assert "secao_mojibake('outro'" in saida
    assert saida == GOLDEN


GOLDEN = "# Diagnóstico de qualidade — domínio rural\n\n## 1. Mojibake residual (bronze)\n\nsecao_mojibake('outro', ['bronze_shpt_monit_cad_pj_rural_mensal', 'bronze_shpt_monit_cadastro_pf_rural_mensal', 'bronze_shpt_monit_mov_obra_rural_mensal', 'bronze_shpt_monit_mov_financ_rural_mensal', 'bronze_shpt_dados_prioritarios_disponibilizados_snh_empreendimentos', 'bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa', 'bronze_sftp_snh_pmcmv_dados_prioritarios_af_bb', 'bronze_sftp_int065_pnhr_caixa_empreendimentos', 'bronze_sftp_int057_pnhr_bb_empreendimentos', 'bronze_sftp_int055_liberacoes_caixa_bb', 'bronze_shpt_base_trabalho_social_pnhr_rural_caixa', 'bronze_shpt_base_trabalho_social_pnhr_bb'])\nlinha 2\nlinha 3\n\n## 2a. Mojibake residual — silver (prata)\n\nsecao_mojibake('prata', ['prata_rural_cadastro_pf', 'prata_rural_cadastro_pj', 'prata_rural_empreendimento', 'prata_rural_financeiro_mensal', 'prata_rural_obra_mensal', 'prata_rural_pnhr_bb', 'prata_rural_pnhr_caixa', 'prata_rural_pnhr_liberacoes', 'prata_rural_prioritarios_bb', 'prata_rural_prioritarios_caixa', 'prata_rural_prioritarios_snh', 'prata_rural_trabalho_social_bb', 'prata_rural_trabalho_social_caixa'])\nlinha 2\nlinha 3\n\n## 2b. Mojibake residual — gold (ouro)\n\nsecao_mojibake('ouro', ['ouro_rural_evolucao_financeira', 'ouro_rural_execucao_fisica_financeira', 'ouro_rural_ficha_empreendimento', 'ouro_rural_ficha_trabalho_social', 'ouro_rural_infraestrutura_agua_saneamento', 'ouro_rural_mapa_nacional', 'ouro_rural_panorama_estadual', 'ouro_rural_perfil_beneficiarios', 'ouro_rural_resumo_gerencial'])\nlinha 2\nlinha 3\n\n## 3. Colunas com >= 50% de nulo/vazio\n\nsecao_nulos('prata', ['prata_rural_cadastro_pf', 'prata_rural_cadastro_pj', 'prata_rural_empreendimento', 'prata_rural_financeiro_mensal', 'prata_rural_obra_mensal', 'prata_rural_pnhr_bb', 'prata_rural_pnhr_caixa', 'prata_rural_pnhr_liberacoes', 'prata_rural_prioritarios_bb', 'prata_rural_prioritarios_caixa', 'prata_rural_prioritarios_snh', 'prata_rural_trabalho_social_bb', 'prata_rural_trabalho_social_caixa'], 50.0)\nlinha 2\nlinha 3\nlinha 3\n\n## 4. Duplicidade na chave natural\n\nsecao_duplicidade({'prata_rural_empreendimento': 'prata', 'prata_rural_prioritarios_snh': 'prata', 'prata_rural_prioritarios_caixa': 'prata', 'prata_rural_prioritarios_bb': 'prata', 'prata_rural_cadastro_pj': 'prata', 'prata_rural_cadastro_pf': 'prata', 'prata_rural_pnhr_caixa': 'prata', 'prata_rural_pnhr_bb': 'prata', 'prata_rural_pnhr_liberacoes': 'prata', 'prata_rural_trabalho_social_caixa': 'prata', 'prata_rural_trabalho_social_bb': 'prata', 'prata_rural_obra_mensal': 'prata', 'prata_rural_financeiro_mensal': 'prata', 'ouro_rural_ficha_empreendimento': 'ouro', 'ouro_rural_resumo_gerencial': 'ouro', 'ouro_rural_panorama_estadual': 'ouro', 'ouro_rural_mapa_nacional': 'ouro', 'ouro_rural_evolucao_financeira': 'ouro', 'ouro_rural_execucao_fisica_financeira': 'ouro', 'ouro_rural_ficha_trabalho_social': 'ouro', 'ouro_rural_perfil_beneficiarios': 'ouro', 'ouro_rural_infraestrutura_agua_saneamento': 'ouro'}, {'prata_rural_empreendimento': ['apf'], 'prata_rural_prioritarios_snh': ['apf'], 'prata_rural_prioritarios_caixa': ['apf'], 'prata_rural_prioritarios_bb': ['apf'], 'prata_rural_cadastro_pj': ['apf'], 'prata_rural_pnhr_caixa': ['apf'], 'prata_rural_pnhr_bb': ['apf'], 'prata_rural_trabalho_social_caixa': ['apf'], 'prata_rural_trabalho_social_bb': ['apf'], 'prata_rural_obra_mensal': ['apf'], 'ouro_rural_ficha_empreendimento': ['apf'], 'ouro_rural_evolucao_financeira': ['apf', 'mes'], 'ouro_rural_ficha_trabalho_social': ['apf', 'agente_financeiro'], 'ouro_rural_perfil_beneficiarios': ['apf'], 'ouro_rural_infraestrutura_agua_saneamento': ['apf']})\nlinha 2\nlinha 3\n\n## 5. Cobertura dos joins\n\nsecao_joins('prata', [('prata_rural_prioritarios_snh', 'prata_rural_prioritarios_caixa', 'apf'), ('prata_rural_prioritarios_snh', 'prata_rural_prioritarios_bb', 'apf'), ('prata_rural_prioritarios_snh', 'prata_rural_cadastro_pj', 'apf'), ('prata_rural_prioritarios_snh', 'prata_rural_pnhr_caixa', 'apf'), ('prata_rural_prioritarios_snh', 'prata_rural_pnhr_bb', 'apf'), ('prata_rural_empreendimento', 'prata_rural_cadastro_pf', 'apf'), ('prata_rural_empreendimento', 'prata_rural_trabalho_social_caixa', 'apf'), ('prata_rural_empreendimento', 'prata_rural_obra_mensal', 'apf')])\nlinha 2\nlinha 3\n\n## 6. Valores reais dos campos de corte\n\nsecao_cortes({'prata_rural_empreendimento': 'prata', 'prata_rural_prioritarios_snh': 'prata', 'prata_rural_prioritarios_caixa': 'prata', 'prata_rural_prioritarios_bb': 'prata', 'prata_rural_cadastro_pj': 'prata', 'prata_rural_cadastro_pf': 'prata', 'prata_rural_pnhr_caixa': 'prata', 'prata_rural_pnhr_bb': 'prata', 'prata_rural_pnhr_liberacoes': 'prata', 'prata_rural_trabalho_social_caixa': 'prata', 'prata_rural_trabalho_social_bb': 'prata', 'prata_rural_obra_mensal': 'prata', 'prata_rural_financeiro_mensal': 'prata', 'ouro_rural_ficha_empreendimento': 'ouro', 'ouro_rural_resumo_gerencial': 'ouro', 'ouro_rural_panorama_estadual': 'ouro', 'ouro_rural_mapa_nacional': 'ouro', 'ouro_rural_evolucao_financeira': 'ouro', 'ouro_rural_execucao_fisica_financeira': 'ouro', 'ouro_rural_ficha_trabalho_social': 'ouro', 'ouro_rural_perfil_beneficiarios': 'ouro', 'ouro_rural_infraestrutura_agua_saneamento': 'ouro'}, [('prata_rural_prioritarios_snh', 'agente_financeiro'), ('prata_rural_prioritarios_snh', 'modalidade'), ('prata_rural_prioritarios_snh', 'situacao'), ('prata_rural_empreendimento', 'agente_financeiro'), ('ouro_rural_ficha_empreendimento', 'programa'), ('ouro_rural_ficha_empreendimento', 'status_execucao_simplificado'), ('ouro_rural_ficha_empreendimento', 'status_prazo')])\nlinha 2\nlinha 3\n\n## 7. Divergência entre fontes da mesma medida\n\nsecao_divergencia('prata', 1.0)\nlinha 2\nlinha 3\n\n## 8. Atualidade e procedência das medições\n\nsecao_atualidade('prata',)\nlinha 2\nlinha 3\n"


def test_falha_no_meio_mantem_o_que_ja_saiu(monkeypatch, capsys):
    def quebra(*_):
        raise RuntimeError("banco caiu")

    monkeypatch.setattr(dq, "conectar", _Conexao)
    for nome in [n for n in dir(dq) if n.startswith("secao_")]:
        monkeypatch.setattr(dq, nome, lambda *_a, _n=nome: [f"<{_n}>"])
    monkeypatch.setattr(dq, "secao_joins", quebra)
    monkeypatch.setattr(sys, "argv", ["diagnostico_qualidade.py"])
    with pytest.raises(RuntimeError, match="banco caiu"):
        dq.main()
    saida = capsys.readouterr().out
    assert "## 4. Duplicidade na chave natural" in saida and "<secao_duplicidade>" in saida
    assert "## 5." not in saida
