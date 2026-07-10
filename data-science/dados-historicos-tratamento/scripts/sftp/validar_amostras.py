#!/usr/bin/env python3
"""Valida amostras de conteúdo extraídas via DBeaver (Fase 2 do batimento).

Lê CSVs de amostra, compara colunas de pares matched e gera relatório de validação.

Uso:
    uv run python scripts/sftp/validar_amostras.py
    uv run python scripts/sftp/validar_amostras.py -r data/sftp/analise/tabelas_relacionadas.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional

import pandas as pd


def normalizar_arquivo(nome: str) -> str:
    """Normaliza nome de tabela para nome de arquivo."""
    return re.sub(r'["\s()]', "_", str(nome)).strip("_").lower()


def encontrar_amostra(tabela: str, schema: str, diretorio: Path) -> Optional[Path]:
    """Encontra o CSV de amostra para uma tabela.

    Tenta match exato com nome normalizado e fuzzy por substring.
    """
    nome_base = normalizar_arquivo(tabela)
    nome_limpo = tabela.replace('"', "").strip().lower()
    padroes = [
        f"amostra_{schema}_{nome_base}.csv",
        f"amostra_{schema}_{nome_limpo}.csv",
    ]
    for padrao in padroes:
        caminho = diretorio / padrao
        if caminho.exists():
            return caminho

    # Busca fuzzy: listar arquivos e procurar por substring
    for f in diretorio.glob(f"amostra_{schema}_*.csv"):
        if nome_base[:20] in f.stem.lower():
            return f

    return None


def validar_formato_cpf(valores: pd.Series) -> tuple[str, str]:
    """Valida formato CPF."""
    amostra = valores.dropna().astype(str).head(5)
    if amostra.empty:
        return "inconclusivo", "sem valores não-nulos"
    padrao_pontuado = re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")
    padrao_limpo = re.compile(r"^\d{11}$")
    pontuado = sum(1 for v in amostra if padrao_pontuado.match(v))
    limpo = sum(1 for v in amostra if padrao_limpo.match(v))
    if pontuado >= len(amostra) * 0.8:
        return "validado", "formato XXX.XXX.XXX-XX"
    elif limpo >= len(amostra) * 0.8:
        return "validado", "formato 11 dígitos"
    else:
        return "divergente", f"formatos: {amostra.tolist()[:3]}"


def validar_formato_cnpj(valores: pd.Series) -> tuple[str, str]:
    """Valida formato CNPJ."""
    amostra = valores.dropna().astype(str).head(5)
    if amostra.empty:
        return "inconclusivo", "sem valores não-nulos"
    padrao = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")
    limpo = re.compile(r"^\d{14}$")
    pontuado = sum(1 for v in amostra if padrao.match(v))
    so_digitos = sum(1 for v in amostra if limpo.match(v))
    if pontuado >= len(amostra) * 0.8:
        return "validado", "formato XX.XXX.XXX/XXXX-XX"
    elif so_digitos >= len(amostra) * 0.8:
        return "validado", "formato 14 dígitos"
    else:
        return "divergente", f"formatos: {amostra.tolist()[:3]}"


def validar_formato_anomes(valores: pd.Series) -> tuple[str, str]:
    """Valida formato ano-mês."""
    amostra = valores.dropna().astype(str).head(5)
    if amostra.empty:
        return "inconclusivo", "sem valores não-nulos"
    padrao = re.compile(r"^\d{6}$")
    matches = sum(1 for v in amostra if padrao.match(v))
    if matches >= len(amostra) * 0.8:
        return "validado", "formato YYYYMM (6 dígitos)"
    else:
        return "divergente", f"formatos: {amostra.tolist()[:3]}"


def validar_tipo_dado(vals_sftp: pd.Series, vals_dh: pd.Series) -> tuple[str, str]:
    """Compara tipo de dado (numérico vs texto) entre schemas."""
    n_sf = len(vals_sftp.dropna())
    n_dh = len(vals_dh.dropna())
    if n_sf == 0 or n_dh == 0:
        return "inconclusivo", "sem valores não-nulos em uma das amostras"

    sf_numeric = pd.to_numeric(vals_sftp.dropna(), errors="coerce").notna().mean()
    dh_numeric = pd.to_numeric(vals_dh.dropna(), errors="coerce").notna().mean()

    sf_tipo = "numérico" if sf_numeric > 0.8 else "texto"
    dh_tipo = "numérico" if dh_numeric > 0.8 else "texto"

    if sf_tipo == dh_tipo:
        return "validado", f"ambos {sf_tipo}"
    else:
        return "divergente", f"SFTP: {sf_tipo}; Dump: {dh_tipo}"


def validar_cardinalidade(vals_sftp: pd.Series, vals_dh: pd.Series) -> tuple[str, str]:
    """Compara cardinalidade (valores distintos / total)."""
    n_sf = len(vals_sftp.dropna())
    n_dh = len(vals_dh.dropna())
    if n_sf == 0 or n_dh == 0:
        return "inconclusivo", "sem valores suficientes"
    dist_sf = vals_sftp.dropna().nunique() / n_sf
    dist_dh = vals_dh.dropna().nunique() / n_dh
    obs = (
        f"SFTP: {dist_sf:.1%} distintos ({n_sf} valores); "
        f"Dump: {dist_dh:.1%} distintos ({n_dh} valores)"
    )
    return "validado", obs


VALIDACOES_ESPECIAIS = {
    "cpf": validar_formato_cpf,
    "cnpj": validar_formato_cnpj,
    "anomes": validar_formato_anomes,
}


def validar_par(
    tabela_sftp: str,
    tabela_dump: str,
    df_sftp: pd.DataFrame,
    df_dh: pd.DataFrame,
) -> pd.DataFrame:
    """Valida um par de tabelas amostradas.

    Para cada coluna em comum (por nome normalizado), executa validações
    de formato, tipo de dado e cardinalidade.
    """
    linhas: list[dict[str, str]] = []

    # Mapa: nome normalizado → nome original
    cols_sftp = {c.strip().lower(): c for c in df_sftp.columns}
    cols_dh = {c.strip().lower(): c for c in df_dh.columns}
    comuns = set(cols_sftp.keys()) & set(cols_dh.keys())

    for col_norm in sorted(comuns):
        col_sf = cols_sftp[col_norm]
        col_dh = cols_dh[col_norm]

        # --- Validação de formato (apenas para colunas de chave conhecida) ---
        if col_norm in VALIDACOES_ESPECIAIS:
            resultado, obs = VALIDACOES_ESPECIAIS[col_norm](df_sftp[col_sf])
            linhas.append(
                {
                    "tabela_sftp": tabela_sftp,
                    "tabela_dump": tabela_dump,
                    "coluna": col_norm,
                    "tipo_validacao": "formato",
                    "resultado": resultado,
                    "observacao": f"SFTP: {obs}",
                }
            )
            resultado, obs = VALIDACOES_ESPECIAIS[col_norm](df_dh[col_dh])
            linhas.append(
                {
                    "tabela_sftp": tabela_sftp,
                    "tabela_dump": tabela_dump,
                    "coluna": col_norm,
                    "tipo_validacao": "formato",
                    "resultado": resultado,
                    "observacao": f"Dump: {obs}",
                }
            )
        else:
            # Validação de tipo de dado (comparativo entre schemas)
            resultado, obs = validar_tipo_dado(df_sftp[col_sf], df_dh[col_dh])
            linhas.append(
                {
                    "tabela_sftp": tabela_sftp,
                    "tabela_dump": tabela_dump,
                    "coluna": col_norm,
                    "tipo_validacao": "tipo_dado",
                    "resultado": resultado,
                    "observacao": obs,
                }
            )

        # --- Cardinalidade (sempre) ---
        resultado, obs = validar_cardinalidade(df_sftp[col_sf], df_dh[col_dh])
        linhas.append(
            {
                "tabela_sftp": tabela_sftp,
                "tabela_dump": tabela_dump,
                "coluna": col_norm,
                "tipo_validacao": "cardinalidade",
                "resultado": resultado,
                "observacao": obs,
            }
        )

    return pd.DataFrame(
        linhas,
        columns=[
            "tabela_sftp",
            "tabela_dump",
            "coluna",
            "tipo_validacao",
            "resultado",
            "observacao",
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida amostras de conteúdo")
    parser.add_argument(
        "--relacionadas",
        "-r",
        default="data/sftp/analise/tabelas_relacionadas.csv",
        help="Caminho para tabelas_relacionadas.csv",
    )
    parser.add_argument(
        "--amostras-dir",
        "-a",
        default="data/sftp/artefatos/",
        help="Diretório com CSVs de amostra",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="data/sftp/analise/validacao_amostras.csv",
        help="Caminho para o CSV de validação de saída",
    )
    args = parser.parse_args()

    relacionadas_path = Path(args.relacionadas)
    if not relacionadas_path.exists():
        print(f"Erro: arquivo não encontrado: {relacionadas_path}")
        print(
            "Execute primeiro o batimento estrutural: uv run python scripts/sftp/batimento_estrutura.py"
        )
        return

    amostras_dir = Path(args.amostras_dir)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_rel = pd.read_csv(relacionadas_path, dtype=str)
    print(f"Pares a validar: {len(df_rel)}")

    todos_resultados: list[pd.DataFrame] = []
    encontradas = 0
    nao_encontradas = 0

    for _, row in df_rel.iterrows():
        ts = str(row["tabela_sftp"])
        td = str(row["tabela_dump"])

        am_sftp = encontrar_amostra(ts, "sftp", amostras_dir)
        am_dh = encontrar_amostra(td, "dados_historicos", amostras_dir)

        if am_sftp is not None and am_dh is not None:
            encontradas += 1
            try:
                df_s = pd.read_csv(am_sftp, dtype=str, nrows=5)
                df_d = pd.read_csv(am_dh, dtype=str, nrows=5)
                resultado = validar_par(ts, td, df_s, df_d)
                todos_resultados.append(resultado)
                print(f"  ✓ {ts} ↔ {td}: {len(resultado)} validações")
            except Exception as e:
                print(f"  ✗ Erro ao validar {ts} ↔ {td}: {e}")
        else:
            nao_encontradas += 1
            if am_sftp is None:
                print(f"  ? Amostra SFTP não encontrada: {ts}")
            if am_dh is None:
                print(f"  ? Amostra Dump não encontrada: {td}")

    print(f"\nResumo: {encontradas} pares validados, {nao_encontradas} sem amostras")

    if todos_resultados:
        df_final = pd.concat(todos_resultados, ignore_index=True)
        df_final.to_csv(output_path, index=False)
        print(f"Resultado salvo: {output_path}")
        print(f"Total de validações: {len(df_final)}")
        print(f"  Validados: {len(df_final[df_final['resultado'] == 'validado'])}")
        print(f"  Divergentes: {len(df_final[df_final['resultado'] == 'divergente'])}")
        print(
            f"  Inconclusivos: {len(df_final[df_final['resultado'] == 'inconclusivo'])}"
        )
    else:
        print("Nenhum resultado de validação gerado.")


if __name__ == "__main__":
    main()
