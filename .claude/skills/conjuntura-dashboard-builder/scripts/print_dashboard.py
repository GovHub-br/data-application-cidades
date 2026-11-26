"""Print das abas do dashboard do Boletim de Conjuntura no Superset, para conferir o visual.

Uso (na raiz do repo, com VPN; o Playwright entra só no ambiente do uvx, não no projeto):
    uvx --with playwright==1.55.0 --with python-dotenv python \\
        .claude/skills/conjuntura-dashboard-builder/scripts/print_dashboard.py <pasta> [abas]

`abas`: números separados por vírgula (padrão: 1..7). Grava <pasta>/pagina_<n>.png.
Na primeira vez: `uvx --from playwright==1.55.0 playwright install chromium`.

Só leitura: faz login pelo formulário (o bearer JWT não enxerga o dashboard nesta
instância) e navega. Não espera `networkidle`, que nunca chega: o Superset fica
consultando o servidor; espera o primeiro quadro aparecer.
"""

import pathlib
import sys

from dotenv import dotenv_values
from playwright.sync_api import sync_playwright

RAIZ = pathlib.Path(__file__).resolve().parents[4]
SLUG = "boletim-conjuntura"


def main() -> None:
    pasta = pathlib.Path(sys.argv[1])
    pasta.mkdir(parents=True, exist_ok=True)
    abas = [int(a) for a in (sys.argv[2] if len(sys.argv) > 2 else "1,2,3,4,5,6,7").split(",")]
    env = dotenv_values(RAIZ / ".env")
    base = env["SUPERSET_URL"].rstrip("/")
    usuario = env.get("SUPERSET_USER") or env["SUPERSET_USERNAME"]
    with sync_playwright() as p:
        navegador = p.chromium.launch()
        pg = navegador.new_page(viewport={"width": 1600, "height": 1000})
        pg.goto(f"{base}/login/", wait_until="domcontentloaded")
        pg.fill("#username", usuario)
        pg.fill("#password", env["SUPERSET_PASSWORD"])
        pg.keyboard.press("Enter")
        pg.wait_for_timeout(4000)
        pg.goto(f"{base}/superset/dashboard/{SLUG}/", wait_until="domcontentloaded")
        pg.wait_for_selector(".dashboard-component-chart-holder", timeout=90_000)
        for n in abas:
            pg.get_by_text(f"Página {n}", exact=True).first.click()
            pg.wait_for_timeout(6000)  # os quadros da aba carregam depois do clique
            pg.screenshot(path=str(pasta / f"pagina_{n}.png"))
            print(pasta / f"pagina_{n}.png")
        navegador.close()


if __name__ == "__main__":
    main()
