#!/usr/bin/env python3
"""Render an official-style Markdown report to HTML and PDF.

Requires pandoc for Markdown -> HTML. Uses Google Chrome/Chromium headless for
HTML -> PDF when available.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


CSS = """
@page {
  size: A4;
  margin: 22mm 18mm 20mm 18mm;
}

:root {
  --ink: #1f2937;
  --muted: #4b5563;
  --line: #d1d5db;
  --soft: #f3f4f6;
  --brand: #14532d;
  --brand-2: #2563eb;
}

body {
  color: var(--ink);
  font-family: "Arial", "Helvetica", sans-serif;
  font-size: 10.5pt;
  line-height: 1.45;
}

h1, h2, h3 {
  color: var(--brand);
  page-break-after: avoid;
}

h1 {
  border-bottom: 2px solid var(--brand);
  font-size: 20pt;
  margin-top: 1.2em;
  padding-bottom: 0.25em;
}

h2 {
  font-size: 15pt;
  margin-top: 1.4em;
}

h3 {
  font-size: 12pt;
  margin-top: 1.2em;
}

p {
  margin: 0.5em 0;
}

table {
  border-collapse: collapse;
  font-size: 8.5pt;
  margin: 0.8em 0 1.1em 0;
  page-break-inside: avoid;
  width: 100%;
}

th {
  background: var(--soft);
  color: #111827;
  font-weight: 700;
}

th, td {
  border: 1px solid var(--line);
  padding: 5px 6px;
  vertical-align: top;
}

code {
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 3px;
  font-family: "DejaVu Sans Mono", "Consolas", monospace;
  font-size: 8.5pt;
  padding: 1px 3px;
}

pre {
  background: #f8fafc;
  border: 1px solid #d1d5db;
  border-left: 4px solid var(--brand-2);
  font-family: "DejaVu Sans Mono", "Consolas", monospace;
  font-size: 7.5pt;
  line-height: 1.28;
  overflow-wrap: anywhere;
  padding: 9px 10px;
  white-space: pre-wrap;
}

img {
  display: block;
  margin: 0.8em auto;
  max-width: 100%;
}

blockquote {
  border-left: 4px solid var(--brand);
  color: var(--muted);
  margin-left: 0;
  padding-left: 1em;
}

.title {
  color: var(--brand);
}

#TOC {
  border: 1px solid var(--line);
  margin: 1em 0 2em;
  padding: 0.8em 1.2em;
}

#TOC ul {
  margin: 0.2em 0;
}
"""


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def find_chrome() -> str | None:
    for name in ("google-chrome", "chromium", "chromium-browser"):
        path = shutil.which(name)
        if path:
            return path
    return None


def try_pandoc_pdf(markdown: Path, pdf_path: Path, css_path: Path) -> bool:
    pandoc = shutil.which("pandoc")
    if not pandoc:
        return False
    for engine in ("xelatex", "lualatex", "pdflatex"):
        if not shutil.which(engine):
            continue
        try:
            run(
                [
                    pandoc,
                    str(markdown),
                    "--toc",
                    "--toc-depth=3",
                    "--resource-path",
                    str(markdown.parent),
                    "--metadata",
                    "lang=pt-BR",
                    "--pdf-engine",
                    engine,
                    "--variable",
                    "geometry:a4paper,margin=20mm",
                    "--variable",
                    "fontsize=10pt",
                    "--highlight-style",
                    "tango",
                    "-o",
                    str(pdf_path),
                ]
            )
            return True
        except subprocess.CalledProcessError:
            continue
    return False


def try_chrome_pdf(html_path: Path, pdf_path: Path) -> bool:
    chrome = find_chrome()
    if not chrome:
        return False
    try:
        run(
            [
                chrome,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--print-to-pdf={pdf_path}",
                str(html_path.resolve()),
            ]
        )
        return True
    except subprocess.CalledProcessError:
        return False


def render(markdown: Path, output_dir: Path, basename: str) -> tuple[Path, Path | None]:
    output_dir.mkdir(parents=True, exist_ok=True)
    css_path = output_dir / f"{basename}.css"
    html_path = output_dir / f"{basename}.html"
    pdf_path = output_dir / f"{basename}.pdf"
    css_path.write_text(CSS, encoding="utf-8")

    pandoc = shutil.which("pandoc")
    if not pandoc:
        raise SystemExit("pandoc not found; cannot render official HTML")

    run(
        [
            pandoc,
            str(markdown),
            "--standalone",
            "--toc",
            "--toc-depth=3",
            "--resource-path",
            str(markdown.parent),
            "--metadata",
            "lang=pt-BR",
            "--css",
            str(css_path.name),
            "-o",
            str(html_path),
        ]
    )

    if try_pandoc_pdf(markdown, pdf_path, css_path):
        return html_path, pdf_path

    if try_chrome_pdf(html_path, pdf_path):
        return html_path, pdf_path

    return html_path, None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("markdown", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--basename", default=None)
    args = parser.parse_args()

    markdown = args.markdown.resolve()
    output_dir = args.output_dir.resolve() if args.output_dir else markdown.parent
    basename = args.basename or markdown.stem
    html, pdf = render(markdown, output_dir, basename)
    print(f"HTML: {html}")
    if pdf:
        print(f"PDF: {pdf}")
    else:
        print("PDF: not generated; Chrome/Chromium not found")


if __name__ == "__main__":
    main()
