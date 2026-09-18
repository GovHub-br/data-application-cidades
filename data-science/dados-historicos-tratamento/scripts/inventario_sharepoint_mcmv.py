#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import pandas as pd
import msal
import requests
from office365.sharepoint.client_context import ClientContext


GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"


FRONT_PATTERNS = {
    "far": [r"\bfar\b", r"fundo de arrendamento", r"arrendamento residencial"],
    "rural": [r"\brural\b", r"pnhr"],
    "entidades": [r"entidades?", r"\bfds\b"],
    "sub50": [r"sub\s*50", r"sub50", r"fnhis"],
    "classe_media": [r"classe\s+media", r"fgts", r"geavo", r"sbpe"],
    "pro_moradia": [r"pro[\s_-]*moradia", r"promoradia"],
    "reforma": [r"reforma\s+casa\s+brasil", r"\brcb\b"],
    "cidades": [r"\bcidades\b", r"periferia"],
    "conjuntura": [r"conjuntura", r"ipea", r"ibge", r"caged", r"fgv", r"fipe"],
    "mcmv_geral": [r"minha\s+casa\s+minha\s+vida", r"\bmcmv\b"],
}

DATA_EXTENSIONS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".xlsm",
    ".parquet",
    ".json",
    ".zip",
    ".txt",
    ".ods",
    ".pbix",
    ".pdf",
}


@dataclass
class Settings:
    site_url: str
    page_url: str
    username: str
    password: str
    tenant: str
    client_id: str
    auth_mode: str
    output_dir: Path


def load_env(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def get_settings(args: argparse.Namespace) -> Settings:
    env = load_env(Path(args.env_file))
    missing = [
        key
        for key in ["SHAREPOINT_SITE_URL", "SHAREPOINT_PAGE_URL", "SHAREPOINT_USERNAME", "SHAREPOINT_PASSWORD"]
        if not env.get(key)
    ]
    if missing:
        raise SystemExit("Variaveis ausentes no local.env: " + ", ".join(missing))
    tenant = env.get("SHAREPOINT_TENANT")
    if not tenant:
        host = urlparse(env["SHAREPOINT_SITE_URL"]).hostname or ""
        tenant = host.split(".sharepoint.com", 1)[0] + ".onmicrosoft.com"
    client_id = env.get("SHAREPOINT_CLIENT_ID") or ""
    auth_mode = args.auth_mode or env.get("SHAREPOINT_AUTH_MODE") or "password"
    if auth_mode in {"password", "device_code"} and not client_id:
        raise SystemExit(
            "SHAREPOINT_CLIENT_ID ausente. O SharePoint Online exige um app registrado/autorizado "
            "no tenant para login moderno. Peca ao administrador um client_id com permissao de leitura "
            "no site InfoDPP, ou use um fluxo de exportacao/download manual."
        )
    return Settings(
        site_url=env["SHAREPOINT_SITE_URL"].rstrip("/"),
        page_url=env["SHAREPOINT_PAGE_URL"],
        username=env["SHAREPOINT_USERNAME"],
        password=env["SHAREPOINT_PASSWORD"],
        tenant=tenant,
        client_id=client_id,
        auth_mode=auth_mode,
        output_dir=Path(args.output_dir),
    )


def classify_front(text: str) -> str:
    haystack = text.lower()
    matches = []
    for front, patterns in FRONT_PATTERNS.items():
        if any(re.search(pattern, haystack, flags=re.IGNORECASE) for pattern in patterns):
            matches.append(front)
    return ";".join(matches) if matches else "nao_classificado"


def is_data_candidate(name: str) -> bool:
    return Path(name).suffix.lower() in DATA_EXTENSIONS


def connect(settings: Settings) -> ClientContext:
    if settings.auth_mode == "device_code":
        ctx = ClientContext(settings.site_url).with_device_flow(settings.tenant, settings.client_id)
    else:
        ctx = ClientContext(settings.site_url).with_username_and_password(
            settings.tenant,
            settings.client_id,
            settings.username,
            settings.password,
        )
    web = ctx.web.get().execute_query()
    print(f"Conectado ao SharePoint: {web.properties.get('Title', settings.site_url)}")
    return ctx


def list_site_lists(ctx: ClientContext) -> pd.DataFrame:
    rows = []
    for sp_list in ctx.web.lists.get().execute_query():
        props = sp_list.properties
        rows.append(
            {
                "title": props.get("Title"),
                "base_template": props.get("BaseTemplate"),
                "base_type": props.get("BaseType"),
                "hidden": props.get("Hidden"),
                "item_count": props.get("ItemCount"),
                "last_item_modified_date": props.get("LastItemModifiedDate"),
                "id": str(props.get("Id")),
            }
        )
    return pd.DataFrame(rows).sort_values(["hidden", "base_template", "title"], na_position="last")


def folder_children(ctx: ClientContext, server_relative_url: str):
    folder = ctx.web.get_folder_by_server_relative_url(server_relative_url)
    folder.expand(["Files", "Folders"]).get().execute_query()
    return folder.files, folder.folders


def graph_token(settings: Settings) -> str:
    scopes = [
        "https://graph.microsoft.com/Sites.Read.All",
        "https://graph.microsoft.com/Files.Read.All",
    ]
    app = msal.PublicClientApplication(
        settings.client_id,
        authority=f"https://login.microsoftonline.com/{settings.tenant}",
    )
    if settings.auth_mode == "device_code":
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise SystemExit(f"Falha ao iniciar device flow: {flow.get('error_description', flow)}")
        print(flow["message"], flush=True)
        result = app.acquire_token_by_device_flow(flow)
    else:
        result = app.acquire_token_by_username_password(
            username=settings.username,
            password=settings.password,
            scopes=scopes,
        )
    if "access_token" not in result:
        error = result.get("error", "unknown_error")
        description = result.get("error_description", "Sem descricao retornada.")
        raise SystemExit(f"Falha de autenticacao Graph: {error}: {description}")
    return result["access_token"]


def graph_session(settings: Settings) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {graph_token(settings)}"})
    return session


def graph_get(session: requests.Session, url: str, params: dict | None = None) -> dict:
    response = session.get(url, params=params, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Graph HTTP {response.status_code}: {response.text[:800]}")
    return response.json()


def graph_collect_all(session: requests.Session, url: str, params: dict | None = None) -> list[dict]:
    rows: list[dict] = []
    next_url = url
    next_params = params
    while next_url:
        payload = graph_get(session, next_url, params=next_params)
        rows.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
        next_params = None
    return rows


def graph_site_id(settings: Settings, session: requests.Session) -> tuple[str, str]:
    parsed = urlparse(settings.site_url)
    site_path = parsed.path.strip("/")
    payload = graph_get(session, f"{GRAPH_BASE_URL}/sites/{parsed.hostname}:/{site_path}")
    print(f"Conectado via Microsoft Graph: {payload.get('displayName') or payload.get('name')}")
    return payload["id"], parsed.hostname or ""


def list_graph_containers(settings: Settings, session: requests.Session, site_id: str) -> pd.DataFrame:
    rows = []
    for drive in graph_collect_all(session, f"{GRAPH_BASE_URL}/sites/{site_id}/drives"):
        rows.append(
            {
                "source_type": "drive",
                "title": drive.get("name"),
                "id": drive.get("id"),
                "web_url": drive.get("webUrl"),
                "drive_type": drive.get("driveType"),
                "created_date_time": drive.get("createdDateTime"),
                "last_modified_date_time": drive.get("lastModifiedDateTime"),
            }
        )
    try:
        lists = graph_collect_all(session, f"{GRAPH_BASE_URL}/sites/{site_id}/lists")
    except Exception as exc:
        lists = []
        rows.append({"source_type": "list_error", "title": "lists", "id": "", "web_url": "", "error": str(exc)[:500]})
    for sp_list in lists:
        rows.append(
            {
                "source_type": "list",
                "title": sp_list.get("displayName") or sp_list.get("name"),
                "id": sp_list.get("id"),
                "web_url": sp_list.get("webUrl"),
                "drive_type": "",
                "created_date_time": sp_list.get("createdDateTime"),
                "last_modified_date_time": sp_list.get("lastModifiedDateTime"),
            }
        )
    return pd.DataFrame(rows)


def inventory_graph_drives(session: requests.Session, site_id: str, containers_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    drives = containers_df[containers_df["source_type"].eq("drive")]
    for _, drive in drives.iterrows():
        drive_id = drive["id"]
        drive_title = drive["title"]
        print(f"Varrendo drive: {drive_title}")
        stack = [("root", 0)]
        seen: set[str] = set()
        while stack:
            item_id, depth = stack.pop()
            if item_id in seen:
                continue
            seen.add(item_id)
            url = (
                f"{GRAPH_BASE_URL}/drives/{drive_id}/root/children"
                if item_id == "root"
                else f"{GRAPH_BASE_URL}/drives/{drive_id}/items/{item_id}/children"
            )
            try:
                children = graph_collect_all(session, url)
            except Exception as exc:
                rows.append(
                    {
                        "library": drive_title,
                        "kind": "folder_error",
                        "name": item_id,
                        "server_relative_url": "",
                        "web_url": "",
                        "extension": "",
                        "size_bytes": None,
                        "time_last_modified": None,
                        "depth": depth,
                        "front_inferida": classify_front(f"{drive_title} {item_id}"),
                        "is_data_candidate": False,
                        "error": str(exc)[:500],
                    }
                )
                continue
            for item in children:
                name = item.get("name", "")
                kind = "folder" if "folder" in item else "file" if "file" in item else "item"
                parent_path = (item.get("parentReference") or {}).get("path", "")
                path_text = f"{parent_path}/{name}"
                rows.append(
                    {
                        "library": drive_title,
                        "kind": kind,
                        "name": name,
                        "server_relative_url": path_text,
                        "web_url": item.get("webUrl", ""),
                        "extension": Path(name).suffix.lower() if kind == "file" else "",
                        "size_bytes": item.get("size"),
                        "time_last_modified": item.get("lastModifiedDateTime"),
                        "depth": depth,
                        "front_inferida": classify_front(f"{drive_title} {path_text}"),
                        "is_data_candidate": kind == "file" and is_data_candidate(name),
                        "error": "",
                    }
                )
                if kind == "folder":
                    stack.append((item["id"], depth + 1))
    return pd.DataFrame(rows)


def run_graph_inventory(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame]:
    session = graph_session(settings)
    site_id, _ = graph_site_id(settings, session)
    containers_df = list_graph_containers(settings, session, site_id)
    files_df = inventory_graph_drives(session, site_id, containers_df)
    return containers_df, files_df


def walk_folder(ctx: ClientContext, library_title: str, root_url: str, max_depth: int | None = None) -> list[dict]:
    rows: list[dict] = []
    stack: list[tuple[str, int]] = [(root_url, 0)]
    seen: set[str] = set()
    while stack:
        folder_url, depth = stack.pop()
        if folder_url in seen:
            continue
        seen.add(folder_url)
        if max_depth is not None and depth > max_depth:
            continue
        try:
            files, folders = folder_children(ctx, folder_url)
        except Exception as exc:
            rows.append(
                {
                    "library": library_title,
                    "kind": "folder_error",
                    "name": Path(folder_url).name,
                    "server_relative_url": folder_url,
                    "web_url": "",
                    "extension": "",
                    "size_bytes": None,
                    "time_last_modified": None,
                    "depth": depth,
                    "front_inferida": classify_front(folder_url),
                    "is_data_candidate": False,
                    "error": str(exc)[:500],
                }
            )
            continue
        for file_obj in files:
            props = file_obj.properties
            name = props.get("Name") or Path(props.get("ServerRelativeUrl", "")).name
            server_url = props.get("ServerRelativeUrl", "")
            web_url = f"{ctx.base_url}{quote(server_url)}" if server_url else ""
            rows.append(
                {
                    "library": library_title,
                    "kind": "file",
                    "name": name,
                    "server_relative_url": server_url,
                    "web_url": web_url,
                    "extension": Path(name).suffix.lower(),
                    "size_bytes": props.get("Length"),
                    "time_last_modified": props.get("TimeLastModified"),
                    "depth": depth,
                    "front_inferida": classify_front(f"{library_title} {server_url} {name}"),
                    "is_data_candidate": is_data_candidate(name),
                    "error": "",
                }
            )
        for folder_obj in folders:
            props = folder_obj.properties
            name = props.get("Name") or Path(props.get("ServerRelativeUrl", "")).name
            server_url = props.get("ServerRelativeUrl", "")
            rows.append(
                {
                    "library": library_title,
                    "kind": "folder",
                    "name": name,
                    "server_relative_url": server_url,
                    "web_url": "",
                    "extension": "",
                    "size_bytes": None,
                    "time_last_modified": props.get("TimeLastModified"),
                    "depth": depth,
                    "front_inferida": classify_front(f"{library_title} {server_url} {name}"),
                    "is_data_candidate": False,
                    "error": "",
                }
            )
            if name.lower() != "forms":
                stack.append((server_url, depth + 1))
    return rows


def inventory_libraries(ctx: ClientContext, lists_df: pd.DataFrame, max_depth: int | None = None) -> pd.DataFrame:
    rows: list[dict] = []
    candidates = lists_df[lists_df["base_template"].isin([101, 119])]
    for _, item in candidates.iterrows():
        library = ctx.web.lists.get_by_title(item["title"])
        library.expand(["RootFolder"]).get().execute_query()
        root_url = library.root_folder.properties.get("ServerRelativeUrl")
        if not root_url:
            continue
        print(f"Varrendo biblioteca: {item['title']} ({root_url})")
        rows.extend(walk_folder(ctx, item["title"], root_url, max_depth=max_depth))
    return pd.DataFrame(rows)


def write_summary(settings: Settings, lists_df: pd.DataFrame, files_df: pd.DataFrame) -> Path:
    out = settings.output_dir
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    data = files_df[files_df["kind"].eq("file") & files_df["is_data_candidate"].eq(True)].copy()
    front_counts = Counter()
    for fronts in data["front_inferida"].fillna("nao_classificado"):
        for front in str(fronts).split(";"):
            front_counts[front] += 1
    summary = [
        "# Inventario SharePoint - MCMV",
        "",
        f"- Gerado em: `{now}`",
        f"- Site: `{settings.site_url}`",
        f"- Pagina de referencia: `{settings.page_url}`",
        f"- Listas/bibliotecas encontradas: `{len(lists_df)}`",
        f"- Itens varridos em bibliotecas: `{len(files_df)}`",
        f"- Arquivos candidatos a dado/BI/documento: `{len(data)}`",
        "",
        "## Arquivos por frente inferida",
        "",
        "| Frente | Arquivos candidatos |",
        "|---|---:|",
    ]
    for front, count in sorted(front_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        summary.append(f"| {front} | {count} |")
    summary.extend(["", "## Arquivos por extensao", "", "| Extensao | Arquivos |", "|---|---:|"])
    ext_counts = data["extension"].fillna("").replace("", "sem_extensao").value_counts()
    for ext, count in ext_counts.items():
        summary.append(f"| {ext} | {count} |")
    summary.extend(
        [
            "",
            "## Evidencias geradas",
            "",
            "- `sharepoint_listas.csv`: listas e bibliotecas do site.",
            "- `sharepoint_arquivos.csv`: arquivos e pastas varridos.",
            "- `sharepoint_arquivos_candidatos_mcmv.csv`: recorte de arquivos candidatos a uso em dados/dashboards.",
            "",
            "Observacao: `front_inferida` e uma classificacao por palavras-chave no nome/caminho. Ela orienta a triagem, mas ainda precisa de validacao humana ou leitura do conteudo dos arquivos prioritarios.",
        ]
    )
    md_path = out / "sharepoint-inventario-mcmv.md"
    md_path.write_text("\n".join(summary) + "\n", encoding="utf-8")
    return md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default="local.env")
    parser.add_argument(
        "--output-dir",
        default="data-science/dados-historicos-tratamento/docs/evidencias/sharepoint-mcmv",
    )
    parser.add_argument("--max-depth", type=int, default=None)
    parser.add_argument("--auth-mode", choices=["password", "device_code"], default=None)
    parser.add_argument("--api", choices=["graph", "sharepoint"], default="graph")
    args = parser.parse_args()

    settings = get_settings(args)
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    if args.api == "graph":
        lists_df, files_df = run_graph_inventory(settings)
    else:
        ctx = connect(settings)
        lists_df = list_site_lists(ctx)
        files_df = inventory_libraries(ctx, lists_df, max_depth=args.max_depth)
    candidates_df = files_df[files_df["kind"].eq("file") & files_df["is_data_candidate"].eq(True)].copy()

    lists_df.to_csv(settings.output_dir / "sharepoint_listas.csv", index=False)
    files_df.to_csv(settings.output_dir / "sharepoint_arquivos.csv", index=False)
    candidates_df.to_csv(settings.output_dir / "sharepoint_arquivos_candidatos_mcmv.csv", index=False)
    md_path = write_summary(settings, lists_df, files_df)

    print(f"Inventario salvo em: {settings.output_dir}")
    print(f"Resumo: {md_path}")
    print(f"Arquivos candidatos: {len(candidates_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
