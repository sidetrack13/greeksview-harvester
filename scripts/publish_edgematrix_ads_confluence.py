#!/usr/bin/env python3
"""
GreeksView Confluence Publisher: Marketing & Acquisition Strategy: Intraday EdgeMatrix™
========================================================================================
Publishes Google Ads & Landing Pages specification to Atlassian Confluence Cloud under Space SD.
Parent Page: Product Specification: Intraday EdgeMatrix™ — Real-Time 5-Minute Pattern Scanner, SEO Strategy & 0DTE Options Playbook (ID: 18743297)
"""

import base64
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

PARENT_PAGE_ID = "18743297"
PAGE_TITLE = "Marketing & Acquisition Strategy: Intraday EdgeMatrix™ — Google Ads Campaigns, 4-Pillar Landing Pages & Subscription Funnel"
DOC_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "docs",
    "confluence",
    "intraday-edgematrix-google-ads-landing-pages.md",
)


def load_env():
    candidates = [
        "/Users/senthilchinnappan/Projects/github/greeksview/.env.ai-engineering",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env.ai-engineering"),
    ]
    env = {}
    for path in candidates:
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        env[k.strip()] = v.strip().strip('"').strip("'")
            break
    return env


def get_confluence_client():
    env = load_env()
    url = env.get("JIRA_URL", "").rstrip("/")
    email = env.get("JIRA_USER_EMAIL", "")
    token = env.get("JIRA_API_TOKEN", "")
    space_key = "SD"

    if not url or not email or not token:
        print("[ERROR] Confluence credentials missing in .env.ai-engineering.")
        return None

    auth_str = base64.b64encode(f"{email}:{token}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_str}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    return {
        "base_url": f"{url}/wiki/rest/api",
        "headers": headers,
        "space_key": space_key,
        "parent_id": PARENT_PAGE_ID,
        "site_url": url,
    }


def find_page_by_title(client, title):
    if not client:
        return None
    url = (
        f"{client['base_url']}/content?spaceKey={client['space_key']}&title={urllib.parse.quote(title)}&expand=version"
    )
    req = urllib.request.Request(url, headers=client["headers"])
    ctx = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            data = json.loads(resp.read().decode())
            results = data.get("results", [])
            return results[0] if results else None
    except Exception as e:
        print(f"  [DEBUG] Error checking page '{title}': {e}")
        return None


def publish_page(client, title, html_body, parent_id=None):
    if not client:
        return None
    existing = find_page_by_title(client, title)
    pid = parent_id or client["parent_id"]

    if existing:
        page_id = existing["id"]
        version_num = existing["version"]["number"] + 1
        payload = {
            "id": page_id,
            "type": "page",
            "title": title,
            "space": {"key": client["space_key"]},
            "body": {
                "storage": {
                    "value": html_body,
                    "representation": "storage",
                }
            },
            "version": {"number": version_num},
        }
        url = f"{client['base_url']}/content/{page_id}"
        method = "PUT"
        action = "Updated"
    else:
        payload = {
            "type": "page",
            "title": title,
            "space": {"key": client["space_key"]},
            "ancestors": [{"id": str(pid)}],
            "body": {
                "storage": {
                    "value": html_body,
                    "representation": "storage",
                }
            },
        }
        url = f"{client['base_url']}/content"
        method = "POST"
        action = "Created"

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=client["headers"], method=method)
    ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            res_data = json.loads(resp.read().decode())
            new_id = res_data.get("id")
            webui_path = res_data.get("_links", {}).get("webui", "")
            full_url = f"{client['site_url']}/wiki{webui_path}"
            print(f"[SUCCESS] {action} page '{title}' (ID: {new_id})")
            print(f"          URL: {full_url}")
            return new_id, full_url
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"[ERROR] Failed to {action.lower()} page: {e.code} {e.reason}")
        print(f"        Response: {err_body}")
        return None
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def markdown_to_confluence_html(md_text):
    lines = md_text.splitlines()
    html_parts = []
    in_code_block = False
    code_lines = []
    in_table = False
    table_lines = []
    in_ul = False
    in_ol = False

    def close_lists():
        nonlocal in_ul, in_ol
        parts = []
        if in_ul:
            parts.append("</ul>")
            in_ul = False
        if in_ol:
            parts.append("</ol>")
            in_ol = False
        return parts

    def close_table():
        nonlocal in_table, table_lines
        if not in_table or not table_lines:
            in_table = False
            table_lines = []
            return []
        t_html = [
            '<table style="border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 13px; font-family: -apple-system, BlinkMacSystemFont, sans-serif;">'
        ]
        is_header = True
        for row in table_lines:
            raw_cells = [c.strip() for c in row.strip("|").split("|")]
            if is_header:
                t_html.append('<thead><tr style="background-color: #1e293b; color: #ffffff;">')
                for c in raw_cells:
                    clean_c = format_inlines(c)
                    t_html.append(
                        f'<th style="padding: 10px; border: 1px solid #334155; text-align: left;">{clean_c}</th>'
                    )
                t_html.append("</tr></thead><tbody>")
                is_header = False
            elif all(set(c).issubset({"-", ":", " "}) for c in raw_cells):
                continue
            else:
                t_html.append('<tr style="border-bottom: 1px solid #e2e8f0;">')
                for c in raw_cells:
                    clean_c = format_inlines(c)
                    t_html.append(f'<td style="padding: 8px 10px; border: 1px solid #cbd5e1;">{clean_c}</td>')
                t_html.append("</tr>")
        t_html.append("</tbody></table>")
        table_lines = []
        in_table = False
        return t_html

    def format_inlines(txt):
        txt = txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        txt = re.sub(r"\*\*\*(.*?)\*\*\*", r"<strong><em>\1</em></strong>", txt)
        txt = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", txt)
        txt = re.sub(r"\*(.*?)\*", r"<em>\1</em>", txt)
        txt = re.sub(
            r"`(.*?)`",
            r'<code style="background: #f1f5f9; color: #0f172a; padding: 2px 5px; border-radius: 4px; font-family: monospace;">\1</code>',
            txt,
        )
        txt = re.sub(r"\$\$(.*?)\$\$", r"<code>\1</code>", txt)
        txt = re.sub(r"\$(.*?)\$", r"<code>\1</code>", txt)
        txt = re.sub(
            r"\[(.*?)\]\((.*?)\)", r'<a href="\2" style="color: #0284c7; text-decoration: underline;">\1</a>', txt
        )
        return txt

    i = 0
    while i < len(lines):
        line = lines[i]

        if line.strip().startswith("```"):
            if in_code_block:
                in_code_block = False
                escaped_code = "\n".join(code_lines).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                html_parts.append(
                    f'<pre style="background: #0f172a; color: #f8fafc; padding: 14px; border-radius: 6px; font-family: monospace; font-size: 12px; line-height: 1.5; overflow-x: auto;">{escaped_code}</pre>'
                )
                code_lines = []
            else:
                html_parts.extend(close_lists())
                html_parts.extend(close_table())
                in_code_block = True
                code_lines = []
            i += 1
            continue

        if in_code_block:
            code_lines.append(line)
            i += 1
            continue

        if line.strip().startswith("|") and line.strip().endswith("|"):
            html_parts.extend(close_lists())
            in_table = True
            table_lines.append(line)
            i += 1
            continue
        elif in_table:
            html_parts.extend(close_table())

        if line.startswith("# "):
            html_parts.extend(close_lists())
            title_text = format_inlines(line[2:].strip())
            html_parts.append(
                f'<h1 style="color: #0284c7; margin-top: 20px; font-size: 26px; border-bottom: 2px solid #0284c7; padding-bottom: 6px;">{title_text}</h1>'
            )
            i += 1
            continue
        elif line.startswith("## "):
            html_parts.extend(close_lists())
            h_text = format_inlines(line[3:].strip())
            html_parts.append(
                f'<h2 style="color: #0f172a; border-bottom: 2px solid #e2e8f0; padding-bottom: 4px; margin-top: 28px; font-size: 20px;">{h_text}</h2>'
            )
            i += 1
            continue
        elif line.startswith("### "):
            html_parts.extend(close_lists())
            h_text = format_inlines(line[4:].strip())
            html_parts.append(f'<h3 style="color: #1e293b; margin-top: 20px; font-size: 16px;">{h_text}</h3>')
            i += 1
            continue
        elif line.startswith("#### "):
            html_parts.extend(close_lists())
            h_text = format_inlines(line[5:].strip())
            html_parts.append(f'<h4 style="color: #334155; margin-top: 14px; font-size: 14px;">{h_text}</h4>')
            i += 1
            continue

        if line.strip() in ("---", "***", "___"):
            html_parts.extend(close_lists())
            html_parts.append('<hr style="border: 0; border-top: 1px solid #cbd5e1; margin: 24px 0;"/>')
            i += 1
            continue

        ul_match = re.match(r"^(\s*)[-*+]\s+(.*)$", line)
        if ul_match:
            if not in_ul:
                html_parts.extend(close_lists())
                in_ul = True
                html_parts.append('<ul style="line-height: 1.6; padding-left: 20px; margin: 10px 0;">')
            item_text = format_inlines(ul_match.group(2))
            html_parts.append(f'<li style="margin-bottom: 4px;">{item_text}</li>')
            i += 1
            continue

        ol_match = re.match(r"^\s*\d+\.\s+(.*)$", line)
        if ol_match:
            if not in_ol:
                html_parts.extend(close_lists())
                in_ol = True
                html_parts.append('<ol style="line-height: 1.6; padding-left: 20px; margin: 10px 0;">')
            item_text = format_inlines(ol_match.group(1))
            html_parts.append(f'<li style="margin-bottom: 4px;">{item_text}</li>')
            i += 1
            continue

        if not line.strip():
            html_parts.extend(close_lists())
            i += 1
            continue

        html_parts.extend(close_lists())
        if line.startswith(">"):
            q_text = format_inlines(line.lstrip("> ").strip())
            html_parts.append(
                f'<blockquote style="border-left: 4px solid #0284c7; padding: 8px 16px; margin: 12px 0; background: #f8fafc; color: #475569;">{q_text}</blockquote>'
            )
            i += 1
            continue

        p_text = format_inlines(line)
        html_parts.append(f'<p style="margin: 8px 0; line-height: 1.6; color: #1e293b;">{p_text}</p>')
        i += 1

    html_parts.extend(close_lists())
    html_parts.extend(close_table())
    return "\n".join(html_parts)


def main():
    print(f"Reading markdown file: {DOC_PATH}")
    if not os.path.exists(DOC_PATH):
        print(f"[ERROR] File not found: {DOC_PATH}")
        sys.exit(1)

    with open(DOC_PATH) as f:
        md_content = f.read()

    print("Converting Markdown to Confluence Storage XHTML...")
    html_body = markdown_to_confluence_html(md_content)

    print(f"Connecting to Confluence Cloud for page: '{PAGE_TITLE}'...")
    client = get_confluence_client()
    if not client:
        print("[ERROR] Could not initialize Confluence client.")
        sys.exit(1)

    res = publish_page(client, PAGE_TITLE, html_body, PARENT_PAGE_ID)
    if res:
        page_id, url = res
        print(f"[DONE] Page successfully live at: {url}")
    else:
        print("[ERROR] Page publish failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
