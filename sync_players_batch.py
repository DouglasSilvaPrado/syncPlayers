# sync_players_batch.py
import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode, urljoin

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

BASE_URL = "https://sofifa.com"
PLAYERS_PATH = "/players"
PAGE_SIZE = 60
HL = "pt-BR"

PAGES_PER_RUN = int(os.environ.get("PAGES_PER_RUN", "10"))
SLEEP_BETWEEN_PAGES = float(os.environ.get("SLEEP_BETWEEN_PAGES", "1.2"))
PAGE_GOTO_TIMEOUT_MS = int(os.environ.get("PAGE_GOTO_TIMEOUT_MS", "60000"))
WAIT_SELECTOR_TIMEOUT_MS = int(os.environ.get("WAIT_SELECTOR_TIMEOUT_MS", "15000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "6"))

SHOW_COLS = [
    "ae","oa","pt","vl","wg","tt","pi","by","hi","wi","pf","bo","bp","gu","jt","le","rc",
    "ta","cr","fi","he","sh","vo","ts","dr","fr","cu","lo","bl","to","ac","sp","ag","re",
    "ba","tp","so","ju","st","sr","ln","te","ar","in","po","vi","pe","cm","td","ma","sa",
    "sl","tg","gd","gh","gc","gp","gr","bs","wk","sk","aw","dw","ir","bt","hc",
    "pac","sho","pas","dri","def","phy","t1","t2","ps1","ps2","tc","at","cp","cj",
]
BASE_PARAMS = {"col": "tt", "sort": "desc"}

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# -------------------------
# Parser (igual ao seu)
# -------------------------
def normalize_height(s: str) -> str:
    if not s:
        return s
    s = s.strip()
    m = re.match(r"^\s*(\d+)\s*cm\s*(\d+)\s*'\s*(\d+)\s*\"?\s*$", s)
    if m:
        cm, ft, inch = m.groups()
        return f"{cm}cm {ft}ft {inch}in"
    return s.replace('"', '').replace('″', '')

def pick_largest_from_srcset(srcset: str) -> str:
    if not srcset:
        return ""
    best_url = ""
    best_scale = -1.0
    parts = [p.strip() for p in srcset.split(",") if p.strip()]
    for p in parts:
        tokens = p.split()
        if not tokens:
            continue
        url = tokens[0].strip()
        scale = 1.0
        if len(tokens) > 1:
            descriptor = tokens[1].strip().lower()
            if descriptor.endswith("x"):
                try:
                    scale = float(descriptor[:-1])
                except:
                    scale = 1.0
            elif descriptor.endswith("w"):
                try:
                    scale = float(descriptor[:-1])
                except:
                    scale = 1.0
        if scale > best_scale:
            best_scale = scale
            best_url = url
    return best_url

def force_120_url(url: str) -> str:
    if not url:
        return ""
    url2 = re.sub(r"/(\d{2})_(\d+)\.png$", r"/\1_120.png", url)
    url2 = re.sub(r"/meta/team/(\d+)/(\d+)\.png$", r"/meta/team/\1/120.png", url2)
    url2 = re.sub(r"/(30|60|90)\.png$", "/120.png", url2)
    return url2

def get_img_url_120(img_tag) -> str:
    if not img_tag:
        return ""
    srcset = ""
    if img_tag.has_attr("data-srcset") and img_tag["data-srcset"]:
        srcset = img_tag["data-srcset"].strip()
    elif img_tag.has_attr("srcset") and img_tag["srcset"]:
        srcset = img_tag["srcset"].strip()

    if srcset:
        best = pick_largest_from_srcset(srcset)
        return force_120_url(best)

    if img_tag.has_attr("data-src") and img_tag["data-src"]:
        return force_120_url(img_tag["data-src"].strip())
    if img_tag.has_attr("src") and img_tag["src"]:
        return force_120_url(img_tag["src"].strip())
    return ""

def get_img_url(img_tag):
    if not img_tag:
        return ""
    if img_tag.has_attr("data-src") and img_tag["data-src"]:
        return img_tag["data-src"].strip()
    if img_tag.has_attr("src") and img_tag["src"]:
        return img_tag["src"].strip()
    return ""

def get_title(el):
    if not el:
        return ""
    if el.has_attr("title") and el["title"]:
        return el["title"].strip()
    if el.has_attr("alt") and el["alt"]:
        return el["alt"].strip()
    return ""

def clean_text(el):
    return el.get_text(" ", strip=True) if el else ""

def parse_player_id_from_href(href: str) -> str:
    m = re.search(r"/player/(\d+)/", href or "")
    return m.group(1) if m else ""

def build_players_url(offset: int) -> str:
    params = []
    for k, v in BASE_PARAMS.items():
        params.append((k, v))
    for c in SHOW_COLS:
        params.append(("showCol[]", c))
    params.append(("hl", HL))
    params.append(("offset", str(offset)))
    return f"{BASE_URL}{PLAYERS_PATH}?{urlencode(params, doseq=True)}"

def parse_list_page(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tbody tr")
    players = []

    for tr in rows:
        a = tr.select_one("a[href^='/player/']")
        href = a["href"] if a and a.has_attr("href") else ""
        player_url = urljoin(BASE_URL, href) if href else ""
        player_id = parse_player_id_from_href(href)
        name = clean_text(a)

        name_cell = a.find_parent("td") if a else None
        pos_spans = name_cell.select("span.pos") if name_cell else []
        positions = ",".join(dict.fromkeys([clean_text(s) for s in pos_spans if clean_text(s)]))

        img = tr.select_one("img.player-check") or tr.select_one("img[alt][src*='players/']") or tr.select_one("td img")
        player_img = get_img_url_120(img)

        nation_name = ""
        nation_img = ""
        if name_cell:
            flag = name_cell.select_one("img.flag[title]") or name_cell.select_one("img.flag")
            nation_name = get_title(flag)
            nation_img = get_img_url(flag)

        club_name = ""
        club_img = ""
        team_link = tr.select_one("a[href^='/team/']")
        if team_link:
            club_name = clean_text(team_link)
            team_td = team_link.find_parent("td")
            crest = None
            if team_td:
                crest = team_td.select_one("img.team") or team_td.select_one("figure.avatar img")
            club_img = get_img_url_120(crest)

        cols = {}
        for td in tr.select("td[data-col]"):
            key = td.get("data-col", "").strip()
            if not key:
                continue
            tag = td.select_one("span.bp3-tag") or td.select_one("span") or td
            val = clean_text(tag)
            if key == "hi":
                val = normalize_height(val)
            cols[key] = val

        players.append({
            "player_id": player_id,
            "player_url": player_url,
            "name": name,
            "positions": positions,
            "player_img": player_img,
            "nation_name": nation_name,
            "nation_img": nation_img,
            "club_name": club_name,
            "club_img": club_img,
            **cols,
        })

    return players

# -------------------------
# Supabase REST helpers
# -------------------------
def sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def stable_hash(payload: Dict[str, Any]) -> str:
    payload2 = dict(payload)
    payload2.pop("sofifa_hash", None)
    payload2.pop("sofifa_last_synced_at", None)
    payload2.pop("sofifa_etag", None)
    s = json.dumps(payload2, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def get_next_offset(sb_client: httpx.Client) -> int:
    url = f"{SUPABASE_URL}/rest/v1/sofifa_sync_state?id=eq.1&select=next_offset"
    r = sb_client.get(url, headers=sb_headers())
    r.raise_for_status()
    data = r.json()
    return int((data[0]["next_offset"] if data else 0) or 0)

def set_next_offset(sb_client: httpx.Client, next_offset: int):
    url = f"{SUPABASE_URL}/rest/v1/sofifa_sync_state?id=eq.1"
    payload = {"next_offset": int(next_offset), "updated_at": datetime.now(timezone.utc).isoformat()}
    r = sb_client.patch(url, headers={**sb_headers(), "Prefer": "return=minimal"}, json=payload)
    r.raise_for_status()

def fetch_hashes_for_ids(sb_client: httpx.Client, sofifa_ids: List[int]) -> Dict[int, Optional[str]]:
    """
    Busca hashes atuais no DB para comparar.
    Usa filtro in.(...) no PostgREST.
    """
    if not sofifa_ids:
        return {}

    # PostgREST: sofifa_player_id=in.(1,2,3)
    ids_str = ",".join(str(i) for i in sofifa_ids)
    url = f"{SUPABASE_URL}/rest/v1/players?sofifa_player_id=in.({ids_str})&select=sofifa_player_id,sofifa_hash"
    r = sb_client.get(url, headers=sb_headers())
    r.raise_for_status()
    data = r.json()
    return {int(row["sofifa_player_id"]): row.get("sofifa_hash") for row in data}

def upsert_players(sb_client: httpx.Client, rows: List[Dict[str, Any]]):
    url = f"{SUPABASE_URL}/rest/v1/players?on_conflict=sofifa_player_id"
    headers = {**sb_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"}
    r = sb_client.post(url, headers=headers, json=rows)
    r.raise_for_status()

def patch_last_synced(sb_client: httpx.Client, sofifa_ids: List[int], now_iso: str):
    """
    Atualiza somente sofifa_last_synced_at (sem mexer no resto) para os que não mudaram.
    """
    if not sofifa_ids:
        return
    ids_str = ",".join(str(i) for i in sofifa_ids)
    url = f"{SUPABASE_URL}/rest/v1/players?sofifa_player_id=in.({ids_str})"
    headers = {**sb_headers(), "Prefer": "return=minimal"}
    r = sb_client.patch(url, headers=headers, json={"sofifa_last_synced_at": now_iso})
    r.raise_for_status()

def chunked(items: List[Any], size: int):
    for i in range(0, len(items), size):
        yield items[i:i+size]

# -------------------------
# Playwright fetcher
# -------------------------
def fetch_page_html_with_playwright(page, url: str) -> str:
    delay = 2.0
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT_MS)
            if resp is not None and resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}")

            try:
                page.wait_for_selector("tbody tr", timeout=WAIT_SELECTOR_TIMEOUT_MS)
            except Exception:
                pass

            return page.content()

        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES:
                break
            log(f"⚠️ Falha page.goto ({attempt}/{MAX_RETRIES}) -> {e} | retry em {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 1.8, 30)

    raise RuntimeError(f"Falha ao baixar {url}: {last_err}")

# =========================
# Main
# =========================
def main():
    now_iso = datetime.now(timezone.utc).isoformat()
    sb_timeout = httpx.Timeout(30.0)
    sb_client = httpx.Client(timeout=sb_timeout, follow_redirects=True)

    offset = get_next_offset(sb_client)
    log(f"▶️ Sync start | next_offset={offset} | pages_per_run={PAGES_PER_RUN}")

    total_players_seen = 0
    total_changed = 0
    total_unchanged = 0
    total_pages = 0

    with sync_playwright() as p:
        log("🚀 Abrindo Chromium (headless=True)")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.7,en;q=0.6"},
        )

        context.route(
            "**/*",
            lambda route, request: route.abort()
            if request.resource_type in ("image", "media", "font")
            else route.continue_(),
        )

        page = context.new_page()

        try:
            for i in range(PAGES_PER_RUN):
                url = build_players_url(offset)
                log(f"🌍 Página {i+1}/{PAGES_PER_RUN} | offset={offset}")

                html = fetch_page_html_with_playwright(page, url)
                players = parse_list_page(html)

                if not players:
                    log("🏁 Página vazia (fim). Resetando offset para 0.")
                    offset = 0
                    set_next_offset(sb_client, offset)
                    break

                # transforma em rows e calcula hash
                rows: List[Dict[str, Any]] = []
                sofifa_ids: List[int] = []

                for pl in players:
                    pid = pl.get("player_id")
                    if not pid:
                        continue
                    sofifa_id = int(pid)
                    sofifa_ids.append(sofifa_id)

                    row = dict(pl)
                    row.pop("player_id", None)
                    row["sofifa_player_id"] = sofifa_id
                    row["sofifa_last_synced_at"] = now_iso
                    row["sofifa_hash"] = stable_hash(row)
                    rows.append(row)

                total_players_seen += len(rows)
                total_pages += 1

                # pega hashes atuais do DB pra comparar
                current_hashes = fetch_hashes_for_ids(sb_client, sofifa_ids)

                changed_rows: List[Dict[str, Any]] = []
                unchanged_ids: List[int] = []

                for r in rows:
                    sid = r["sofifa_player_id"]
                    old_hash = current_hashes.get(sid)
                    if old_hash == r["sofifa_hash"] and old_hash is not None:
                        unchanged_ids.append(sid)
                    else:
                        changed_rows.append(r)

                # upsert só do que mudou (ou não existia antes)
                for batch in chunked(changed_rows, 200):
                    upsert_players(sb_client, batch)

                # pros que não mudaram, só atualiza last_synced
                for batch_ids in chunked(unchanged_ids, 400):
                    patch_last_synced(sb_client, batch_ids, now_iso)

                total_changed += len(changed_rows)
                total_unchanged += len(unchanged_ids)

                log(f"✅ Página ok | vistos={len(rows)} | mudaram={len(changed_rows)} | iguais={len(unchanged_ids)}")

                offset += PAGE_SIZE
                set_next_offset(sb_client, offset)
                time.sleep(SLEEP_BETWEEN_PAGES)

        finally:
            context.close()
            browser.close()
            sb_client.close()

    log(f"✅ Sync done | pages={total_pages} | vistos={total_players_seen} | mudaram={total_changed} | iguais={total_unchanged} | next_offset={offset}")

if __name__ == "__main__":
    main()