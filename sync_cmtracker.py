"""CMTracker -> Liga Virtual. Safe by default: dry-run, existing players only."""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import httpx

API_BASE = os.getenv('CMTRACKER_API_BASE', 'https://api.cmtracker.net/api/v1').rstrip('/')
DB_PATH = os.getenv('CMTRACKER_DBS_PATH', '/dbs')
PLAYER_PATH = os.getenv('CMTRACKER_PLAYERS_PATH', '/players')
PAGE_SIZE = int(os.getenv('CMTRACKER_PAGE_SIZE', '25'))
MAX_PAGES = int(os.getenv('CMTRACKER_MAX_PAGES', '20'))
DELAY = float(os.getenv('CMTRACKER_DELAY_SECONDS', '1.0'))
# API endpoint/parameter conventions can be changed without editing code.
PAGE_PARAM = os.getenv('CMTRACKER_PAGE_PARAM', 'page')
SIZE_PARAM = os.getenv('CMTRACKER_PAGE_SIZE_PARAM', 'pageSize')
DB_PARAM = os.getenv('CMTRACKER_DB_PARAM', 'db')
FIRST_PAGE = int(os.getenv('CMTRACKER_FIRST_PAGE', '0'))
DB_SLUG = os.getenv('CMTRACKER_DB_SLUG', '').strip()
TOKEN = os.getenv('CMTRACKER_API_KEY', '').strip()
AUTH_HEADER = os.getenv('CMTRACKER_AUTH_HEADER', 'x-api-key').strip()
if AUTH_HEADER.lower() not in ('x-api-key', 'authorization'):
    raise SystemExit('CMTRACKER_AUTH_HEADER deve ser x-api-key ou authorization')
SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
TIMEOUT = httpx.Timeout(40.0)

# Only well-known matching fields are written. Never write team/contract fields.
FIELD_ALIASES = {
    'name': ('name', 'shortName'),
    'positions': ('positions',),
    'player_img': ('player_img', 'playerImage', 'imageUrl'),
    'nation_name': ('nation_name', 'nationalityName'),
    'nation_img': ('nation_img', 'nationalityImage'),
    'club_name': ('club_name', 'clubName'),
    'club_img': ('club_img', 'clubImage'),
    'oa': ('oa', 'overall'),
    'pt': ('pt', 'potential'),
}


def info(s):
    print(s, flush=True)


def fetch_json(client, path, params=None):
    headers = {'Accept': 'application/json'}
    if TOKEN:
        headers[AUTH_HEADER] = TOKEN if AUTH_HEADER.lower() != 'authorization' else f'Bearer {TOKEN}'
    for attempt in range(5):
        try:
            r = client.get(f'{API_BASE}/{path.lstrip("/")}', params=params, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < 4:
                    pause = max(float(r.headers.get('Retry-After', '0') or 0), min(2 ** attempt * 3, 60))
                    time.sleep(pause)
                    continue
            if r.status_code in (401, 403):
                raise SystemExit(
                    f'CMTracker HTTP {r.status_code} em {path}: autenticação recusada. '
                    'Confira CMTRACKER_API_KEY nos Repository secrets e o formato do header '
                    '(CMTRACKER_AUTH_HEADER=x-api-key ou authorization). '
                    'A resposta não foi registrada para proteger credenciais.'
                )
            r.raise_for_status()
            return r.json()
        except (httpx.TimeoutException, httpx.NetworkError):
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt * 3, 60))
    raise RuntimeError('Limite de tentativas da API CMTracker')


def unpack_list(value, candidates):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in candidates:
            if isinstance(value.get(key), list):
                return value[key]
        for key in ('data', 'result'):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
            if isinstance(nested, dict):
                found = unpack_list(nested, candidates)
                if found is not None:
                    return found
    return None


def database_slug(db):
    for k in ('slug', 'dbSlug', 'db', 'databaseSlug'):
        if db.get(k) is not None:
            return str(db[k])
    return None


def get_latest_db(client):
    if DB_SLUG:
        return DB_SLUG
    response = fetch_json(client, DB_PATH)
    databases = unpack_list(response, ('databases', 'dbs', 'items'))
    if databases is None:
        raise ValueError('Formato de /dbs desconhecido; configure CMTRACKER_DB_SLUG')
    candidates = [db for db in databases if isinstance(db, dict) and database_slug(db)]
    # Slugs shown by CMTracker are sortable date-like strings (e.g. 26100100).
    numeric = [db for db in candidates if database_slug(db).isdigit() and len(database_slug(db)) == 8]
    if not numeric:
        raise ValueError('Não foi possível identificar slug numérico; configure CMTRACKER_DB_SLUG')
    return database_slug(max(numeric, key=lambda db: int(database_slug(db))))


def value_of(obj, keys):
    for k in keys:
        value = obj.get(k)
        if value is not None and value != '':
            return value
    return None


def normalize_player(raw):
    if not isinstance(raw, dict):
        return None
    # No guessing: only EA/SoFIFA IDs. A generic id may be internal to CMTracker.
    raw_id = value_of(raw, ('ea_player_id', 'eaPlayerId', 'sofifa_player_id', 'sofifaId'))
    if raw_id is None and os.getenv('CMTRACKER_ID_IS_EA_ID') == 'true':
        raw_id = raw.get('id')
    try:
        pid = int(raw_id)
        if pid <= 0:
            return None
    except (TypeError, ValueError):
        return None
    row = {'sofifa_player_id': pid}
    for col, aliases in FIELD_ALIASES.items():
        val = value_of(raw, aliases)
        if val is None or isinstance(val, (dict, list)) and col != 'positions':
            continue
        if col == 'positions':
            if isinstance(val, list):
                val = ','.join(str(x) for x in val if isinstance(x, (str, int)))
            elif not isinstance(val, str):
                continue
        elif col in ('oa', 'pt'):
            try:
                val = str(int(val))
            except (TypeError, ValueError):
                continue
        elif not isinstance(val, str):
            continue
        row[col] = val
    return row if len(row) > 1 else None


def sb_headers():
    return {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'}


def existing_players(sb, ids):
    params = {'sofifa_player_id': 'in.(' + ','.join(str(x) for x in ids) + ')',
              'select': ','.join(['sofifa_player_id', *FIELD_ALIASES.keys()])}
    r = sb.get(f'{SUPABASE_URL}/rest/v1/players', params=params, headers=sb_headers())
    r.raise_for_status()
    return {int(x['sofifa_player_id']): x for x in r.json()}


def update_existing(sb, pid, changes):
    url = f'{SUPABASE_URL}/rest/v1/players'
    r = sb.patch(url, params={'sofifa_player_id': f'eq.{pid}'}, json=changes,
                 headers={**sb_headers(), 'Prefer': 'return=minimal'})
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='Gravar alterações em jogadores existentes')
    ap.add_argument('--inspect', action='store_true', help='Exibir formato de uma amostra sem credenciais Supabase')
    args = ap.parse_args()
    if PAGE_SIZE < 1 or PAGE_SIZE > 100 or MAX_PAGES < 1:
        raise SystemExit('PAGE_SIZE deve ser 1..100; MAX_PAGES deve ser positivo')
    if not TOKEN:
        raise SystemExit('CMTRACKER_API_KEY ausente/vazio. Adicione em Settings > Secrets and variables > Actions > Repository secrets.')
    info(f'Credencial CMTracker: configurada | cabeçalho: {AUTH_HEADER}')
    if not args.inspect and (not SUPABASE_URL or not SUPABASE_KEY):
        raise SystemExit('SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios')
    if args.apply and os.getenv('CMTRACKER_ALLOW_WRITE') != 'YES':
        raise SystemExit('Gravação bloqueada; configure CMTRACKER_ALLOW_WRITE=YES')
    count = {'seen': 0, 'mapped': 0, 'changed': 0, 'unchanged': 0, 'not_in_supabase': 0, 'unmapped': 0}
    with httpx.Client(timeout=TIMEOUT) as api, httpx.Client(timeout=TIMEOUT) as sb:
        slug = get_latest_db(api)
        info(f'Versão CMTracker: {slug} | modo: {"APLICAR" if args.apply else "SIMULAR"}')
        visited = set()
        for page in range(FIRST_PAGE, FIRST_PAGE + (1 if args.inspect else MAX_PAGES)):
            payload = fetch_json(api, PLAYER_PATH, {DB_PARAM: slug, PAGE_PARAM: page, SIZE_PARAM: PAGE_SIZE})
            records = unpack_list(payload, ('players', 'items', 'results'))
            if records is None:
                info('Resposta: ' + json.dumps(payload, ensure_ascii=False, default=str)[:1800])
                raise ValueError('Formato de resposta de jogadores não reconhecido; nenhuma escrita foi realizada nesta página')
            if args.inspect:
                info('Chaves da resposta: ' + str(list(payload.keys()) if isinstance(payload, dict) else '[array]'))
                info('Primeiro jogador (campos): ' + str(list(records[0].keys()) if records and isinstance(records[0], dict) else 'vazio'))
                info('Primeiro jogador normalizado: ' + json.dumps(normalize_player(records[0]) if records else None, ensure_ascii=False))
                info('Total retornado: ' + str(len(records)))
                return
            if not records:
                info(f'Fim da lista na página {page}')
                break
            rows = [normalize_player(raw) for raw in records]
            rows = [r for r in rows if r]
            if len(rows) < len(records) * 0.9:
                raise ValueError('Mais de 10% da página sem identificador/mapeamento; escrita bloqueada')
            ids = [r['sofifa_player_id'] for r in rows]
            count['seen'] += len(records)
            count['mapped'] += len(rows)
            count['unmapped'] += len(records) - len(rows)
            # Fail closed on ambiguous / repeated pagination.
            current = set(ids)
            if current and visited.intersection(current):
                raise ValueError('IDs repetidos entre páginas: verifique paginação CMTracker')
            visited.update(current)
            prev = existing_players(sb, ids) if ids else {}
            for row in rows:
                pid = row.pop('sofifa_player_id')
                original = prev.get(pid)
                if not original:
                    count['not_in_supabase'] += 1
                    continue  # Protect cross-tenant contracts: no automatic inserts.
                changes = {k: v for k, v in row.items() if str(original.get(k) or '') != str(v)}
                if not changes:
                    count['unchanged'] += 1
                    continue
                count['changed'] += 1
                if args.apply:
                    update_existing(sb, pid, changes)
            info(f'Página {page}: {len(records)} registros, {len(rows)} mapeados, {count["changed"]} mudanças acumuladas')
            if len(records) < PAGE_SIZE:
                break
            time.sleep(DELAY)
    info(json.dumps({'database': slug, 'mode': 'apply' if args.apply else 'dry-run', **count}, ensure_ascii=False))
    if count['mapped'] == 0:
        raise SystemExit('Nenhum jogador mapeado: confira os nomes dos campos na API')
    if count['unmapped'] > count['seen'] * 0.1:
        raise SystemExit('Mais de 10% dos jogadores sem identificador confiável: verifique mapeamento')

if __name__ == '__main__':
    main()
