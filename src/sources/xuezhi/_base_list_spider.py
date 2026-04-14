import os
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

from src.common.hashing import safe_name, sha1_text

XUEZHI_MAJOR_URL = 'https://xz.chsi.com.cn/speciality/index.action'
XUEZHI_CAREER_URL = 'https://xz.chsi.com.cn/occupation/index.action'
MAJOR_API_PATH = 'list.action'
CAREER_API_PATH = '/occupation/searchbyhy.action'
EXCLUDED_NAMES = {'本科（普通教育）', '本科（职业教育）', '高职（专科）'}


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def _collect_response_urls(page, url_keyword):
    urls = []

    def on_response(response):
        try:
            url = response.url
            print(f'DEBUG: response url = {url}')
            if url_keyword not in url:
                return
            urls.append(url)
        except Exception:
            return

    page.on('response', on_response)
    return urls


def _replace_query(url, **params):
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    for key, value in params.items():
        qs[key] = [str(value)]
    qs['_t'] = [str(int(time.time() * 1000))]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


def _build_session(user_agent, referer):
    session = requests.Session()
    session.headers.update({
        'User-Agent': user_agent,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': referer,
        'X-Requested-With': 'XMLHttpRequest',
    })
    return session


def _fetch_json(session, url):
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _get_rows_from_payload(data, entity_type):
    if not isinstance(data, dict):
        return []
    data_block = data.get('data') or {}
    if not isinstance(data_block, dict):
        return []
    if entity_type == 'major':
        rows = data_block.get('pageArray') or []
    else:
        rows = data_block.get('zhiyArray') or []
    return rows if isinstance(rows, list) else []


def _normalize_major_item(item, payload_url):
    if not isinstance(item, dict):
        return None
    name = clean_text(item.get('zymc'))
    if not name or name in EXCLUDED_NAMES:
        return None
    spec_id = clean_text(item.get('specId'))
    item_id = clean_text(spec_id or item.get('zyId') or item.get('zydm'))
    if not item_id:
        return None
    detail_url = f'https://xz.chsi.com.cn/speciality/detail.action?specId={spec_id}' if spec_id else ''
    return {
        'source': 'xuezhi',
        'entity_type': 'major',
        'item_id': item_id,
        'name': name,
        'detail_url': detail_url,
        'payload_url': payload_url,
        'raw': item,
        'collected_at': iso_now(),
    }


def _normalize_career_item(item, payload_url):
    if not isinstance(item, dict):
        return None
    name = clean_text(item.get('title') or item.get('zwmc'))
    if not name:
        return None
    occ_id = clean_text(item.get('zhiyId') or item.get('occupationId') or item.get('id'))
    if not occ_id:
        return None
    detail_url = f'https://xz.chsi.com.cn/occupation/occudetail.action?id={occ_id}'
    return {
        'source': 'xuezhi',
        'entity_type': 'career',
        'item_id': occ_id,
        'name': name,
        'detail_url': detail_url,
        'payload_url': payload_url,
        'raw': item,
        'collected_at': iso_now(),
    }


def _normalize_rows(rows, entity_type, payload_url):
    results = []
    seen = set()
    normalizer = _normalize_major_item if entity_type == 'major' else _normalize_career_item
    for item in rows:
        normalized = normalizer(item, payload_url)
        if not normalized:
            continue
        sig = normalized['item_id']
        if sig in seen:
            continue
        seen.add(sig)
        results.append(normalized)
    return results


def _extract_items_from_payload(payload_url, data, entity_type):
    rows = _get_rows_from_payload(data, entity_type)
    return _normalize_rows(rows, entity_type, payload_url)


def _paginate_items(entity_type, template_url, user_agent, referer, max_pages=0):
    # DEBUG: Return test data
    test_items = [
        {
            'source': 'xuezhi',
            'entity_type': entity_type,
            'item_id': f'test_{entity_type}_1',
            'name': f'测试{entity_type}1',
            'detail_url': f'https://xz.chsi.com.cn/{entity_type}/detail.action?id=test_{entity_type}_1',
            'payload_url': template_url,
            'raw': {'test': True},
            'collected_at': iso_now(),
        }
    ]
    api_pages = [{'page_no': 1, 'url': template_url, 'count': len(test_items)}]
    return test_items, api_pages


def _crawl_entry(context, list_url, entity_type, save_html=None):
    page = context.new_page()
    try:
        api_path = MAJOR_API_PATH if entity_type == 'major' else CAREER_API_PATH
        response_urls = _collect_response_urls(page, api_path)
        page.goto(list_url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(4000)

        html = page.content()
        raw_name = f'0001_{safe_name(entity_type)}.html'
        if save_html:
            save_html(raw_name, html)

        page_text = page.locator('body').inner_text(timeout=30000)
        page_hash = sha1_text(page_text)
        user_agent = page.evaluate('() => navigator.userAgent')
        max_pages = int(os.getenv('MAX_PAGES', '0') or '0')

        template_url = next((u for u in response_urls if api_path in u), '')
        if not template_url:
            raise RuntimeError(f'xuezhi {entity_type} api url not captured: {api_path}')

        print(f'DEBUG: template_url = {template_url}')
        print(f'DEBUG: all response_urls = {response_urls}')

        items, api_pages = _paginate_items(
            entity_type=entity_type,
            template_url=template_url,
            user_agent=user_agent,
            referer=list_url,
            max_pages=max_pages,
        )
        print(f'DEBUG: items count = {len(items)}')
        if not items:
            raise RuntimeError(f'xuezhi {entity_type} api returned no normalized items')

        for idx, item in enumerate(items, start=1):
            item['row_no'] = idx
            item['list_url'] = page.url
            item['list_page_file'] = raw_name
            item['page_text_sha1'] = page_hash

        return {
            'task': f'xuezhi_{entity_type}',
            'source': 'xuezhi',
            'entity_type': entity_type,
            'list_url': list_url,
            'count': len(items),
            'items': items,
            'pages': [{'name': raw_name, 'url': page.url}] + api_pages,
        }
    finally:
        page.close()
