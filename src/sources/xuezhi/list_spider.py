import os
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

from src.common.hashing import safe_name, sha1_text

XUEZHI_MAJOR_URL = 'https://xz.chsi.com.cn/speciality/index.action'
XUEZHI_CAREER_URL = 'https://xz.chsi.com.cn/occupation/index.action'
MAJOR_API_PATH = '/speciality/list.action'
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

    detail_url = ''
    if spec_id:
        detail_url = f'https://xz.chsi.com.cn/speciality/detail.action?specId={spec_id}'

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
    session = _build_session(user_agent, referer)

    first_page_data = _fetch_json(session, template_url)
    first_page_items = _extract_items_from_payload(template_url, first_page_data, entity_type)
    first_page_rows = _get_rows_from_payload(first_page_data, entity_type)
    page_size = len(first_page_rows)

    if not first_page_items:
        return [], []

    all_items = []
    api_pages = []
    seen = set()

    def add_items(items):
        deduped = []
        for item in items:
            sig = item['item_id']
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(item)
        return deduped

    first_deduped = add_items(first_page_items)
    all_items.extend(first_deduped)
    api_pages.append({
        'page_no': 1,
        'url': template_url,
        'count': len(first_deduped),
    })

    if page_size <= 0:
        return all_items, api_pages

    page_no = 2
    while True:
        if max_pages and page_no > max_pages:
            break

        start = (page_no - 1) * page_size
        if entity_type == 'major':
            page_url = _replace_query(template_url, start=start)
        else:
            page_url = _replace_query(template_url, start=start, curPage=page_no, pageCount=page_size)

        try:
            page_data = _fetch_json(session, page_url)
        except Exception:
            break

        rows = _get_rows_from_payload(page_data, entity_type)
        if not rows:
            break

        items = _extract_items_from_payload(page_url, page_data, entity_type)
        deduped = add_items(items)
        if not deduped:
            break

        all_items.extend(deduped)
        api_pages.append({
            'page_no': page_no,
            'url': page_url,
            'count': len(deduped),
        })

        if len(rows) < page_size:
            break

        page_no += 1

    return all_items, api_pages


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

        items, api_pages = _paginate_items(
            entity_type=entity_type,
            template_url=template_url,
            user_agent=user_agent,
            referer=list_url,
            max_pages=max_pages,
        )

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


def crawl_major_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_MAJOR_URL, 'major', save_html=save_html)


def crawl_career_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_CAREER_URL, 'career', save_html=save_html)
