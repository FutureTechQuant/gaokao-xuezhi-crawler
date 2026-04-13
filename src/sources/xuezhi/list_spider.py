import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

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


def _collect_response_payloads(page, url_keyword):
    payloads = []

    def on_response(response):
        try:
            url = response.url
            if url_keyword not in url:
                return
            ctype = (response.headers.get('content-type') or '').lower()
            if 'json' not in ctype and 'javascript' not in ctype and 'text/plain' not in ctype:
                return
            payloads.append({'url': url, 'data': response.json()})
        except Exception:
            return

    page.on('response', on_response)
    return payloads


def _iter_dicts(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_dicts(item)


def _normalize_major_item(item, payload_url):
    if not isinstance(item, dict):
        return None
    name = clean_text(item.get('zymc'))
    if not name or name in EXCLUDED_NAMES:
        return None
    item_id = clean_text(item.get('specId') or item.get('zyId') or item.get('zydm'))
    if not item_id:
        return None
    return {
        'source': 'xuezhi',
        'entity_type': 'major',
        'item_id': item_id,
        'name': name,
        'detail_url': '',
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
    item_id = clean_text(item.get('zhiyId') or item.get('occupationId') or item.get('id'))
    if not item_id:
        return None
    return {
        'source': 'xuezhi',
        'entity_type': 'career',
        'item_id': item_id,
        'name': name,
        'detail_url': '',
        'payload_url': payload_url,
        'raw': item,
        'collected_at': iso_now(),
    }


def _extract_items_from_payloads(payloads, entity_type):
    results = []
    seen = set()
    normalizer = _normalize_major_item if entity_type == 'major' else _normalize_career_item
    for payload in payloads:
        payload_url = payload.get('url', '')
        for item in _iter_dicts(payload.get('data')):
            normalized = normalizer(item, payload_url)
            if not normalized:
                continue
            sig = normalized['item_id'] or normalized['name']
            if sig in seen:
                continue
            seen.add(sig)
            results.append(normalized)
    return results


def _extract_items_from_dom(page, entity_type):
    anchors = page.locator('a')
    results = []
    seen = set()
    for i in range(anchors.count()):
        a = anchors.nth(i)
        text = clean_text(a.inner_text())
        href = a.get_attribute('href') or ''
        if not text or text in EXCLUDED_NAMES:
            continue
        full = urljoin(page.url, href) if href else ''
        if re.search(r'首页|查询|立即留言|取消|提交|专业洞察|职业探索|职业测评|职业人物|职业微视频|就业指导课|专题汇总', text):
            continue
        if entity_type == 'major':
            if 'speciality' not in full or 'index.action' in full:
                continue
        else:
            if 'occupation' not in full or 'index.action' in full:
                continue
        sig = (text, full)
        if sig in seen:
            continue
        seen.add(sig)
        results.append({
            'source': 'xuezhi',
            'entity_type': entity_type,
            'item_id': '',
            'name': text,
            'detail_url': full,
            'payload_url': '',
            'raw': {},
            'collected_at': iso_now(),
        })
    return results


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


def _paginate_items(entity_type, template_url, first_page_items, user_agent, referer, max_pages=0):
    all_items = list(first_page_items)
    api_pages = []
    if not template_url or not first_page_items:
        return all_items, api_pages

    page_size = len(first_page_items)
    seen = {item.get('item_id') or item.get('name') for item in all_items}
    session = _build_session(user_agent, referer)
    page_no = 1
    api_pages.append({'page_no': 1, 'url': template_url, 'count': len(first_page_items)})

    while True:
        if max_pages and page_no >= max_pages:
            break
        next_page = page_no + 1
        start = (next_page - 1) * page_size
        if entity_type == 'major':
            next_url = _replace_query(template_url, start=start)
        else:
            next_url = _replace_query(template_url, start=start, curPage=next_page, pageCount=page_size)
        try:
            response = session.get(next_url, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception:
            break
        new_items = _extract_items_from_payloads([{'url': next_url, 'data': data}], entity_type)
        deduped = []
        for item in new_items:
            sig = item.get('item_id') or item.get('name')
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(item)
        if not deduped:
            break
        all_items.extend(deduped)
        api_pages.append({'page_no': next_page, 'url': next_url, 'count': len(deduped)})
        if len(new_items) < page_size:
            break
        page_no = next_page

    return all_items, api_pages


def _crawl_entry(context, list_url, entity_type, save_html=None):
    page = context.new_page()
    try:
        api_path = MAJOR_API_PATH if entity_type == 'major' else CAREER_API_PATH
        payloads = _collect_response_payloads(page, api_path)
        page.goto(list_url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(3500)

        html = page.content()
        raw_name = f'0001_{safe_name(entity_type)}.html'
        if save_html:
            save_html(raw_name, html)

        page_text = page.locator('body').inner_text(timeout=30000)
        page_hash = sha1_text(page_text)
        user_agent = page.evaluate('() => navigator.userAgent')
        max_pages = int(os.getenv('MAX_PAGES', '0') or '0')

        items = _extract_items_from_payloads(payloads, entity_type)
        api_pages = []
        template_url = next((p['url'] for p in payloads if api_path in p.get('url', '')), '')
        if items and template_url:
            items, api_pages = _paginate_items(
                entity_type=entity_type,
                template_url=template_url,
                first_page_items=items,
                user_agent=user_agent,
                referer=list_url,
                max_pages=max_pages,
            )
        elif not items:
            items = _extract_items_from_dom(page, entity_type)

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
