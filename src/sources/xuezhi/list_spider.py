import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from src.common.hashing import safe_name, sha1_text

XUEZHI_MAJOR_URL = 'https://xz.chsi.com.cn/speciality/index.action'
XUEZHI_CAREER_URL = 'https://xz.chsi.com.cn/occupation/index.action'
NAME_KEYS = ['name', 'title', 'zymc', 'mc', 'zwmc', 'occupationName', 'specialityName']
ID_KEYS = ['id', 'specialityId', 'occupationId', 'zyh', 'code', 'dm', 'zymdm']


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def _collect_response_payloads(page, keyword):
    payloads = []

    def on_response(response):
        try:
            ctype = response.headers.get('content-type', '')
            if 'json' not in ctype and keyword not in response.url:
                return
            data = response.json()
            payloads.append({'url': response.url, 'data': data})
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


def _extract_items_from_payloads(payloads, entity_type):
    results = []
    seen = set()
    for payload in payloads:
        for item in _iter_dicts(payload.get('data')):
            name = ''
            for k in NAME_KEYS:
                if k in item and clean_text(item.get(k)):
                    name = clean_text(item.get(k))
                    break
            if not name:
                continue
            item_id = ''
            for k in ID_KEYS:
                if k in item and clean_text(item.get(k)):
                    item_id = clean_text(item.get(k))
                    break
            detail_url = ''
            for key, value in item.items():
                if isinstance(value, str) and ('detail' in value or 'index.action' in value):
                    detail_url = value
                    break
            if detail_url and detail_url.startswith('/'):
                detail_url = urljoin(XUEZHI_MAJOR_URL if entity_type == 'major' else XUEZHI_CAREER_URL, detail_url)
            sig = (item_id, name)
            if sig in seen:
                continue
            seen.add(sig)
            results.append({
                'source': 'xuezhi',
                'entity_type': entity_type,
                'item_id': item_id,
                'name': name,
                'detail_url': detail_url,
                'payload_url': payload.get('url', ''),
                'raw': item,
                'collected_at': iso_now(),
            })
    return results


def _extract_items_from_dom(page, entity_type):
    keyword = 'speciality' if entity_type == 'major' else 'occupation'
    anchors = page.locator('a')
    results = []
    seen = set()
    for i in range(anchors.count()):
        a = anchors.nth(i)
        text = clean_text(a.inner_text())
        href = a.get_attribute('href') or ''
        if not text:
            continue
        full = urljoin(page.url, href) if href else ''
        if keyword not in full and len(text) < 2:
            continue
        if re.search(r'首页|查询|立即留言|取消|提交|专业洞察|职业探索|职业测评|职业人物|职业微视频|就业指导课|专题汇总', text):
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


def _crawl_entry(context, list_url, entity_type, save_html=None):
    page = context.new_page()
    try:
        payloads = _collect_response_payloads(page, 'action')
        page.goto(list_url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(3500)
        html = page.content()
        raw_name = f'0001_{safe_name(entity_type)}.html'
        if save_html:
            save_html(raw_name, html)
        items = _extract_items_from_payloads(payloads, entity_type)
        if not items:
            items = _extract_items_from_dom(page, entity_type)
        for idx, item in enumerate(items, start=1):
            item['row_no'] = idx
            item['list_url'] = page.url
            item['list_page_file'] = raw_name
            item['page_text_sha1'] = sha1_text(page.locator('body').inner_text(timeout=30000))
        return {
            'task': f'xuezhi_{entity_type}',
            'source': 'xuezhi',
            'entity_type': entity_type,
            'list_url': list_url,
            'count': len(items),
            'items': items,
            'pages': [{'name': raw_name, 'url': page.url}],
        }
    finally:
        page.close()


def crawl_major_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_MAJOR_URL, 'major', save_html=save_html)


def crawl_career_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_CAREER_URL, 'career', save_html=save_html)
