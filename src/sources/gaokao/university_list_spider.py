from datetime import datetime, timezone
from urllib.parse import urljoin

from src.common.hashing import safe_name, sha1_text

GAOKAO_UNIV_URL_TEMPLATE = (
    'https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-{start}.dhtml'
)

PAGE_SIZE = 20
LAST_START = 2900


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def _extract_page_items(page, list_url, raw_name, page_no):
    cards = page.locator('a[href*="schoolInfo--schId-"]')
    items = []
    seen = set()

    count = cards.count()
    body_text_hash = sha1_text(page.locator('body').inner_text(timeout=30000))

    for i in range(count):
        a = cards.nth(i)
        name = clean_text(a.inner_text())
        href = a.get_attribute('href') or ''
        if not name or not href:
            continue

        detail_url = urljoin(page.url, href)
        sig = (name, detail_url)
        if sig in seen:
            continue
        seen.add(sig)

        items.append({
            'source': 'gaokao',
            'entity_type': 'university',
            'school_name': name,
            'detail_url': detail_url,
            'list_url': list_url,
            'page_no': page_no,
            'collected_at': iso_now(),
            'page_text_sha1': body_text_hash,
            'list_page_file': raw_name,
        })

    return items


def crawl_university_list(context, save_html=None):
    page = context.new_page()
    try:
        all_items = []
        all_pages = []

        starts = list(range(0, LAST_START + PAGE_SIZE, PAGE_SIZE))

        for idx, start in enumerate(starts, start=1):
            list_url = GAOKAO_UNIV_URL_TEMPLATE.format(start=start)
            page.goto(list_url, wait_until='domcontentloaded', timeout=60000)
            page.wait_for_timeout(2000)

            raw_name = f'{idx:04d}_{safe_name("university")}.html'
            html = page.content()
            if save_html:
                save_html(raw_name, html)

            items = _extract_page_items(
                page=page,
                list_url=list_url,
                raw_name=raw_name,
                page_no=idx,
            )

            all_pages.append({
                'name': raw_name,
                'url': list_url,
                'page_no': idx,
                'start': start,
                'count': len(items),
            })

            if not items:
                continue

            all_items.extend(items)

        deduped = []
        seen = set()
        for item in all_items:
            sig = item['detail_url']
            if sig in seen:
                continue
            seen.add(sig)
            item['row_no'] = len(deduped) + 1
            deduped.append(item)

        return {
            'task': 'gaokao_university',
            'source': 'gaokao',
            'entity_type': 'university',
            'list_url': GAOKAO_UNIV_URL_TEMPLATE.format(start=0),
            'count': len(deduped),
            'items': deduped,
            'pages': all_pages,
        }
    finally:
        page.close()
