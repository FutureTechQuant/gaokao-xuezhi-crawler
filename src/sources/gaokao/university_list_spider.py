import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from src.common.hashing import safe_name, sha1_text

GAOKAO_UNIV_LIST_URL = (
    'https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-0.dhtml'
)


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def _extract_total_pages(page):
    text = page.locator('body').inner_text(timeout=30000)
    m = re.search(r'共\s*(\d+)\s*页', text)
    if m:
        return int(m.group(1))
    return 1


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

        page.goto(GAOKAO_UNIV_LIST_URL, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(2500)

        total_pages = _extract_total_pages(page)

        current_page_no = 1
        while True:
            raw_name = f'{current_page_no:04d}_{safe_name("university")}.html'
            html = page.content()
            if save_html:
                save_html(raw_name, html)

            all_pages.append({
                'name': raw_name,
                'url': page.url,
                'page_no': current_page_no,
            })

            items = _extract_page_items(
                page=page,
                list_url=page.url,
                raw_name=raw_name,
                page_no=current_page_no,
            )
            all_items.extend(items)

            if current_page_no >= total_pages:
                break

            next_link = page.locator('a:has-text("下一页")')
            if next_link.count() == 0:
                break

            next_link.first.click()
            page.wait_for_timeout(2500)
            current_page_no += 1

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
            'list_url': GAOKAO_UNIV_LIST_URL,
            'count': len(deduped),
            'items': deduped,
            'pages': all_pages,
        }
    finally:
        page.close()
