from datetime import datetime, timezone

from src.common.hashing import sha1_text


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def _crawl_detail_page(context, row, save_html=None, file_name=None):
    detail_url = row.get('detail_url')
    if not detail_url:
        raise ValueError('missing detail_url in list row')

    page = context.new_page()
    try:
        page.goto(detail_url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(2500)

        html = page.content()
        page_text = page.locator('body').inner_text(timeout=30000)
        page_text_sha1 = sha1_text(page_text)

        if save_html and file_name:
            save_html(file_name, html)

        return {
            'source': row.get('source', ''),
            'entity_type': row.get('entity_type', ''),
            'name': row.get('name') or row.get('major_name') or row.get('school_name') or '',
            'item_id': row.get('item_id') or row.get('spec_id') or row.get('occupation_id') or row.get('occ_id') or '',
            'detail_url': detail_url,
            'detail_page_file': file_name or '',
            'detail_page_sha1': page_text_sha1,
            'collected_at': iso_now(),
            'list_row_no': row.get('row_no'),
            'list_page_file': row.get('list_page_file', ''),
            'list_url': row.get('list_url', ''),
        }
    finally:
        page.close()


def crawl_major_detail(context, row, save_html=None, file_name=None):
    return _crawl_detail_page(context, row, save_html=save_html, file_name=file_name)


def crawl_career_detail(context, row, save_html=None, file_name=None):
    return _crawl_detail_page(context, row, save_html=save_html, file_name=file_name)
