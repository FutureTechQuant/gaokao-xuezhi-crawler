import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from src.common.hashing import safe_name, sha1_text

GAOKAO_MAJOR_URL = 'https://gaokao.chsi.com.cn/zyk/zybk/'
GAOKAO_UNIVERSITY_URL = 'https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-0.dhtml'
LEVEL_NAMES = ['本科（普通教育）', '本科（职业教育）', '高职（专科）']
NAV_BLACKLIST = {
    '首页', '高考资讯', '阳光志愿', '高招咨询', '招生动态', '试题评析', '院校库', '专业库',
    '院校满意度', '专业满意度', '专业推荐', '更多', '招生政策', '选科参考', '云咨询周',
    '成绩查询', '招生章程', '名单公示', '志愿参考', '咨询室', '录取结果', '高职招生',
    '工作动态', '心理测评', '直播安排', '批次线', '专业解读', '各地网站', '职业前景',
    '特殊类型招生', '志愿填报时间', '招办访谈', '登录', '注册', '搜索', '查看', '取消',
    '基本信息', '开设院校', '开设课程', '图解专业', '选科要求', '更多>'
}
SCHOOL_NAME_RE = re.compile(r'(大学|学院|学校|职业大学|职业学院|高等专科学校|师范大学|师范学院|医学院|中医药大学)$')


def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()


def clean_text(text):
    if text is None:
        return ''
    return ' '.join(str(text).split()).strip()


def unique_keep_order(items, key_fn=None):
    seen = set()
    out = []
    for item in items:
        key = key_fn(item) if key_fn else repr(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _wait_major_home(page):
    page.goto(GAOKAO_MAJOR_URL, wait_until='domcontentloaded', timeout=60000)
    page.wait_for_selector('#app', timeout=30000)
    page.wait_for_function(
        """() => {
            const t = document.body ? document.body.innerText : '';
            return t.includes('专业知识库')
                && t.includes('本科（普通教育）专业目录')
                && t.includes('本科（职业教育）专业目录')
                && t.includes('高职（专科）专业目录');
        }""",
        timeout=60000,
    )
    page.wait_for_selector('.index-cc-list', timeout=30000)
    page.wait_for_timeout(1200)


def _wait_major_table(page):
    page.wait_for_selector('.zyk-table-con .ivu-table-body tbody tr', timeout=30000)
    page.wait_for_function(
        """
        () => {
            const rows = document.querySelectorAll('.zyk-table-con .ivu-table-body tbody tr');
            if (!rows.length) return false;
            const loading = document.querySelector('.ivu-spin-spinning') || document.querySelector('.ivu-spin-show-text');
            return !loading;
        }
        """,
        timeout=30000,
    )
    page.wait_for_timeout(600)


def _get_level_texts(page):
    items = page.locator('.index-cc-list li')
    out = []
    for i in range(items.count()):
        txt = clean_text(items.nth(i).inner_text())
        if txt:
            out.append(txt)
    return out


def _click_level_by_text(page, level_name):
    items = page.locator('.index-cc-list li')
    for i in range(items.count()):
        item = items.nth(i)
        txt = clean_text(item.inner_text())
        if txt == level_name:
            item.click()
            page.wait_for_timeout(800)
            return
    raise RuntimeError(f'未找到培养层次：{level_name}')


def _get_group(page, idx):
    return page.locator('.spec-list .zyk-lb-ul-con').nth(idx)


def _get_group_items_texts(group):
    items = group.locator('ul.zyk-lb-ul > li')
    out = []
    for i in range(items.count()):
        txt = clean_text(items.nth(i).inner_text())
        if txt:
            out.append(txt)
    return out


def _click_group_item_by_text(group, text):
    items = group.locator('ul.zyk-lb-ul > li')
    for i in range(items.count()):
        item = items.nth(i)
        txt = clean_text(item.inner_text())
        if txt == text:
            item.click()
            return
    raise RuntimeError(f'未找到分组项：{text}')


def _extract_spec_id(detail_href, school_href):
    for url in [detail_href, school_href]:
        if not url:
            continue
        m = re.search(r'specId=(\d+)', url)
        if m:
            return m.group(1)
        m = re.search(r'/detail/(\d+)', url)
        if m:
            return m.group(1)
    return ''


def _extract_major_table_rows(page, level_name, discipline, major_class):
    row_data = page.locator('.zyk-table-con .ivu-table-body tbody tr').evaluate_all(
        """
        rows => rows.map(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            const majorA = tds[0]?.querySelector('a');
            const schoolA = tds[2]?.querySelector('a');
            return {
                cell_count: tds.length,
                major_name: (tds[0]?.innerText || '').trim(),
                major_code: (tds[1]?.innerText || '').trim(),
                school_text: (tds[2]?.innerText || '').trim(),
                satisfaction: (tds[3]?.innerText || '').trim(),
                detail_href: majorA?.getAttribute('href') || '',
                school_href: schoolA?.getAttribute('href') || '',
            };
        })
        """
    )
    rows = []
    for item in row_data:
        if item.get('cell_count', 0) < 4:
            continue
        major_name = clean_text(item.get('major_name', ''))
        major_code = clean_text(item.get('major_code', ''))
        school_text = clean_text(item.get('school_text', ''))
        satisfaction = clean_text(item.get('satisfaction', ''))
        if not major_name or '暂无' in major_name:
            continue
        detail_href = urljoin(GAOKAO_MAJOR_URL, item.get('detail_href', '')) if item.get('detail_href') else ''
        school_href = urljoin(GAOKAO_MAJOR_URL, item.get('school_href', '')) if item.get('school_href') else ''
        spec_id = _extract_spec_id(detail_href, school_href)
        if not school_href and spec_id:
            school_href = f'https://gaokao.chsi.com.cn/zyk/zybk/ksyxPage?specId={spec_id}'
        rows.append({
            'source': 'gaokao',
            'entity_type': 'major',
            'spec_id': spec_id,
            'level_name': level_name,
            'discipline_name': discipline,
            'major_class_name': major_class,
            'major_name': major_name,
            'major_code': major_code,
            'major_satisfaction': satisfaction,
            'school_text': school_text,
            'detail_url': detail_href,
            'school_list_url': school_href,
            'list_url': page.url,
            'collected_at': iso_now(),
        })
    return rows


def crawl_major_list(context, save_html=None):
    page = context.new_page()
    try:
        _wait_major_home(page)
        levels_found = [name for name in LEVEL_NAMES if name in _get_level_texts(page)]
        items = []
        raw_pages = []
        seen = set()
        for level_name in levels_found:
            _click_level_by_text(page, level_name)
            discipline_group = _get_group(page, 0)
            discipline_texts = _get_group_items_texts(discipline_group)
            for discipline in discipline_texts:
                discipline_group = _get_group(page, 0)
                _click_group_item_by_text(discipline_group, discipline)
                page.wait_for_timeout(700)
                class_group = _get_group(page, 1)
                class_texts = _get_group_items_texts(class_group)
                for major_class in class_texts:
                    class_group = _get_group(page, 1)
                    _click_group_item_by_text(class_group, major_class)
                    _wait_major_table(page)
                    html = page.content()
                    raw_name = f"{len(raw_pages)+1:04d}_{safe_name(level_name)}_{safe_name(discipline)}_{safe_name(major_class)}.html"
                    raw_pages.append({
                        'name': raw_name,
                        'url': page.url,
                        'level_name': level_name,
                        'discipline_name': discipline,
                        'major_class_name': major_class,
                        'html': html,
                    })
                    for row in _extract_major_table_rows(page, level_name, discipline, major_class):
                        key = row['spec_id'] or (row['level_name'], row['discipline_name'], row['major_class_name'], row['major_name'], row['major_code'])
                        if key in seen:
                            continue
                        seen.add(key)
                        row['list_page_file'] = raw_name
                        items.append(row)
        items = unique_keep_order(items, key_fn=lambda x: x.get('spec_id') or x.get('detail_url') or x.get('major_name'))
        if save_html:
            for raw in raw_pages:
                save_html(raw['name'], raw['html'])
        return {
            'task': 'gaokao_major',
            'source': 'gaokao',
            'entity_type': 'major',
            'list_url': GAOKAO_MAJOR_URL,
            'count': len(items),
            'items': items,
            'pages': [{k: v for k, v in p.items() if k != 'html'} for p in raw_pages],
        }
    finally:
        page.close()


def crawl_university_list(context, save_html=None, max_pages=0):
    page = context.new_page()
    try:
        page.goto(GAOKAO_UNIVERSITY_URL, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(1500)
        items = []
        raw_pages = []
        seen = set()
        page_no = 1
        while True:
            body_text = page.locator('body').inner_text(timeout=30000)
            html = page.content()
            raw_name = f'{page_no:04d}.html'
            raw_pages.append({'name': raw_name, 'url': page.url, 'page_no': page_no, 'html': html})
            anchors = page.locator('a')
            for i in range(anchors.count()):
                a = anchors.nth(i)
                text = clean_text(a.inner_text())
                href = a.get_attribute('href') or ''
                if not text or text in NAV_BLACKLIST:
                    continue
                if not SCHOOL_NAME_RE.search(text):
                    continue
                full = urljoin(page.url, href) if href else ''
                key = full or text
                if key in seen:
                    continue
                seen.add(key)
                items.append({
                    'source': 'gaokao',
                    'entity_type': 'university',
                    'school_name': text,
                    'detail_url': full,
                    'list_url': page.url,
                    'page_no': page_no,
                    'collected_at': iso_now(),
                    'page_text_sha1': sha1_text(body_text),
                    'list_page_file': raw_name,
                })
            next_btn = page.locator('.pages a.next, .ivu-page-next:not(.ivu-page-disabled), a.next-page')
            if max_pages and page_no >= max_pages:
                break
            if next_btn.count() == 0:
                break
            try:
                next_btn.first.click()
                page.wait_for_timeout(1200)
                page_no += 1
            except Exception:
                break
        if save_html:
            for raw in raw_pages:
                save_html(raw['name'], raw['html'])
        return {
            'task': 'gaokao_university',
            'source': 'gaokao',
            'entity_type': 'university',
            'list_url': GAOKAO_UNIVERSITY_URL,
            'count': len(items),
            'items': items,
            'pages': [{k: v for k, v in p.items() if k != 'html'} for p in raw_pages],
        }
    finally:
        page.close()
