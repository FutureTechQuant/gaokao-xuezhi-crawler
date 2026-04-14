import json
from pathlib import Path

import requests


HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/123.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}


def read_jsonl(path):
    text = Path(path).read_text(encoding='utf-8').strip()
    if not text:
        return []

    decoder = json.JSONDecoder()
    rows = []
    i = 0
    n = len(text)

    while i < n:
        while i < n and text[i].isspace():
            i += 1

        if i >= n:
            break

        try:
            obj, end = decoder.raw_decode(text, i)
            rows.append(obj)
            i = end
            continue
        except json.JSONDecodeError:
            pass

        if text.startswith('\\n', i):
            i += 2
            continue

        next_pos = text.find('{', i + 1)
        if next_pos == -1:
            break
        i = next_pos

    return rows

def run_one(row, referer, label):
    url = row['detail_url']
    headers = dict(HEADERS)
    headers['Referer'] = referer

    print('=' * 80)
    print('LABEL:', label)
    print('URL:', url)

    resp = requests.get(url, headers=headers, timeout=60)
    print('STATUS:', resp.status_code)
    resp.raise_for_status()

    if not resp.encoding or resp.encoding.lower() == 'iso-8859-1':
        resp.encoding = resp.apparent_encoding or 'utf-8'

    html = resp.text
    print('LEN:', len(html))
    print('HAS <title>:', '<title' in html.lower())
    print('HAS body:', '<body' in html.lower())

    keywords = ['专业', '职业', '就业', '课程', '培养', '方向', '介绍']
    for kw in keywords:
        print(f'HAS {kw}:', kw in html)

    out_dir = Path('output/xuezhi_requests_test')
    out_dir.mkdir(parents=True, exist_ok=True)
    name = row.get('item_id') or label
    out_path = out_dir / f'{name}.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print('SAVED:', out_path)


def main():
    candidates = [
        ('xuezhi_major', Path('data/stage/01_list_index/2026-04-14T005710/xuezhi_major.jsonl'), 'https://xz.chsi.com.cn/speciality/index.action'),
        ('xuezhi_career', Path('data/stage/01_list_index/2026-04-14T005748/xuezhi_career.jsonl'), 'https://xz.chsi.com.cn/occupation/index.action'),
    ]

    found_any = False

    for label, path, referer in candidates:
        print('-' * 80)
        print('CHECK:', label, path)
        print('EXISTS:', path.exists())

        if not path.exists():
            continue

        found_any = True
        rows = read_jsonl(path)
        print('ROWS:', len(rows))

        if not rows:
            print('EMPTY FILE:', path)
            continue

        for i, row in enumerate(rows[:3], start=1):
            try:
                test_one(row, referer, f'{label}_{i}')
            except Exception as e:
                print('ERROR:', label, i, row.get('detail_url'), repr(e))

    if not found_any:
        print('NO INPUT FILE FOUND')


if __name__ == '__main__':
    main()