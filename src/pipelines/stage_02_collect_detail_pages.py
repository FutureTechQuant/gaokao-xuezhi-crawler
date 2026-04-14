import argparse
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

from src.common.logger import get_logger
from src.common.paths import make_run_id, run_dir
from src.sources.gaokao.major_detail_spider import crawl_major_detail
from src.storage.manifest import build_manifest
from src.storage.writer import write_json, write_jsonl, write_text

logger = get_logger(__name__)


def _browser_context(playwright, headless=True):
    browser = playwright.chromium.launch(
        headless=headless,
        args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled'],
    )
    context = browser.new_context(
        viewport={'width': 1440, 'height': 900},
        locale='zh-CN',
        timezone_id='Asia/Shanghai',
        user_agent=(
            'Mozilla/5.0 (X11; Linux x86_64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/123.0.0.0 Safari/537.36'
        ),
    )
    return browser, context


def _save_html_factory(base_dir: Path):
    def save_html(name: str, html: str):
        write_text(base_dir / name, html)
    return save_html


def _read_jsonl(path: Path):
    rows = []
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _detail_raw_dir(source: str, entity: str, run_id: str) -> Path:
    return Path('data') / 'raw' / source / entity / 'detail_pages' / run_id


def _detail_index_dir(run_id: str) -> Path:
    return Path('data') / 'stage' / '02_detail_index' / run_id


def _task_specs():
    return {
        'gaokao_major': ('gaokao', 'major', crawl_major_detail),
    }


def _default_input_path(target: str) -> Path:
    latest_link = Path('data') / 'stage' / '01_list_index' / 'latest' / f'{target}.jsonl'
    return latest_link


def run(target='gaokao_major', input_path=None, run_id=None, headless=None, limit=0):
    run_id = run_id or make_run_id()
    if headless is None:
        headless = os.getenv('HEADLESS', '1') == '1'

    specs = _task_specs()
    source, entity, func = specs[target]

    input_path = Path(input_path) if input_path else _default_input_path(target)
    rows = _read_jsonl(input_path)

    if limit and limit > 0:
        rows = rows[:limit]

    raw_dir = _detail_raw_dir(source, entity, run_id)
    index_dir = _detail_index_dir(run_id)
    index_path = index_dir / f'{target}.jsonl'
    meta_path = index_dir / f'{target}.meta.json'

    logger.info('start target=%s input=%s raw_dir=%s', target, input_path, raw_dir)

    results = []
    failed = []

    with sync_playwright() as p:
        browser, context = _browser_context(p, headless=headless)
        try:
            for idx, row in enumerate(rows, start=1):
                file_name = f'{idx:08d}.html'
                try:
                    result = func(
                        context,
                        row=row,
                        save_html=_save_html_factory(raw_dir),
                        file_name=file_name,
                    )
                    result['detail_row_no'] = idx
                    results.append(result)
                    logger.info('done detail idx=%s url=%s', idx, row.get('detail_url'))
                except Exception as e:
                    failed.append({
                        'detail_row_no': idx,
                        'detail_url': row.get('detail_url'),
                        'error': str(e),
                    })
                    logger.exception('failed detail idx=%s url=%s', idx, row.get('detail_url'))
        finally:
            context.close()
            browser.close()

    write_jsonl(index_path, results)
    write_json(meta_path, {
        'task': target,
        'source': source,
        'entity_type': entity,
        'input_path': str(input_path),
        'count': len(results),
        'failed_count': len(failed),
        'failed': failed,
    })

    manifest = build_manifest('stage_02_collect_detail_pages', run_id, [{
        'task': target,
        'source': source,
        'entity_type': entity,
        'count': len(results),
        'failed_count': len(failed),
        'index_path': str(index_path),
        'meta_path': str(meta_path),
        'raw_dir': str(raw_dir),
    }])
    manifest_path = run_dir(run_id) / 'stage_02_manifest.json'
    write_json(manifest_path, manifest)
    logger.info('manifest=%s', manifest_path)

    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', default='gaokao_major', choices=['gaokao_major'])
    parser.add_argument('--input-path', default=None)
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--headed', action='store_true')
    args = parser.parse_args()

    run(
        target=args.target,
        input_path=args.input_path,
        run_id=args.run_id,
        headless=not args.headed,
        limit=args.limit,
    )


if __name__ == '__main__':
    main()