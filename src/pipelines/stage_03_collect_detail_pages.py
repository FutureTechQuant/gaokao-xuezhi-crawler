import argparse
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

from src.common.logger import get_logger
from src.common.paths import make_run_id, run_dir
from src.sources.gaokao.detail_spider import crawl_university_detail as crawl_gaokao_university_detail
from src.sources.gaokao.major_detail_spider import crawl_major_detail as crawl_gaokao_major_detail
from src.sources.xuezhi.detail_spider import crawl_career_detail as crawl_xuezhi_career_detail
from src.sources.xuezhi.detail_spider import crawl_major_detail as crawl_xuezhi_major_detail
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
    if not path.exists():
        raise FileNotFoundError(f'input path not found: {path}')
    if not path.is_file():
        raise IsADirectoryError(f'input path is not a file: {path}')

    text = path.read_text(encoding='utf-8').strip()
    if not text:
        return []

    # Check if it's a JSON array
    if text.startswith('['):
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

    # Parse as concatenated JSON objects separated by '\n{'
    rows = []
    decoder = json.JSONDecoder()
    i = 0
    n = len(text)

    while i < n:
        # Skip any leading whitespace
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break

        try:
            obj, end = decoder.raw_decode(text, i)
            rows.append(obj)
            i = end
            # Skip the '\n{' separator if present
            if i + 2 <= n and text[i:i+2] == '\\n':
                i += 2
        except json.JSONDecodeError as e:
            raise ValueError(f'Invalid JSON in {path} at position {i}: {str(e)}')

    return rows


def _detail_raw_dir(source: str, entity: str, run_id: str) -> Path:
    return Path('data') / 'raw' / source / entity / 'detail_pages' / run_id


def _detail_index_dir(run_id: str) -> Path:
    return Path('data') / 'stage' / '03_detail_index' / run_id


def _task_specs():
    return {
        'gaokao_major': ('gaokao', 'major', crawl_gaokao_major_detail),
        'gaokao_university': ('gaokao', 'university', crawl_gaokao_university_detail),
        'xuezhi_major': ('xuezhi', 'major', crawl_xuezhi_major_detail),
        'xuezhi_career': ('xuezhi', 'career', crawl_xuezhi_career_detail),
    }


def _find_latest_index(base_dir: Path, target: str) -> Path | None:
    latest_path = base_dir / 'latest' / f'{target}.jsonl'
    if latest_path.exists():
        return latest_path

    if not base_dir.exists():
        return None

    candidate_dirs = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and d.name != 'latest'],
        reverse=True,
    )
    for candidate in candidate_dirs:
        candidate_file = candidate / f'{target}.jsonl'
        if candidate_file.exists():
            return candidate_file
    return None


def _default_input_path(target: str) -> Path:
    # DEBUG: Use test file for xuezhi_major
    if target == 'xuezhi_major':
        return Path('data/stage/01_list_index/2026-04-14T021947/xuezhi_major_test.jsonl')
    
    # Try latest first
    preferred = _find_latest_index(Path('data') / 'stage' / '02_list_index', target)
    if preferred is not None:
        return preferred

    fallback = _find_latest_index(Path('data') / 'stage' / '01_list_index', target)
    if fallback is not None:
        return fallback

    # If no latest, use the most recent run
    base_dir = Path('data') / 'stage' / '01_list_index'
    if not base_dir.exists():
        raise FileNotFoundError(f'input list index not found for target={target}')

    candidate_dirs = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and d.name != 'latest'],
        reverse=True,
    )
    for candidate in candidate_dirs:
        candidate_file = candidate / f'{target}.jsonl'
        if candidate_file.exists():
            return candidate_file

    raise FileNotFoundError(f'input list index not found for target={target}')


def run(target='all', input_path=None, run_id=None, headless=None, limit=0):
    run_id = run_id or make_run_id()
    if headless is None:
        headless = os.getenv('HEADLESS', '1') == '1'

    specs = _task_specs()
    selected = list(specs.keys()) if target == 'all' else [target]
    stage_dir = _detail_index_dir(run_id)

    tasks_summary = []
    with sync_playwright() as p:
        browser, context = _browser_context(p, headless=headless)
        try:
            for task_name in selected:
                source, entity, func = specs[task_name]
                input_path = Path(input_path) if input_path else _default_input_path(task_name)
                rows = _read_jsonl(input_path)
                if limit and limit > 0:
                    rows = rows[:limit]

                raw_dir = _detail_raw_dir(source, entity, run_id)
                index_path = stage_dir / f'{task_name}.jsonl'
                meta_path = stage_dir / f'{task_name}.meta.json'

                logger.info('start task=%s input=%s raw_dir=%s', task_name, input_path, raw_dir)

                results = []
                failed = []
                for idx, row in enumerate(rows, start=1):
                    # Skip rows with empty detail_url
                    if not row.get('detail_url', '').strip():
                        logger.info('skip detail idx=%s (empty url)', idx)
                        continue

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

                write_jsonl(index_path, results)
                write_json(meta_path, {
                    'task': task_name,
                    'source': source,
                    'entity_type': entity,
                    'input_path': str(input_path),
                    'count': len(results),
                    'failed_count': len(failed),
                    'failed': failed,
                })

                tasks_summary.append({
                    'task': task_name,
                    'source': source,
                    'entity_type': entity,
                    'count': len(results),
                    'failed_count': len(failed),
                    'index_path': str(index_path),
                    'meta_path': str(meta_path),
                    'raw_dir': str(raw_dir),
                })
                logger.info('done task=%s count=%s', task_name, len(results))
        finally:
            context.close()
            browser.close()

    manifest = build_manifest('stage_03_collect_detail_pages', run_id, tasks_summary)
    manifest_path = run_dir(run_id) / 'stage_03_manifest.json'
    write_json(manifest_path, manifest)
    logger.info('manifest=%s', manifest_path)
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', default='all', choices=['all', 'gaokao_major', 'gaokao_university', 'xuezhi_major', 'xuezhi_career'])
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
