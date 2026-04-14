import argparse
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

from src.common.logger import get_logger
from src.common.paths import list_index_dir, list_raw_dir, make_run_id, run_dir
from src.sources.gaokao.major_list_spider import crawl_major_list as crawl_gaokao_major_list
from src.sources.gaokao.university_list_spider import crawl_university_list as crawl_gaokao_university_list
from src.sources.xuezhi.career_list_spider import crawl_career_list as crawl_xuezhi_career_list
from src.sources.xuezhi.major_list_spider import crawl_major_list as crawl_xuezhi_major_list
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


def _task_specs():
    return {
        'gaokao_major': ('gaokao', 'major', crawl_gaokao_major_list),
        'gaokao_university': ('gaokao', 'university', crawl_gaokao_university_list),
        'xuezhi_major': ('xuezhi', 'major', crawl_xuezhi_major_list),
        'xuezhi_career': ('xuezhi', 'career', crawl_xuezhi_career_list),
    }


def run(target='all', run_id=None, headless=None):
    run_id = run_id or make_run_id()
    if headless is None:
        headless = os.getenv('HEADLESS', '1') == '1'

    specs = _task_specs()
    selected = list(specs.keys()) if target == 'all' else [target]
    tasks_summary = []
    stage_dir = list_index_dir(run_id)

    with sync_playwright() as p:
        browser, context = _browser_context(p, headless=headless)
        try:
            for task_name in selected:
                source, entity, func = specs[task_name]
                raw_dir = list_raw_dir(source, entity, run_id)
                logger.info('start task=%s raw_dir=%s', task_name, raw_dir)
                result = func(context, save_html=_save_html_factory(raw_dir))
                index_path = stage_dir / f'{task_name}.jsonl'
                meta_path = stage_dir / f'{task_name}.meta.json'

                write_jsonl(index_path, result['items'])
                write_json(meta_path, {
                    'task': task_name,
                    'source': source,
                    'entity_type': entity,
                    'count': result['count'],
                    'list_url': result['list_url'],
                    'pages': result['pages'],
                })

                tasks_summary.append({
                    'task': task_name,
                    'source': source,
                    'entity_type': entity,
                    'count': result['count'],
                    'index_path': str(index_path.relative_to(Path.cwd())),
                    'meta_path': str(meta_path.relative_to(Path.cwd())),
                    'raw_dir': str(raw_dir.relative_to(Path.cwd())),
                })
                logger.info('done task=%s count=%s', task_name, result['count'])
        finally:
            context.close()
            browser.close()

    manifest = build_manifest('stage_01_collect_list_pages', run_id, tasks_summary)
    manifest_path = run_dir(run_id) / 'stage_01_manifest.json'
    write_json(manifest_path, manifest)
    logger.info('manifest=%s', manifest_path)
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--target',
        default='all',
        choices=['all', 'gaokao_major', 'gaokao_university', 'xuezhi_major', 'xuezhi_career'],
    )
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--headed', action='store_true')
    args = parser.parse_args()
    run(target=args.target, run_id=args.run_id, headless=not args.headed)


if __name__ == '__main__':
    main()