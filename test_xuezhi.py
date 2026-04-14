#!/usr/bin/env python3

from src.sources.xuezhi.major_list_spider import crawl_major_list
from playwright.sync_api import sync_playwright

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        try:
            result = crawl_major_list(context)
            print('result count:', result['count'])
            print('first item:', result['items'][0] if result['items'] else 'None')
        finally:
            context.close()
            browser.close()

if __name__ == '__main__':
    main()