from src.sources.xuezhi._base_list_spider import XUEZHI_MAJOR_URL, _crawl_entry


def crawl_major_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_MAJOR_URL, 'major', save_html=save_html)
