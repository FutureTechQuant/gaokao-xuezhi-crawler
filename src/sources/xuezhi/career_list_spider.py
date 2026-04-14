from src.sources.xuezhi._base_list_spider import XUEZHI_CAREER_URL, _crawl_entry


def crawl_career_list(context, save_html=None):
    return _crawl_entry(context, XUEZHI_CAREER_URL, 'career', save_html=save_html)
