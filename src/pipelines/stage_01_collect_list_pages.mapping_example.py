from src.sources.gaokao.university_list_spider import crawl_university_list
from src.sources.gaokao.major_list_spider import crawl_major_list as crawl_gaokao_major_list
from src.sources.xuezhi.major_list_spider import crawl_major_list as crawl_xuezhi_major_list
from src.sources.xuezhi.career_list_spider import crawl_career_list as crawl_xuezhi_career_list

TASKS = {
    'gaokao_university': crawl_university_list,
    'gaokao_major': crawl_gaokao_major_list,
    'xuezhi_major': crawl_xuezhi_major_list,
    'xuezhi_career': crawl_xuezhi_career_list,
}
