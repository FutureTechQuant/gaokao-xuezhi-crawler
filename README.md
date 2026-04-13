# gaokao-xuezhi-crawler

用于爬取阳光高考与学职平台的专业、院校、职业数据，并按阶段保存原始页、中间结果与最终 JSON。

## 当前进度

当前已实现第一阶段列表页抓取：

- 阳光高考专业列表
- 阳光高考院校列表
- 学职平台专业列表
- 学职平台职业列表

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## 运行

```bash
python -m src.pipelines.stage_01_collect_list_pages --target all
python -m src.pipelines.stage_01_collect_list_pages --target gaokao_major
python -m src.pipelines.stage_01_collect_list_pages --target gaokao_university
python -m src.pipelines.stage_01_collect_list_pages --target xuezhi_major
python -m src.pipelines.stage_01_collect_list_pages --target xuezhi_career
```

## 输出

- 原始页面：`data/raw/.../list_pages/<run_id>/`
- 列表索引：`data/stage/01_list_index/<run_id>/`
- 运行记录：`data/runs/<run_id>/stage_01_manifest.json`
