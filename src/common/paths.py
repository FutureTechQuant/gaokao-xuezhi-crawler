from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'data'
RAW_DIR = DATA_DIR / 'raw'
STAGE_DIR = DATA_DIR / 'stage'
FINAL_DIR = DATA_DIR / 'final'
RUNS_DIR = DATA_DIR / 'runs'
STATE_DIR = ROOT / 'state'


def make_run_id() -> str:
    return datetime.now().astimezone().strftime('%Y-%m-%dT%H%M%S')


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_raw_dir(source: str, entity: str, run_id: str) -> Path:
    return ensure_dir(RAW_DIR / source / entity / 'list_pages' / run_id)


def list_index_dir(run_id: str) -> Path:
    return ensure_dir(STAGE_DIR / '01_list_index' / run_id)


def run_dir(run_id: str) -> Path:
    return ensure_dir(RUNS_DIR / run_id)
