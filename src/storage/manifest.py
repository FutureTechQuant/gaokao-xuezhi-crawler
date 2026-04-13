from datetime import datetime, timezone


def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def build_manifest(stage: str, run_id: str, tasks: list[dict]) -> dict:
    return {
        'stage': stage,
        'run_id': run_id,
        'created_at': iso_now(),
        'tasks': tasks,
    }
