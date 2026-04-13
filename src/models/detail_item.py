from pydantic import BaseModel

class DetailItem(BaseModel):
    source_id: str
    source: str
    entity_type: str
    detail: dict
