from pydantic import BaseModel

class FinalEntity(BaseModel):
    schema_version: str
    entity_type: str
    source: str
    source_id: str
    name: str
    meta: dict
