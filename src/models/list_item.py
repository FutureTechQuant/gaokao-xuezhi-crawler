from pydantic import BaseModel

class ListItem(BaseModel):
    name: str
    url: str
    source: str
    entity_type: str
