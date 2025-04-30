from pydantic import BaseModel
from typing import List, Any, Optional


class SubmitExampleRequestBody(BaseModel):
    """
    Example request body for the API.
    """
    name: str
    age: int
    hobbies: List[str]
    details: Optional[dict] = None
