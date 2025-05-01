from pydantic import BaseModel
from typing import Optional, Dict

class CustomerRequest(BaseModel):
    name: str
    email: str
    uuid: str

class CustomerResponse(BaseModel):
    name: str
    email: str
    uuid: str