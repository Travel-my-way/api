from pydantic import BaseModel, Field
from datetime import date


class Journey(BaseModel):
    """Model for journey input (without uuid)"""

    origin: str = Field(None, alias="from", description="Origin coordinates")
    destination: str = Field(None, alias="to", description="Destination coordinates")
    start: date = Field(None, description="Start date of journey")

    class Config:
        schema_extra = {
            "example": {
                "from": "48.8727509,2.3711096",
                "to": "46.941296,5.869783",
                "start": date.today()
            }
        }
