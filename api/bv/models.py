from pydantic import BaseModel, UUID4, validator, ValidationError, Field
from datetime import date
import uuid


class JourneyIn(BaseModel):
    """Model for journey input (without uuid)"""

    origin: str = Field(None, alias="from", description="Origin coordinates")
    destination: str = Field(None, alias="to", description="Destination coordinates")
    start: date = Field(None, description="Start date of journey")

    def is_valid(self):
        return True

    def submit(self):
        return uuid.uuid4()

    @validator("origin")
    def check_point(cls, v):
        if "," not in v:
            raise ValidationError("Not a lat/lon string")

    class Config:
        schema_extra = {
            "example": {
                "from": "48.8727509,2.3711096",
                "to": "46.941296,5.869783",
                "start": date.today()
            }
        }


class Journey(BaseModel):
    """Real journey model, stored in DB."""

    origin: str = Field(None, description="Origin coordinates")
    destination: str = Field(None, description="Destination coordinates")
    start: date = Field(None, description="Start date of journey")
    uuid: UUID4 = Field(
        None, description="UUID of journey in application", title="UUID of journey"
    )
