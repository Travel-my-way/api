from pydantic import BaseModel, Field
from datetime import date


class Journey(BaseModel):
    """Model for journey input (without uuid)"""

    origin: str = Field(None, alias="from", description="Origin coordinates")
    destination: str = Field(None, alias="to", description="Destination coordinates")
    start: date = Field(None, description="Start date of journey")

    def __str__(self):
        return "Journey from {} to {} @ {}".format(
            self.origin,
            self.destination,
            self.start
        )

    def as_celery_kwargs(self) -> dict:
        return {
            "from_loc": self.origin,
            "to_loc": self.destination,
            "start_date": self.start.strftime("%Y-%m-%d")
        }

    class Config:
        schema_extra = {
            "example": {
                "from": "48.8727509,2.3711096",
                "to": "46.941296,5.869783",
                "start": date.today()
            }
        }


class Result(BaseModel):
    id: str
    status: str
    journeys: list
