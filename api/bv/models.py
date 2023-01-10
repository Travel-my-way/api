from pydantic import BaseModel, Field
from datetime import date


class Journey(BaseModel):
    """Model for journey input (without uuid)"""

    origin: str = Field(None, description="Origin coordinates")
    destination: str = Field(None, description="Destination coordinates")
    start: int = Field(None, description="Start date of journey")
    nb_passenger: int = Field(None, description="Passenger count")

    def __str__(self):
        return "Journey from {} to {} on {} for {} passengers".format(
            self.origin,
            self.destination,
            self.start,
            self.nb_passenger
        )

    def as_celery_kwargs(self) -> dict:
        return {
            "from_loc": self.origin,
            "to_loc": self.destination,
            "start_date": self.start,
            "nb_passenger": self.nb_passenger
        }

    class Config:
        schema_extra = {
            "example": {
                "from": "48.8727509,2.3711096",
                "to": "46.941296,5.869783",
                "start": date.today(),
                "nb_passenger": 3
            }
        }


class Result(BaseModel):
    id: str
    status: str
    journeys: list
