from typing import Any, List, Optional, Tuple
import uuid
from pydantic import BaseModel
from pydantic.fields import Field
from cosmopy.mixins import ManagableDocumentMixin, CosmosContainer


def uuid4_factory():
    return str(uuid.uuid4())


class CosmosBaseModel(ManagableDocumentMixin, BaseModel):
    id: str = Field(default_factory=uuid4_factory)
    rid: Optional[str] = Field(None, alias="_rid")
    self: Optional[str] = Field(None, alias="_self")
    etag: Optional[str] = Field(None, alias="_etag")
    attachments: Optional[str] = Field(None, alias="_attachments")
    ts: Optional[str] = Field(None, alias="_ts")

    _partition_key = "id"

    __exclude_repr_args__ = ["rid", "self", "etag", "attachments", "ts"]

    def __repr_args__(self) -> Tuple[str, Any]:
        original_args = super().__repr_args__()
        args: List[Tuple[str, Any]] = []
        for key, value in original_args:
            if key not in self.__exclude_repr_args__:
                args.append((key, value))
        return args


class Engine(BaseModel):
    hp: int
    vol: int


class Car(CosmosBaseModel):
    model: str
    make: str
    engine: Engine

    _container_name = "cars"
    _container = CosmosContainer()


class SUV(Car):
    four_wheels_drive: bool

    _container_name = "suvs"
    _container = CosmosContainer()

class Bike(CosmosBaseModel):
    model: str
    make: str

    _container_name = "bikes"
    _container = CosmosContainer()


if __name__ == "__main__":
    car = Car(
        model="VW", 
        make="Golf", 
        engine=Engine(
            hp=100, 
            vol=1600
        )
    )
    car.save()
    _id = car.id
    car = Car.get(id=_id)
    print(car)
    suv = SUV(model="Santa fe", make="Hyundai", engine=Engine(hp=115, vol=2000), four_wheels_drive=True)
    suv.save()
    bikes = Bike.all()
    print(bikes)
    cars = Car.all()
    print(cars)
    bike = Bike(make="Romet", model="Wigry")
    bike.save()
    romet_bikes = Bike.query(make="Romet")
    print(romet_bikes)
