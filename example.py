from re import I
from typing import Optional
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from cosmopy.model import CosmosModel
from pydantic import BaseModel


class Engine(BaseModel):
    hp: int
    volume: int


class Car(CosmosModel):
    make: str
    model: str
    engine: Optional[Engine]


if __name__ == "__main__":
    passat = Car(make="VW", model="Passat")
    print(f"Car: {passat}")
    passat.save()

    passat.model = "Golf"
    golf = passat.save()
    print(f"Model changed: {golf}")

    passat = Car(make="VW", model="Passat", engine=Engine(hp=100, volume=1600))
    passat.save()
    print(f"New passat: {passat}")

    cars_100_hp = Car.query(engine__hp=100)
    print(f"Cars with 100 HP: {cars_100_hp}")

    cars = Car.all()
    print(f"All cars: {cars}")

    for c in cars:
        print(f"Deleting: {c}")
        c.delete()
