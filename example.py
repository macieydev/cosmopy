from cosmopy.model import BaseModel


class Car(BaseModel):
    make: str
    model: str


if __name__ == "__main__":
    car = Car(make="VW", model="Passat")
    print(f"{car=}")
    print(f"{car._meta.container_name=}")
    car.save()
    print(f"{car=}")
    car.model = "Golf"
    car.save()
    print(f"{car=}")
