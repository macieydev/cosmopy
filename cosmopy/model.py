import os
import uuid
from typing import Union

import azure.cosmos.documents as documents
from azure.cosmos.cosmos_client import CosmosClient, DatabaseProxy
from pydantic.main import BaseModel as PydanticModel
from pydantic.main import ModelMetaclass as PydanticMetaclass


def get_client(obj):
    return CosmosClient.from_connection_string(
        os.environ["COSMOS_DB_CONNECTION_STRING"]
    )


def get_or_create_database(obj) -> DatabaseProxy:
    return obj._meta.client.create_database_if_not_exists(os.environ["COSMOS_DB_NAME"])


def get_or_create_container(obj):
    return obj._meta.database.create_container_if_not_exists(
        obj._meta.container_name,
        {
            "paths": [f"/{obj._meta.partition_key}"],
            "kind": documents.PartitionKind.Hash,
        },
    )


def connection(func):
    def wrapper(obj):
        if not hasattr(obj._meta, "client"):
            client = get_client(obj)
            setattr(obj._meta, "client", client)

        if not hasattr(obj._meta, "database"):
            setattr(obj._meta, "database", get_or_create_database(obj))

        if not hasattr(obj._meta, "container"):
            setattr(obj._meta, "container", get_or_create_container(obj))

        return func(obj)

    return wrapper


class BaseQuery:
    def get(self, **kwargs):
        pass

    def filter(self, **kwargs):
        pass


class Metaclass(PydanticMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        super_new = super().__new__

        parents = [b for b in bases if isinstance(b, Metaclass)]
        if not parents:
            return super_new(mcs, name, bases, namespace)

        module = namespace.pop("__module__")
        namespace.update({"__module__": module})
        classcell = namespace.pop("__classcell__", None)
        if classcell is not None:
            namespace.update({"__classcell__": classcell})
        attr_meta = namespace.pop("Meta", None)
        new_class = super_new(mcs, name, bases, namespace, **kwargs)
        meta = attr_meta or getattr(new_class, "Meta", None)
        base_meta = getattr(new_class, "_meta", None)

        setattr(new_class, "_meta", meta)

        if not hasattr(meta, "id_attr"):
            new_class._meta.id_attr = base_meta.id_attr

        if not hasattr(meta, "partition_key"):
            new_class._meta.partition_key = base_meta.partition_key

        if not hasattr(meta, "container_name"):
            new_class._meta.container_name = new_class.__name__

        return new_class


class BaseModel(PydanticModel, metaclass=Metaclass):
    id: Union[str, uuid.UUID] = str(uuid.uuid4())

    class Meta:
        container_name: str
        id_attr: str = "id"
        partition_key: str = "id"

    @connection
    def save(self):
        self._meta.container.upsert_item(self.dict())
        return self

    @classmethod
    @connection
    def get(cls, **kwargs):
        pass

    @classmethod
    @connection
    def query(cls, **kwargs):
        pass

    @connection
    def delete(self):
        pass
