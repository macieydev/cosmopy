import os
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union
from azure.cosmos import partition_key

import azure.cosmos.documents as documents
from azure.cosmos.cosmos_client import CosmosClient, DatabaseProxy
from azure.cosmos.container import ContainerProxy
from pydantic.main import BaseModel as PydanticModel
from pydantic.main import ModelMetaclass as PydanticMetaclass
from pydantic import Field
from .exceptions import TooManyObjectsFound, NoObjectFound


def get_client(obj):
    return CosmosClient.from_connection_string(
        os.environ["COSMOS_DB_CONNECTION_STRING"]
    )


def get_or_create_database(obj) -> DatabaseProxy:
    return obj._meta.client.create_database_if_not_exists(os.environ["COSMOS_DB_NAME"])


def get_or_create_container(obj) -> ContainerProxy:
    return obj._meta.database.create_container_if_not_exists(
        obj._meta.container_name,
        {
            "paths": [f"/{obj._meta.partition_key}"],
            "kind": documents.PartitionKind.Hash,
        },
    )


def instance_connection(func):
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


def class_connection(func):
    def wrapper(cls, **kwargs):
        if not hasattr(cls.Meta, "client"):
            client = get_client(cls)
            setattr(cls.Meta, "client", client)

        if not hasattr(cls.Meta, "database"):
            setattr(cls.Meta, "database", get_or_create_database(cls))

        if not hasattr(cls.Meta, "container"):
            setattr(cls.Meta, "container", get_or_create_container(cls))

        return func(cls, **kwargs)

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


def uuid_factory():
    return str(uuid.uuid4())


class CosmosModel(PydanticModel, metaclass=Metaclass):
    id: str = Field(default_factory=uuid_factory)
    rid: Optional[str] = Field(None, alias="_rid")
    self: Optional[str] = Field(None, alias="_self")
    etag: Optional[str] = Field(None, alias="_etag")
    attachments: Optional[str] = Field(None, alias="_attachments")
    ts: Optional[str] = Field(None, alias="_ts")

    __exclude_repr_args__ = ["rid", "self", "etag", "attachments", "ts"]

    class Meta:
        container_name: str
        id_attr: str = "id"
        partition_key: str = "id"

    def __repr_args__(self) -> Tuple[str, Any]:
        original_args = super().__repr_args__()
        args: List[Tuple[str, Any]] = []
        for key, value in original_args:
            if key not in self.__exclude_repr_args__:
                args.append((key, value))
        return args

    @instance_connection
    def save(self):
        upserted = self._meta.container.upsert_item(self.dict(by_alias=True))
        self.parse_obj(upserted)
        return self
        
    @classmethod
    @class_connection
    def all(cls):
        results = cls.Meta.container.read_all_items()
        results = list(results)
        return list(cls(**r) for r in results)

    @classmethod
    @class_connection
    def get(cls, **kwargs):
        params = cls.__parse_to_dot_notation(kwargs)
        params = cls.__format_for_str_values(params)
        params_str = cls.__prepare_params_str(params)
        
        query_str = f"SELECT * FROM c WHERE {params_str}"

        results = cls.Meta.container.query_items(
            query=query_str,
            enable_cross_partition_query=True,
        )
        results = list(results)

        if not results:
            raise NoObjectFound

        if len(results) > 1:
            raise TooManyObjectsFound

        return cls(**results[0])

    @classmethod
    @class_connection
    def query(cls, **kwargs):
        params = cls.__parse_to_dot_notation(kwargs)
        params = cls.__format_for_str_values(params)
        params_str = cls.__prepare_params_str(params)

        query_str = f"SELECT * FROM c WHERE {params_str}"

        results = cls.Meta.container.query_items(
            query=query_str,
            enable_cross_partition_query=True,
        )

        return list(cls(**r) for r in results)

    @staticmethod
    def __prepare_params_str(params):
        return " AND ".join([f"c.{k} = {v}" for k, v in params.items()])

    @staticmethod
    def __parse_to_dot_notation(params):
        return {
            key.replace('__', '.'): params[key] for key in params
        }

    @staticmethod
    def __format_for_str_values(params):
        for key in params:
            if isinstance(params[key], str):
                params[key] = f"\"{params[key]}\""
        return params

    @instance_connection
    def delete(self):
        self._meta.container.delete_item(self.id, partition_key=self.id)
