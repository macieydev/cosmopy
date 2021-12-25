import os

import azure.cosmos.documents as documents
from azure.cosmos.cosmos_client import CosmosClient

from .exceptions import NoObjectFound, TooManyObjectsFound


class CosmosContainer:
    cached_container = None

    def __get__(self, instance, owner):
        if self.cached_container:
            print("getting cached container for", owner.__name__)
            return self.cached_container
        print("getting new container for", owner.__name__)
        client = CosmosClient.from_connection_string(
            os.environ["COSMOS_DB_CONNECTION_STRING"]
        )
        database = client.create_database_if_not_exists(os.environ["COSMOS_DB_NAME"])
        partition_key = getattr(owner, "_partition_key")
        self.cached_container = database.create_container_if_not_exists(
            getattr(owner, "_container_name", owner.__name__),
            {
                "paths": [f"/{partition_key}"],
                "kind": documents.PartitionKind.Hash,
            },
        )
        return self.cached_container


class ManagableDocumentMixin:
    def save(self):
        print("saving", str(self))
        upserted = self._container.upsert_item(self.dict(by_alias=True))
        self.parse_obj(upserted)
        return self

    def delete(self):
        print("deleting", str(self))
        partition_key_name = getattr(self, "_partition_key")
        partition_key_value = getattr(self, partition_key_name)
        self._container.delete_item(self.id, partition_key=partition_key_value)

    @classmethod
    def query(cls, **kwargs):
        print("querying", str(cls.__name__))
        params = cls.__parse_to_dot_notation(kwargs)
        params = cls.__format_for_str_values(params)
        params_str = cls.__prepare_params_str(params)

        query_str = f"SELECT * FROM c WHERE {params_str}"

        results = cls._container.query_items(
            query=query_str,
            enable_cross_partition_query=True,
        )

        return list(cls(**r) for r in results)

    @classmethod
    def get(cls, **kwargs):
        print("getting", str(cls.__name__))
        params = cls.__parse_to_dot_notation(kwargs)
        params = cls.__format_for_str_values(params)
        params_str = cls.__prepare_params_str(params)

        query_str = f"SELECT * FROM c WHERE {params_str}"

        results = cls._container.query_items(
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
    def all(cls):
        print("getting all", str(cls.__name__))
        results = cls._container.read_all_items()
        results = list(results)
        return list(cls(**r) for r in results)

    @staticmethod
    def __prepare_params_str(params):
        return " AND ".join([f"c.{k} = {v}" for k, v in params.items()])

    @staticmethod
    def __parse_to_dot_notation(params):
        return {key.replace("__", "."): params[key] for key in params}

    @staticmethod
    def __format_for_str_values(params):
        for key in params:
            if isinstance(params[key], str):
                params[key] = f'"{params[key]}"'
        return params
