# cosmopy
##### Django-like ORM based on Pydantic for Azure CosmosDB

- pydantic syntax when defining a model
- models have interface to manage themselves (CRUD)


### TODO:
- Ideas for lazy loading
- Contain raw objects from cosmos in querysets
- get rid off class Meta and use underscore symbol for class atributes
- investigate cosmos connection cycle
- provide in memory containers for testing
- lookups:
    - arrays
    - like
    - case insensitive lookups
    - gt / lt / gte / lte
- "relationships" - syntax sugar?
    - CosmosModel can be a value of an attribute in other CosmosModel, if so "parent" model would save to db only id
    - "child" object would be loaded lazily
    - "child" object don't know about "relationship" with "parent" object
