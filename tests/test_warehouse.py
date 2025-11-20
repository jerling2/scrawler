import pytest

"""
---
# Lowest Level Model & Model Interface

This is another 'serialization | deserialization' problem. It's difficult
to implement safety on the model level. What seems the most reliable is
defining the shape of the repository via the TypedDict. And, the services
ontop of the models implement the various CRUD operations without exposing
variable conditionals 

```PostgresModel```
Schema: TypedDict

```MilvusModel```
Schema: TypedDict

must expose a schema via typed dict. Each model only needs one typed dict
because serialization and deserialization are the same.

---
# Second Level | Repository Implemention

Combines the necessary configurations according the the selected
database to implement the model

```PostgresRepo``
conn: PostgresConn
model: PostgresModel

CREATE_TABLE
DROP_TABLE

- validates

```MilvusRepo```
conn: MilvusConn
model: MilvusModel

CREATE_COLLECTION
DROP_COLLECTION
---
# Third Level | Service Layer

Exposes an API that helps with CRUD operations on the Repository implementation

```Example: UserRepo

job_repo = PostgresRepo
hs_meta_repo = PostgresRepo
job_vec = MilvusRepo

add_users(users: list[Users]) -> int:
select_users(user_ids: list[int]) -> list[Users]:
remove_users(user_ids: list[int]) -> int:
update_users(new_fields: dict[user_id, new_fields]) -> int:



--- Postgres Notes
INESRT INTO table_name 
( column_name [, ...] ) 
VALUES ( expression [, ...]);
>> int

SELECT [ ALL | DISTINCT ] column_name [,...]
FROM table_name
[WHERE condition]
[ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
[ LIMIT { count | ALL } ]
[ OFFSET start [ ROWS | ROWS ] ]
...
>> dict[T]

UPDATE FROM table_name
SET column_name = expression [, ...]
[WHERE condition]
>> int

DELETE FROM table_name
[WHERE condition]
>> int
"""