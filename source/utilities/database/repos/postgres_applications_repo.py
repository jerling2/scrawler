from psycopg2 import sql
from source.utilities.database.connections.postgres_connection import PostgresConnection
from source.utilities.database.models.postgres_applications_model import PostgresApplicationsModel, PostgresApplicationEntity


class PostgresApplicationsRepo:

    def __init__(self, table_name: str, conn: PostgresConnection):
        self.table_name = table_name
        self.conn = conn
        self.model = PostgresApplicationsModel()

    def create_table(self):
        query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(", ").join(self.model.column_defs)
        )
        with self.conn.client.cursor() as cur:
            cur.execute(query)

    def drop_table(self):
        query = sql.SQL("DROP TABLE {}").format(
            sql.Identifier(self.table_name)
        )
        with self.conn.client.cursor() as cur:
            cur.execute(query)

    def create_application(self, entity: PostgresApplicationEntity):
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(entity.col_names()),
            sql.SQL(', ').join(sql.Placeholder() * entity.num_cols())
        )
        with self.conn.client.cursor() as cur:
            cur.execute(query, entity.vars())