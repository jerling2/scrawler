import os
from typing import TypeAlias, Literal
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, Mapped


SupportedDialect: TypeAlias = Literal['postgresql']


class DSNFactory:

    POSTGRES_URL = 'postgresql+{_db_api}://{_user}:{_pass}@{_host}:{_port}/{_database}'

    @classmethod
    def from_env(cls, dialect: SupportedDialect = 'postgresql') -> str:
        if dialect == 'postgresql':
            return cls.POSTGRES_URL.format(
                _db_api='psycopg2',
                _user=os.environ['SCRAWLER_POSTGRES_USER'],
                _pass=os.environ['SCRAWLER_POSTGRES_PASS'],
                _host=os.environ['SCRAWLER_POSTGRES_HOST'],
                _port=os.environ['SCRAWLER_POSTGRES_PORT'],
                _database=os.environ['SCRAWLER_POSTGRES_DATABASE']
            )
        raise ValueError(f"dialiect '{dialect}' is not supported")




if __name__ == '__main__':
    pass
    engine = create_engine(DSNFactory.from_env(), echo=True)
