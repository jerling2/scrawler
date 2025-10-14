from psycopg2 import sql
from psycopg2.extras import NumericRange
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Union

@dataclass(frozen=True)
class PostgresApplicationEntity:
    posted_at: datetime
    work_type: str
    position: str
    location: str
    company: str
    pay: Union[tuple[int, int], NumericRange]

    def __post_init__(self):
        if isinstance(self.pay, tuple):
            object.__setattr__(self, 'pay', NumericRange(*self.pay))
    
    def num_cols(self) -> int:
        return len(list(asdict(self)))

    def col_names(self):
        return [sql.Identifier(col_name) for col_name in list(asdict(self))]

    def vars(self):
        return tuple(list(asdict(self).values()))


@dataclass
class PostgresApplicationsModel:

    @property
    def columns(self):
        return \
        [
            ('application_id', 'SERIAL PRIMARY KEY'),
            ('posted_at', 'TIMESTAMP'),
            ('work_type', 'TEXT'),
            ('position', 'TEXT'),
            ('location', 'TEXT'),
            ('company', 'TEXT'),
            ('pay', 'NUMRANGE')
        ]

    @property
    def column_defs(self):
        return \
        [
            sql.SQL("{} {}").format(
                sql.Identifier(col_name),
                sql.SQL(col_type)
            )
            for col_name, col_type in self.columns
        ]
