#!/usr/bin/env python
from sqlalchemy import create_engine

engine = create_engine(
    f'snowflake://{"prasanna"}:{"Nanji123@a"}@{"https://hr91746.central-us.azure"}/'
)
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()