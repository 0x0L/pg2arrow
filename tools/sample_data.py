import asyncpg
import pandas as pd


async def complex_table(conn):
    await conn.execute("""
        DROP TABLE IF EXISTS complex_table;
        DROP TYPE IF EXISTS complex;
        DROP TYPE IF EXISTS mood;

        CREATE TYPE complex AS (
            r real[],
            i double precision
        );

        CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

        CREATE TABLE complex_table
        (
            t1 varchar(5),
            t2 real[],
            t3 complex[],
            t4 mood
        );

        INSERT INTO complex_table VALUES
            ('12345', ARRAY[1, NULL, 3, 4], ARRAY[ROW(ARRAY[1.3], 2.2)::complex], 'sad')
    """)


async def minute_bars(conn):
    import numpy as np

    r = """
    DROP TABLE IF EXISTS minute_bars;

    CREATE TABLE minute_bars
    (
        timestamp timestamp without time zone,
        symbol integer,
        open_price real,
        high_price real,
        low_price real,
        close_price real,
        volume integer
    );
    """

    await conn.execute(r)

    times = pd.date_range('2000', '2000-02-01', freq='1min')
    symbols = list(range(100))

    records = [
        (t, s, *np.random.randn(4).tolist(), np.random.randint(0, 1_000_000))
        for t in times
        for s in symbols
    ]

    await conn.copy_records_to_table('minute_bars', records=records)


async def main(dsn):
    conn = await asyncpg.connect(dsn)
    await complex_table(conn)
    # await minute_bars(conn)


if __name__ == '__main__':
    import asyncio

    dsn = 'postgres://localhost/mytests'
    asyncio.run(main(dsn))

