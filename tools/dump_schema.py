import asyncpg
import pyarrow as pa


async def get_type_info(conn, typid):
    r = f"""
    SELECT
        nspname, typname,
        typlen, typbyval, typalign,
        typtype, typrelid, typelem
    FROM
        pg_catalog.pg_type t,
        pg_catalog.pg_namespace n
    WHERE
        t.typnamespace = n.oid
        AND t.oid = {typid}
    """
    return dict(await conn.fetchrow(r))


async def get_composite_info(conn, typid):
    r = f"""
    SELECT
        nspname, typname,
        attnum, attname, atttypid, atttypmod,
        attlen, attbyval, attalign,
        typtype, typrelid, typelem
    FROM
        pg_catalog.pg_attribute a,
        pg_catalog.pg_type t,
        pg_catalog.pg_namespace n
    WHERE
        t.typnamespace = n.oid
        AND a.atttypid = t.oid
        AND a.attrelid = {typid}
    """
    return await conn.fetch(r)


async def get_enum_info(conn, typid):
    r = f"""
    SELECT
        enumlabel
    FROM
        pg_catalog.pg_enum
    WHERE enumtypid = {typid}
    """
    return await conn.fetch(r)


async def _build_tree(conn, typid, d={}):
    d = d | dict(await get_type_info(conn, typid))

    if d["typtype"] == b"b":
        if d["typelem"] > 0:
            d["element"] = await _build_tree(conn, d["typelem"])

    elif d["typtype"] == b"c":
        h = await get_composite_info(conn, d["typrelid"])
        d["subtypes"] = [await _build_tree(conn, r["atttypid"], d=dict(r)) for r in h]

    elif d["typtype"] == b"e":
        d["enumdict"] = [r["enumlabel"] for r in await get_enum_info(conn, typid)]

    return d


async def build_tree(conn, table):
    r = f"""
    SELECT
        attnum, attname, atttypid, atttypmod
    FROM
        pg_attribute
    WHERE
        attrelid = '{table}'::regclass
        AND attnum > 0
    """
    cols = await conn.fetch(r)
    return [await _build_tree(conn, c["atttypid"], d=dict(c)) for c in cols]


_type_map = {
    "bool": pa.bool_(),
    "bpchar": pa.utf8(),
    "bytea": pa.binary(),
    "date": pa.date32(),
    "float4": pa.float32(),
    "float8": pa.float64(),
    "int2": pa.int16(),
    "int4": pa.int32(),
    "int8": pa.int64(),
    "interval": pa.duration("us"),
    "json": pa.utf8(),
    "jsonb": pa.binary(),
    # "numeric": pa.decimal128()
    "serial2": pa.int16(),
    "serial4": pa.int32(),
    "serial8": pa.int64(),
    "text": pa.utf8(),
    "time": pa.time64("us"),
    # "timetz": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "timestamptz": pa.timestamp("us", tz="utc"),
    "uuid": pa.binary(length=16),
    "varchar": pa.utf8(),
    "xml": pa.utf8(),
}


def _sql_to_arrow(typ):
    if "element" in typ:
        return pa.list_(_sql_to_arrow(typ["element"]))
    elif "subtypes" in typ:
        return pa.struct(
            [
                (t["attname"], _sql_to_arrow(t))
                for t in sorted(typ["subtypes"], key=lambda t: t["attnum"])
            ]
        )
    elif "enumdict" in typ:
        return pa.dictionary(pa.int32(), pa.utf8())

    return _type_map[typ["typname"]]


async def make_schema(conn, table):
    columns = await build_tree(conn, table)
    schema = pa.schema([(c["attname"], _sql_to_arrow(c)) for c in columns])
    return schema


async def get_schema(dsn, table):
    conn = await asyncpg.connect(dsn)
    return await make_schema(conn, table)


if __name__ == "__main__":
    import asyncio
    import argparse

    dsn = 'postgres://localhost/mytests'

    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', action='store', default=dsn)
    parser.add_argument('table', action='store')

    args = parser.parse_args()
    schema = asyncio.run(get_schema(args.dsn, args.table))
    print(schema)
