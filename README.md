# pg2arrow

pg2arrow is a lightweight tool to parse PostgreSQL binary data into Apache Arrow.

This project is similar to https://github.com/heterodb/pg2arrow though much simpler since we're using the Arrow C++ API.

Presently connection string, table name and schema are hardcoded in `main.cc`.

Schema can be computed via `tools/dump_schema.py`

## TODO

* Missing `numeric`, `hstore` decoders

* C++ implementation of schema builder

* python bindings

* error handling

* some tests would be nice

* replace `DecoderMap` by a more efficient container for our use case: int64_t keys (really) with really small number of elements ?

## Type mapping

Most common base types are supported. `timetz` is not supported.

The following type map is used

| PostgreSQL  | Apache Arrow                        |
|------------:|:------------------------------------|
| bool        | `bool_()`                           |
| bpchar      | `utf8()`                            |
| bytea       | `binary()`                          |
| date        | `date32()`                          |
| float4      | `float32()`                         |
| float8      | `float64()`                         |
| int2        | `int16()`                           |
| int4        | `int32()`                           |
| int8        | `int64()`                           |
| interval    | `duration(TimeUnit::MICRO)`         |
| json        | `utf8()`                            |
| jsonb       | `binary()`                          |
| serial2     | `int16()`                           |
| serial4     | `int32()`                           |
| serial8     | `int64()`                           |
| text        | `utf8()`                            |
| time        | `time64(TimeUnit::MICRO)`           |
| timestamp   | `timestamp(TimeUnit::MICRO)`        |
| timestamptz | `timestamp(TimeUnit::MICRO, "utc")` |
| uuid        | `fixed_size_binary(16)`             |
| varchar     | `utf8()`                            |
| xml         | `utf8()`                            |

SQL composite types are mapped to Arrow `struct_(...)`

SQL arrays are mapped to Arrow `list_(...)`. Only 1D arrays are fully supported. Higher dimensional arrays will be flattened.
