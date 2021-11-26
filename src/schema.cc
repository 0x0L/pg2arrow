#include "pg2arrow.h"

#include <iostream>

namespace Pg2Arrow {

std::vector<std::tuple<std::string, Oid>> GetCompositeInfo(PGconn* conn, Oid typid) {
    char query[4096];
    snprintf(
        query, sizeof(query), R"(
        SELECT
            attnum, attname, atttypid
        FROM
            pg_catalog.pg_attribute a,
            pg_catalog.pg_type t,
            pg_catalog.pg_namespace n
        WHERE
            t.typnamespace = n.oid
            AND a.atttypid = t.oid
            AND a.attrelid = %u
        )",
        typid);

    auto res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        std::cout << "get composite descr failed: " << PQresultErrorMessage(res)
                  << std::endl;

    int nfields = PQntuples(res);
    std::vector<std::tuple<std::string, Oid>> fields(nfields);

    for (size_t i = 0; i < nfields; i++) {
        int attnum = atoi(PQgetvalue(res, i, 0));
        const char* attname = PQgetvalue(res, i, 1);
        Oid atttypid = atooid(PQgetvalue(res, i, 2));

        fields[attnum - 1] = {attname, atttypid};
    }

    PQclear(res);
    return fields;
}

std::map<std::string, std::shared_ptr<arrow::DataType>> kTypeMap = {
    {"bool", arrow::boolean()},
    {"bpchar", arrow::utf8()},
    {"bytea", arrow::binary()},
    {"date", arrow::date32()},
    {"float4", arrow::float32()},
    {"float8", arrow::float64()},
    {"int2", arrow::int16()},
    {"int4", arrow::int32()},
    {"int8", arrow::int64()},
    {"interval", arrow::duration(arrow::TimeUnit::MICRO)},
    {"json", arrow::utf8()},
    {"jsonb", arrow::binary()},
    // {"numeric", arrow::decimal128(})
    {"serial2", arrow::int16()},
    {"serial4", arrow::int32()},
    {"serial8", arrow::int64()},
    {"text", arrow::utf8()},
    {"time", arrow::time64(arrow::TimeUnit::MICRO)},
    // {"timetz", arrow::time64(arrow::TimeUnit::MICRO)},
    {"timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)},
    {"timestamptz", arrow::timestamp(arrow::TimeUnit::MICRO, "utc")},
    {"uuid", arrow::fixed_size_binary(16)},
    {"varchar", arrow::utf8()},
    {"xml", arrow::utf8()}};

std::shared_ptr<arrow::DataType> GetArrowType(PGconn* conn, Oid typid) {
    char query[4096];
    snprintf(
        query, sizeof(query), R"(
        SELECT
            typname, typtype, typelem, typrelid
        FROM
            pg_catalog.pg_type t,
            pg_catalog.pg_namespace n
        WHERE
            t.typnamespace = n.oid
            AND t.oid = %u
        )",
        typid);

    auto res = PQexec(conn, query);
    // auto status = PQresultStatus(res) != PGRES_TUPLES_OK;

    std::string typname = PQgetvalue(res, 0, 0);
    char typtype = *PQgetvalue(res, 0, 1);
    Oid typelem = atooid(PQgetvalue(res, 0, 2));
    Oid typrelid = atooid(PQgetvalue(res, 0, 3));

    PQclear(res);

    switch (typtype) {
        case 'b': {
            if (typelem > 0) {
                return arrow::list(GetArrowType(conn, typelem));
            } else {
                return kTypeMap[typname];
            }
        } break;

        case 'c': {
            auto fields_info = GetCompositeInfo(conn, typrelid);
            arrow::FieldVector fields;
            for (size_t i = 0; i < fields_info.size(); i++) {
                auto [field_name, field_oid] = fields_info[i];
                fields.push_back(
                    arrow::field(field_name, GetArrowType(conn, field_oid)));
            }
            return arrow::struct_(fields);
        } break;

        case 'e': {
            return arrow::dictionary(arrow::int32(), arrow::utf8());
        } break;
    }

    return arrow::null();
}

std::shared_ptr<arrow::Schema> GetQuerySchema(PGconn* conn, const char* query) {
    auto descr_query = std::string(query) + " limit 0";
    PGresult* res = PQexec(conn, descr_query.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        std::cout << "get descr failed: " << PQresultErrorMessage(res) << std::endl;

    int nfields = PQnfields(res);
    arrow::FieldVector fields(nfields);
    for (size_t i = 0; i < nfields; i++) {
        const char* name = PQfname(res, i);
        Oid oid = PQftype(res, i);
        fields[i] = arrow::field(name, GetArrowType(conn, oid));
    }

    PQclear(res);

    auto schema = arrow::schema(fields);
    return schema;
}

}  // namespace Pg2Arrow
