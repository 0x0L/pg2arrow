#include <arrow/api.h>
#include <arrow/io/api.h>
#include <libpq-fe.h>
#include <parquet/arrow/writer.h>

#include <getopt.h>
#include <chrono>
#include <iostream>
#include "./pg2arrow.h"

static const char* postgres_dsn = "postgresql://localhost/mytests";
static const char* table_name = "complex_table";
static const char* output_filename = "test.parquet";

static void parse_options(int argc, char* const argv[]) {
    static struct option options[] = {
        {"dsn", 1, NULL, 'd'},   {"table", 1, NULL, 't'}, {"file", 1, NULL, 'f'},
        {"help", 0, NULL, 9999}, {NULL, 0, NULL, 0},
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:t:f:", options, NULL)) >= 0) {
        if (c == 'd')
            postgres_dsn = optarg;
        else if (c == 't')
            table_name = optarg;
        else if (c == 'f')
            output_filename = optarg;
        else {
            printf("usage: pg2arrow -d dsn -t table -f file");
        }
    }
}

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
    // auto status = PQresultStatus(res) != PGRES_TUPLES_OK;

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

std::shared_ptr<arrow::Schema> BuildSchema(PGconn* conn, const char* table) {
    char query[4096];
    snprintf(
        query, sizeof(query), R"(
        SELECT
            attnum, attname, atttypid
        FROM
            pg_attribute
        WHERE
            attrelid = '%s'::regclass
            AND attnum > 0
        )",
        table);

    auto res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        std::cout << "failed on pg_type system catalog query: "
                  << PQresultErrorMessage(res) << std::endl;

    int nfields = PQntuples(res);
    arrow::FieldVector fields(nfields);

    for (size_t j = 0; j < nfields; j++) {
        int attnum = atoi(PQgetvalue(res, j, 0));
        const char* attname = PQgetvalue(res, j, 1);
        Oid atttypid = atooid(PQgetvalue(res, j, 2));
        fields[attnum - 1] = arrow::field(attname, GetArrowType(conn, atttypid));
    }

    PQclear(res);

    auto schema = arrow::schema(fields);
    return schema;
}

void CopyTable(PGconn* conn, const char* table, Pg2Arrow::PgBuilder& builder) {
    char query[4096];
    snprintf(
        query, sizeof(query), "COPY (SELECT * FROM %s) TO STDOUT (FORMAT binary);",
        table);

    auto res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COPY_OUT)
        std::cout << "error in copy command: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    char* tuple;
    auto status = PQgetCopyData(conn, &tuple, 0);
    if (status > 0) {
        const int kBinaryHeaderSize = 19;
        builder.Append(tuple + kBinaryHeaderSize);
        PQfreemem(tuple);
    }

    // auto begin = std::chrono::system_clock::now();
    // auto elapsed = begin - begin;

    while (true) {
        status = PQgetCopyData(conn, &tuple, 0);
        if (status < 0)
            break;

        // begin = std::chrono::system_clock::now();
        builder.Append(tuple);
        // elapsed += std::chrono::system_clock::now() - begin;
        PQfreemem(tuple);
    }

    // std::cout << "took "
    //           <<
    //           std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
    //           << " ms" << std::endl;

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
    PQclear(res);
}

int main(int argc, char** argv) {
    parse_options(argc, argv);

    auto conn = PQconnectdb(postgres_dsn);
    if (PQstatus(conn) != CONNECTION_OK)
        std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn)
                  << std::endl;

    auto res = PQexec(conn, "BEGIN READ ONLY");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    auto schema = BuildSchema(conn, table_name);
    Pg2Arrow::PgBuilder builder(schema);

    CopyTable(conn, table_name, builder);

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = builder.Flush(&batch);
    auto table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile, arrow::io::FileOutputStream::Open(output_filename));

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), outfile, table->num_rows()));

    res = PQexec(conn, "END");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to end transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    PQfinish(conn);
    return 0;
}
