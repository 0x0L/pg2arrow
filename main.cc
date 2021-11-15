#include <libpq-fe.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <memory>

#include "./pg2arrow.h"

void CopyTable(PGconn* conn, PgBuilder& builder) {
    const int kBinaryHeaderSize = 19;
    PGresult* res;
    int status;

    res = PQexec(conn, "COPY (SELECT * FROM missing_types) TO STDOUT (FORMAT binary);");
    if (PQresultStatus(res) != PGRES_COPY_OUT)
        std::cout << "error in copy command: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    char* tuple;
    status = PQgetCopyData(conn, &tuple, 0);
    if (status > 0) {
        builder.AppendRow(tuple + kBinaryHeaderSize);
        PQfreemem(tuple);
    }

    while (true) {
        status = PQgetCopyData(conn, &tuple, 0);
        if (status < 0)
            break;
        builder.AppendRow(tuple);
        PQfreemem(tuple);
    }

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
    PQclear(res);
}

int main(int argc, char** argv) {
    PGconn* conn;
    PGresult* res;
    int status;

    conn = PQconnectdb("postgresql://localhost/mytests");
    status = PQstatus(conn);
    if (status != CONNECTION_OK)
        std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn)
                  << std::endl;

    res = PQexec(conn, "BEGIN READ ONLY");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    auto schema = arrow::schema(
        {arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
         arrow::field("symbol", arrow::int32()), arrow::field("open", arrow::float32()),
         arrow::field("high", arrow::float32()), arrow::field("low", arrow::float32()),
         arrow::field("close", arrow::float32()),
         arrow::field("volume", arrow::int32())});

    schema = arrow::schema(
        {arrow::field("t1", arrow::utf8()),
         arrow::field("t2", arrow::list(arrow::float32())),
         arrow::field(
             "t3", arrow::list(arrow::struct_({
                       arrow::field("r", arrow::list(arrow::float32())),
                       arrow::field("i", arrow::float64()),
                   }))),
         arrow::field("t4", arrow::dictionary(arrow::int32(), arrow::utf8()))});

    schema = arrow::schema(
    {
        arrow::field("bool_col", arrow::boolean()),
        arrow::field("date_col", arrow::date32()),
        arrow::field("time_col", arrow::time64(arrow::TimeUnit::MICRO)),
        // arrow::field("interval_col", arrow::duration(arrow::TimeUnit::MICRO)),
        arrow::field("json_col", arrow::utf8()),
        arrow::field("jsonb_col", arrow::binary()),
        arrow::field("uuid_col", arrow::fixed_size_binary(16)),
        arrow::field("xml_col", arrow::utf8()),
        arrow::field("bytea_col", arrow::binary()),
    });

    PgBuilder builder(schema);

    CopyTable(conn, builder);
    auto batch = builder.Finish();

    auto table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile, arrow::io::FileOutputStream::Open("output.parquet"));

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
