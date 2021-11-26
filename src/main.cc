#include "./pg2arrow.h"

#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <getopt.h>
#include <chrono>
#include <iostream>

static const char* conninfo = "postgresql://localhost/mytests";
static const char* query = "select * from minute_bars";
static const char* output_filename = "test.parquet";

static void parse_options(int argc, char* const argv[]) {
    static struct option options[] = {
        {"conninfo", 1, NULL, 'd'},
        {"table", 1, NULL, 'q'},
        {"output_file", 1, NULL, 'o'},
        {"help", 0, NULL, 9999},
        {NULL, 0, NULL, 0},
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:q:o:", options, NULL)) >= 0) {
        if (c == 'd')
            conninfo = optarg;
        else if (c == 'q')
            query = optarg;
        else if (c == 'o')
            output_filename = optarg;
        else {
            printf("usage: pg2arrow -d conninfo -q query -o output_file");
            exit(0);
        }
    }
}

int main(int argc, char** argv) {
    parse_options(argc, argv);

    auto conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
        std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn)
                  << std::endl;

    auto res = PQexec(conn, "BEGIN READ ONLY");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    auto schema = Pg2Arrow::GetQuerySchema(conn, query);
    Pg2Arrow::PgBuilder builder(schema);

    CopyQuery(conn, query, builder);

    res = PQexec(conn, "END");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to end transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    PQfinish(conn);

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = builder.Flush(&batch);
    auto table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();

    std::shared_ptr<arrow::io::FileOutputStream> output_file;
    PARQUET_ASSIGN_OR_THROW(
        output_file, arrow::io::FileOutputStream::Open(output_filename));

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output_file, table->num_rows()));

    return 0;
}
