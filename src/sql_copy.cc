#include "pg2arrow.h"

#include <iostream>

namespace Pg2Arrow {

void CopyQuery(PGconn* conn, const char* query, PgBuilder& builder) {
    auto copy_query = std::string("COPY (") + query + ") TO STDOUT (FORMAT binary)";
    auto res = PQexec(conn, copy_query.c_str());
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

    while (true) {
        status = PQgetCopyData(conn, &tuple, 0);
        if (status < 0)
            break;

        builder.Append(tuple);
        PQfreemem(tuple);
    }

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
    PQclear(res);
}

}  // namespace Pg2Arrow
