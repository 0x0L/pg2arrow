#pragma once

#include <arrow/api.h>

#include <map>
#include <memory>

class PgBuilder {
  public:
    using Decoder = int32_t (*)(PgBuilder &, arrow::ArrayBuilder *,
                                const char *, int32_t);
    using DecoderMap = std::map<arrow::ArrayBuilder *, Decoder>;

    PgBuilder(std::shared_ptr<arrow::Schema> schema);
    int32_t AppendField(arrow::ArrayBuilder *builder, const char *cursor);
    int32_t AppendRow(const char *cursor);
    std::shared_ptr<arrow::RecordBatch> Finish();

  private:
    std::unique_ptr<arrow::RecordBatchBuilder> builder_;
    DecoderMap decoders_;
};
