#pragma once

#include <arrow/api.h>

#include <map>

namespace Pg2Arrow {

class DecoderMap;
typedef int32_t (*FieldDecoder)(DecoderMap&, arrow::ArrayBuilder*, const char*);

class DecoderMap : public std::map<arrow::ArrayBuilder*, FieldDecoder> {};

class PgBuilder {
   public:
    PgBuilder(std::shared_ptr<arrow::Schema> schema);
    int32_t Append(const char* cursor);
    arrow::Status Flush(std::shared_ptr<arrow::RecordBatch>* batch);

   protected:
    std::unique_ptr<arrow::RecordBatchBuilder> builder_;
    std::vector<std::pair<FieldDecoder, arrow::ArrayBuilder*>> field_builders_;
    DecoderMap decoders_;
};

};  // namespace Pg2Arrow
