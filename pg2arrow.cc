#include "./pg2arrow.h"
#include "./hton.h"

using namespace arrow;

#define CAST(typ) static_cast<typ *>(builder)
#define CHECK(typ) (dynamic_cast<typ *>(builder) != nullptr)

int32_t Timestamp(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                  int32_t flen) {
    int64_t v = unpack_int64(cursor) + 946684800000000;
    auto status = CAST(TimestampBuilder)->Append(v);
    return 8;
}

int32_t Float(PgBuilder &, ArrayBuilder *builder, const char *cursor,
              int32_t flen) {
    float v = unpack_float(cursor);
    auto status = CAST(FloatBuilder)->Append(v);
    return 4;
}

int32_t Double(PgBuilder &, ArrayBuilder *builder, const char *cursor,
               int32_t flen) {
    double v = unpack_double(cursor);
    auto status = CAST(DoubleBuilder)->Append(v);
    return 8;
}

int32_t Int16(PgBuilder &, ArrayBuilder *builder, const char *cursor,
              int32_t flen) {
    int16_t v = unpack_int16(cursor);
    auto status = CAST(Int16Builder)->Append(v);
    return 2;
}

int32_t Int32(PgBuilder &, ArrayBuilder *builder, const char *cursor,
              int32_t flen) {
    int32_t v = unpack_int32(cursor);
    auto status = CAST(Int32Builder)->Append(v);
    return 4;
}

int32_t Int64(PgBuilder &, ArrayBuilder *builder, const char *cursor,
              int32_t flen) {
    int64_t v = unpack_int64(cursor);
    auto status = CAST(Int64Builder)->Append(v);
    return 8;
}

int32_t List(PgBuilder &pg_builder, ArrayBuilder *builder, const char *cursor,
             int32_t flen) {
    const char *cur = cursor;

    auto list_builder = CAST(ListBuilder);
    auto status = list_builder->Append();

    int32_t ndims = unpack_int32(cur);
    cur += 4;
    // int32_t hasnulls = unpack_int32(cur);
    cur += 4;
    // int32_t elem_oid = unpack_int32(cur);
    cur += 4;

    int32_t total_elem = 1;
    for (size_t i = 0; i < ndims; i++) {
        int32_t dim_sz = unpack_int32(cur);
        cur += 4;
        // int32_t dim_lb = unpack_int32(cur);
        cur += 4;
        total_elem *= dim_sz;
    }

    auto inner_builder = list_builder->value_builder();
    for (size_t i = 0; i < total_elem; i++) {
        cur += pg_builder.AppendField(inner_builder, cur);
    }

    return cur - cursor;
}

int32_t Struct(PgBuilder &pg_builder, ArrayBuilder *builder, const char *cursor,
               int32_t flen) {
    const char *cur = cursor;

    auto struct_builder = CAST(StructBuilder);
    auto status = struct_builder->Append();

    int32_t nvalids = unpack_int32(cur);
    cur += 4;

    int num_fields = struct_builder->num_fields();
    for (size_t i = 0; i < num_fields; i++) {
        if (i >= nvalids) {
            // TODO: check postgres ref code
            status = struct_builder->field_builder(i)->AppendNull();
            continue;
        }
        // int32_t elem_oid = unpack_int32(cur);
        cur += 4;
        cur += pg_builder.AppendField(struct_builder->field_builder(i), cur);
    }

    return cur - cursor;
}

int32_t Dictionary(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                   int32_t flen) {
    auto status = CAST(DictionaryBuilder<StringType>)->Append(cursor, flen);
    return flen;
}

int32_t String(PgBuilder &, ArrayBuilder *builder, const char *cursor,
               int32_t flen) {
    auto status = CAST(StringBuilder)->Append(cursor, flen);
    return flen;
}

void BuildDecoders(PgBuilder::DecoderMap &decoders, ArrayBuilder *builder) {
    if (CHECK(TimestampBuilder)) {
        decoders[builder] = Timestamp;
    } else if (CHECK(FloatBuilder)) {
        decoders[builder] = Float;
    } else if (CHECK(DoubleBuilder)) {
        decoders[builder] = Double;
    } else if (CHECK(Int16Builder)) {
        decoders[builder] = Int16;
    } else if (CHECK(Int32Builder)) {
        decoders[builder] = Int32;
    } else if (CHECK(Int64Builder)) {
        decoders[builder] = Int64;
    } else if (CHECK(ListBuilder)) {
        BuildDecoders(decoders, CAST(ListBuilder)->value_builder());
        decoders[builder] = List;
    } else if (CHECK(StructBuilder)) {
        auto builder_ = CAST(StructBuilder);
        for (size_t i = 0; i < builder_->num_fields(); i++) {
            BuildDecoders(decoders, builder_->field_builder(i));
        }
        decoders[builder] = Struct;
    } else if (CHECK(DictionaryBuilder<StringType>)) {
        decoders[builder] = Dictionary;
    } else if (CHECK(StringBuilder)) {
        decoders[builder] = String;
    }
}

PgBuilder::PgBuilder(std::shared_ptr<Schema> schema) {
    Status status;
    auto memory_pool = default_memory_pool();
    status = RecordBatchBuilder::Make(schema, memory_pool, &builder_);

    for (size_t i = 0; i < builder_->num_fields(); i++) {
        BuildDecoders(decoders_, builder_->GetField(i));
    }
}

int32_t PgBuilder::AppendField(ArrayBuilder *builder, const char *cursor) {
    int32_t flen = unpack_int32(cursor);
    cursor += 4;

    if (flen == -1) {
        auto status = builder->AppendNull();
        return 4;
    }

    decoders_[builder](*this, builder, cursor, flen);
    return 4 + flen;
}

int32_t PgBuilder::AppendRow(const char *cursor) {
    const char *cur = cursor;
    int nfields = unpack_int16(cur);
    cur += 2;

    if (nfields == -1)
        return 2;

    for (size_t i = 0; i < nfields; i++) {
        cur += AppendField(builder_->GetField(i), cur);
    }
    return cur - cursor;
}

std::shared_ptr<RecordBatch> PgBuilder::Finish() {
    std::shared_ptr<RecordBatch> batch;
    auto status = builder_->Flush(&batch);
    return batch;
}
