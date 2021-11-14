#include "./pg2arrow.h"
#include "./hton.h"

using namespace arrow;

int32_t decode_Timestamp(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                         int32_t flen) {
    int64_t v = unpack_int64(cursor) + 946684800000000;
    auto status = static_cast<TimestampBuilder *>(builder)->Append(v);
    return 8;
}

int32_t decode_Float(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                     int32_t flen) {
    float v = unpack_float(cursor);
    auto status = static_cast<FloatBuilder *>(builder)->Append(v);
    return 4;
}

int32_t decode_Double(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                      int32_t flen) {
    double v = unpack_double(cursor);
    auto status = static_cast<DoubleBuilder *>(builder)->Append(v);
    return 8;
}

int32_t decode_Int16(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                     int32_t flen) {
    int16_t v = unpack_int16(cursor);
    auto status = static_cast<Int16Builder *>(builder)->Append(v);
    return 2;
}

int32_t decode_Int32(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                     int32_t flen) {
    int32_t v = unpack_int32(cursor);
    auto status = static_cast<Int32Builder *>(builder)->Append(v);
    return 4;
}

int32_t decode_Int64(PgBuilder &, ArrayBuilder *builder, const char *cursor,
                     int32_t flen) {
    int64_t v = unpack_int64(cursor);
    auto status = static_cast<Int64Builder *>(builder)->Append(v);
    return 8;
}

int32_t decode_List(PgBuilder &b, ArrayBuilder *builder, const char *cursor,
                    int32_t flen) {
    const char *cur = cursor;

    auto list_builder = static_cast<ListBuilder *>(builder);
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
        cur += b.AppendField(inner_builder, cur);
    }

    return cur - cursor;
}

int32_t decode_Struct(PgBuilder &b, ArrayBuilder *builder, const char *cursor,
                      int32_t flen) {
    const char *cur = cursor;

    auto struct_builder = static_cast<StructBuilder *>(builder);
    auto status = struct_builder->Append();

    // int32_t nvalids = unpack_int32(cur);
    cur += 4;

    int num_fields = struct_builder->num_fields();
    for (size_t i = 0; i < num_fields; i++) {
        // TODO: check postgres ref code, seems a bit odd
        // if (i >= nvalids)
        // {
        //     struct_builder->AppendNull();
        //     continue;
        // }
        // int32_t elem_oid = unpack_int32(cur);
        cur += 4;
        cur += b.AppendField(struct_builder->field_builder(i), cur);
    }

    return cur - cursor;
}

int32_t decode_Dictionary(PgBuilder &b, ArrayBuilder *builder,
                          const char *cursor, int32_t flen) {
    auto status = static_cast<DictionaryBuilder<StringType> *>(builder)->Append(
        cursor, flen);
    return flen;
}

int32_t decode_String(PgBuilder &b, ArrayBuilder *builder, const char *cursor,
                      int32_t flen) {
    auto status = static_cast<StringBuilder *>(builder)->Append(cursor, flen);
    return flen;
}

#define CHECK(typ) (dynamic_cast<typ *>(builder) != nullptr)

void _build_decoders(PgBuilder::DecoderMap &decoders, ArrayBuilder *builder) {
    if (CHECK(TimestampBuilder)) {
        decoders[builder] = decode_Timestamp;
    } else if (CHECK(FloatBuilder)) {
        decoders[builder] = decode_Float;
    } else if (CHECK(DoubleBuilder)) {
        decoders[builder] = decode_Double;
    } else if (CHECK(Int16Builder)) {
        decoders[builder] = decode_Int16;
    } else if (CHECK(Int32Builder)) {
        decoders[builder] = decode_Int32;
    } else if (CHECK(Int64Builder)) {
        decoders[builder] = decode_Int64;
    } else if (CHECK(ListBuilder)) {
        _build_decoders(decoders,
                        static_cast<ListBuilder *>(builder)->value_builder());
        decoders[builder] = decode_List;
    } else if (CHECK(StructBuilder)) {
        auto builder_ = dynamic_cast<StructBuilder *>(builder);
        for (size_t i = 0; i < builder_->num_fields(); i++) {
            _build_decoders(decoders, builder_->field_builder(i));
        }
        decoders[builder] = decode_Struct;
    } else if (CHECK(DictionaryBuilder<StringType>)) {
        decoders[builder] = decode_Dictionary;
    } else if (CHECK(StringBuilder)) {
        decoders[builder] = decode_String;
    }
}

PgBuilder::PgBuilder(std::shared_ptr<Schema> schema) {
    Status status;
    auto memory_pool = default_memory_pool();
    status = RecordBatchBuilder::Make(schema, memory_pool, &builder_);

    for (size_t i = 0; i < builder_->num_fields(); i++) {
        _build_decoders(decoders_, builder_->GetField(i));
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
