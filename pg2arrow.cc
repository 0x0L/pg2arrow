#include "./pg2arrow.h"
#include "./hton.h"

using namespace arrow;

#define CAST(typ) static_cast<typ*>(builder)
#define CHECK(typ) (dynamic_cast<typ*>(builder) != nullptr)

int32_t Timestamp(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int64_t v = unpack_int64(cursor) + 946684800000000;
    auto status = CAST(TimestampBuilder)->Append(v);
    return 8;
}

int32_t Date(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int32_t v = unpack_int32(cursor) + 10957;
    auto status = CAST(Date32Builder)->Append(v);
    return 4;
}

int32_t Time(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int64_t v = unpack_int64(cursor);
    auto status = CAST(Time64Builder)->Append(v);
    return 8;
}

int32_t Float(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    float v = unpack_float(cursor);
    auto status = CAST(FloatBuilder)->Append(v);
    return 4;
}

int32_t Double(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    double v = unpack_double(cursor);
    auto status = CAST(DoubleBuilder)->Append(v);
    return 8;
}

int32_t Boolean(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    auto status = CAST(BooleanBuilder)->Append(*cursor != 0);
    return 1;
}

int32_t Int16(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int16_t v = unpack_int16(cursor);
    auto status = CAST(Int16Builder)->Append(v);
    return 2;
}

int32_t Int32(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int32_t v = unpack_int32(cursor);
    auto status = CAST(Int32Builder)->Append(v);
    return 4;
}

int32_t Int64(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int64_t v = unpack_int64(cursor);
    auto status = CAST(Int64Builder)->Append(v);
    return 8;
}

int32_t Duration(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t) {
    int64_t msecs = unpack_int64(cursor);
    int32_t days = unpack_int32(cursor + 8);
    // int32_t months = unpack_int32(cursor + 12);

    const int64_t kMicrosPerDay = 24 * 3600 * 1000000LL;
    int64_t v = msecs + days * kMicrosPerDay;
    auto status = CAST(DurationBuilder)->Append(v);
    return 16;
}

int32_t List(PgBuilder& pgb, ArrayBuilder* builder, const char* cursor, int32_t) {
    const char* cur = cursor;

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
        cur += pgb.AppendField(inner_builder, cur);
    }

    return cur - cursor;
}

int32_t Struct(PgBuilder& pgb, ArrayBuilder* builder, const char* cursor, int32_t) {
    const char* cur = cursor;

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
        cur += pgb.AppendField(struct_builder->field_builder(i), cur);
    }

    return cur - cursor;
}

template <typename T>
int32_t Raw(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t flen) {
    auto status = CAST(T)->Append(cursor, flen);
    return flen;
}

template <typename T>
int32_t Fixed(PgBuilder&, ArrayBuilder* builder, const char* cursor, int32_t flen) {
    auto status = CAST(T)->Append(cursor);
    return flen;
}

void InitDecoders(PgBuilder::DecoderMap& decoders, ArrayBuilder* builder) {
    if (CHECK(TimestampBuilder)) {
        decoders[builder] = Timestamp;
    } else if (CHECK(Date32Builder)) {
        decoders[builder] = Date;
    } else if (CHECK(Time64Builder)) {
        decoders[builder] = Time;
    } else if (CHECK(FloatBuilder)) {
        decoders[builder] = Float;
    } else if (CHECK(DoubleBuilder)) {
        decoders[builder] = Double;
    } else if (CHECK(BooleanBuilder)) {
        decoders[builder] = Boolean;
    } else if (CHECK(Int16Builder)) {
        decoders[builder] = Int16;
    } else if (CHECK(Int32Builder)) {
        decoders[builder] = Int32;
    } else if (CHECK(Int64Builder)) {
        decoders[builder] = Int64;
    } else if (CHECK(DurationBuilder)) {
        decoders[builder] = Duration;
    } else if (CHECK(ListBuilder)) {
        InitDecoders(decoders, CAST(ListBuilder)->value_builder());
        decoders[builder] = List;
    } else if (CHECK(StructBuilder)) {
        auto builder_ = CAST(StructBuilder);
        for (size_t i = 0; i < builder_->num_fields(); i++) {
            InitDecoders(decoders, builder_->field_builder(i));
        }
        decoders[builder] = Struct;
    } else if (CHECK(DictionaryBuilder<StringType>)) {
        decoders[builder] = Raw<DictionaryBuilder<StringType>>;
    } else if (CHECK(StringBuilder)) {
        decoders[builder] = Raw<StringBuilder>;
    } else if (CHECK(BinaryBuilder)) {
        decoders[builder] = Raw<BinaryBuilder>;
    } else if (CHECK(FixedSizeBinaryBuilder)) {
        decoders[builder] = Fixed<FixedSizeBinaryBuilder>;
    }
}

PgBuilder::PgBuilder(std::shared_ptr<Schema> schema) {
    Status status;
    auto memory_pool = default_memory_pool();
    status = RecordBatchBuilder::Make(schema, memory_pool, &builder_);

    for (size_t i = 0; i < builder_->num_fields(); i++) {
        InitDecoders(decoders_, builder_->GetField(i));
    }
}

int32_t PgBuilder::AppendField(ArrayBuilder* builder, const char* cursor) {
    int32_t flen = unpack_int32(cursor);
    cursor += 4;

    if (flen == -1) {
        auto status = builder->AppendNull();
        return 4;
    }

    decoders_[builder](*this, builder, cursor, flen);
    return 4 + flen;
}

int32_t PgBuilder::AppendRow(const char* cursor) {
    const char* cur = cursor;
    int16_t nfields = unpack_int16(cur);
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
