#include "./pg2arrow.h"

#include "./hton.h"

using namespace arrow;

namespace Pg2Arrow {

struct IdMapper {
    static inline const char* Unpack(const char* x) { return x; }
};

struct TimestampMapper {
    static const int64_t kEpoch = 946684800000000;  // 2000-01-01 - 1970-01-01 (us)
    static inline int64_t Unpack(const char* x) { return unpack_int64(x) + kEpoch; }
};

struct DateMapper {
    static const int32_t kEpoch = 10957;  // 2000-01-01 - 1970-01-01 (days)
    static inline int32_t Unpack(const char* x) { return unpack_int32(x) + kEpoch; }
};

struct IntervalMapper {
    static const int64_t kMicrosecondsPerDay = 24 * 3600 * 1000000LL;
    static inline int64_t Unpack(const char* x) {
        int64_t msecs = unpack_int64(x);
        int32_t days = unpack_int32(x + 8);
        // int32_t months = unpack_int32(cursor + 12);
        return msecs + days * kMicrosecondsPerDay;
    }
};

struct BoolMapper {
    static inline bool Unpack(const char* x) { return (*x != 0); }
};

struct Int16Mapper {
    static inline int16_t Unpack(const char* x) { return unpack_int16(x); }
};

struct Int32Mapper {
    static inline int64_t Unpack(const char* x) { return unpack_int32(x); }
};

struct Int64Mapper {
    static inline int64_t Unpack(const char* x) { return unpack_int64(x); }
};

struct FloatMapper {
    static inline float Unpack(const char* x) { return unpack_float(x); }
};

struct DoubleMapper {
    static inline double Unpack(const char* x) { return unpack_double(x); }
};

template <typename T, typename F>
int32_t GenericDecoder(DecoderMap&, ArrayBuilder* builder, const char* cursor) {
    int32_t flen = unpack_int32(cursor);
    cursor += 4;

    if (flen == -1) {
        if constexpr (
            std::is_base_of<T, FloatBuilder>::value ||
            std::is_base_of<T, DoubleBuilder>::value)
            auto status = ((T*)builder)->Append(NAN);
        else
            auto status = builder->AppendNull();
        return 4;
    }

    auto value = F::Unpack(cursor);
    if constexpr (
        std::is_base_of<T, BinaryBuilder>::value ||
        std::is_base_of<T, StringBuilder>::value ||
        std::is_base_of<T, StringDictionaryBuilder>::value)
        auto status = ((T*)builder)->Append(value, flen);
    else
        auto status = ((T*)builder)->Append(value);

    return 4 + flen;
}

int32_t ListDecoder(DecoderMap& decoders, ArrayBuilder* builder, const char* cursor) {
    int32_t flen = unpack_int32(cursor);
    cursor += 4;

    if (flen == -1) {
        auto status = builder->AppendNull();
        return 4;
    }

    int32_t ndims = unpack_int32(cursor);
    cursor += 4;
    // int32_t hasnulls = unpack_int32(cursor);
    cursor += 4;
    // int32_t elem_oid = unpack_int32(cursor);
    cursor += 4;

    // Element will be flattened
    int32_t total_elem = 1;
    for (size_t i = 0; i < ndims; i++) {
        int32_t dim_sz = unpack_int32(cursor);
        cursor += 4;
        // int32_t dim_lb = unpack_int32(cursor);
        cursor += 4;
        total_elem *= dim_sz;
    }

    auto lbuilder = (ListBuilder*)builder;
    auto status = lbuilder->Append();
    auto value_builder = lbuilder->value_builder();
    auto decoder = decoders[value_builder];
    for (size_t i = 0; i < total_elem; i++) {
        cursor += decoder(decoders, value_builder, cursor);
    }

    return 4 + flen;
}

int32_t StructDecoder(DecoderMap& decoders, ArrayBuilder* builder, const char* cursor) {
    int32_t flen = unpack_int32(cursor);
    cursor += 4;

    if (flen == -1) {
        auto status = builder->AppendNull();
        return 4;
    }

    auto sbuilder = (StructBuilder*)builder;
    auto status = sbuilder->Append();

    int32_t nvalids = unpack_int32(cursor);
    cursor += 4;

    int num_fields = sbuilder->num_fields();
    for (size_t i = 0; i < num_fields; i++) {
        if (i >= nvalids) {
            // TODO: check postgres ref code
            status = sbuilder->field_builder(i)->AppendNull();
            continue;
        }
        // int32_t elem_oid = unpack_int32(cursor);
        cursor += 4;

        auto item_builder = sbuilder->field_builder(i);
        cursor += decoders[item_builder](decoders, item_builder, cursor);
    }

    return 4 + flen;
}

std::map<Type::type, FieldDecoder> gDecoderMap = {
    {Type::type::BOOL, GenericDecoder<BooleanBuilder, BoolMapper>},
    {Type::type::INT16, GenericDecoder<Int16Builder, Int16Mapper>},
    {Type::type::INT32, GenericDecoder<Int32Builder, Int32Mapper>},
    {Type::type::INT64, GenericDecoder<Int64Builder, Int64Mapper>},
    {Type::type::FLOAT, GenericDecoder<FloatBuilder, FloatMapper>},
    {Type::type::DOUBLE, GenericDecoder<DoubleBuilder, DoubleMapper>},
    {Type::type::STRING, GenericDecoder<StringBuilder, IdMapper>},
    {Type::type::BINARY, GenericDecoder<BinaryBuilder, IdMapper>},
    {Type::type::FIXED_SIZE_BINARY, GenericDecoder<FixedSizeBinaryBuilder, IdMapper>},
    {Type::type::DATE32, GenericDecoder<Date32Builder, DateMapper>},
    {Type::type::TIMESTAMP, GenericDecoder<TimestampBuilder, TimestampMapper>},
    {Type::type::TIME64, GenericDecoder<Time64Builder, Int64Mapper>},
    {Type::type::DURATION, GenericDecoder<DurationBuilder, IntervalMapper>},
    {Type::type::DICTIONARY, GenericDecoder<StringDictionaryBuilder, IdMapper>},
    {Type::type::LIST, ListDecoder},
    {Type::type::STRUCT, StructDecoder}};

void InitDecoders(DecoderMap& decoders, ArrayBuilder* builder) {
    auto type = builder->type()->id();
    decoders[builder] = gDecoderMap[type];

    if (type == Type::type::LIST) {
        InitDecoders(decoders, ((ListBuilder*)builder)->value_builder());
    } else if (type == Type::type::STRUCT) {
        auto sbuilder = (StructBuilder*)builder;
        for (size_t i = 0; i < sbuilder->num_fields(); i++) {
            InitDecoders(decoders, sbuilder->field_builder(i));
        }
    }
}

PgBuilder::PgBuilder(std::shared_ptr<arrow::Schema> schema) {
    auto status = RecordBatchBuilder::Make(schema, default_memory_pool(), &builder_);
    for (size_t i = 0; i < builder_->num_fields(); i++) {
        InitDecoders(decoders_, builder_->GetField(i));
    }
}

int32_t PgBuilder::Append(const char* cursor) {
    const char* cur = cursor;
    int16_t nfields = unpack_int16(cur);
    cur += 2;

    if (nfields == -1)
        return 2;

    for (size_t i = 0; i < nfields; i++) {
        auto field_builder = builder_->GetField(i);
        cur += decoders_[field_builder](decoders_, field_builder, cur);
    }
    return cur - cursor;
}

arrow::Status PgBuilder::Flush(std::shared_ptr<arrow::RecordBatch>* batch) {
    return builder_->Flush(batch);
}

}  // namespace Pg2Arrow
