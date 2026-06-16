#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/runtime_type_traits.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/utils.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {
template <LogicalType LT>
class FakeDictDecoder final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        using RT = RunTimeCppType<LT>;
        auto* spec_dst = reinterpret_cast<RT*>(dst);
        for (size_t i = 0; i < count; ++i) {
            spec_dst[i] = i;
        }
        return Status::OK();
    }
};

template <>
class FakeDictDecoder<TYPE_VARCHAR> final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        auto* spec_dst = reinterpret_cast<Slice*>(dst);
        for (size_t i = 0; i < count; ++i) {
            auto data = std::to_string(i);
            Slice slice = Slice(_pool.allocate(data.size()), data.size());
            memcpy(slice.data, data.data(), data.size());
            spec_dst[i] = slice;
        }
        return Status::OK();
    }

private:
    MemPool _pool;
};

static Slice unquote(Slice slice) {
    if ((slice.starts_with("\"") && slice.ends_with("\"")) || (slice.starts_with("'") && slice.ends_with("'"))) {
        slice.remove_prefix(1);
        slice.remove_suffix(1);
    }
    return slice;
}

#define EXPECTED_UNQUOTE(lhs, rhs) EXPECT_EQ(unquote(lhs), rhs)

template <LogicalType DICT_TYPE, LogicalType TARGET_TYPE>
void dict_encoding_test() {
    using DICT_CXX_TYPE = RunTimeCppType<DICT_TYPE>;
    using TARGET_CXX_TYPE = RunTimeCppType<TARGET_TYPE>;
    faststring fs;
    {
        RleEncoder<DICT_CXX_TYPE> encoder(&fs, 32);
        for (size_t i = 0; i < 4096; ++i) {
            encoder.Put(i % 9 + 1, 10);
        }
    }

    DictDecoder<TARGET_CXX_TYPE> decoder;
    FakeDictDecoder<TARGET_TYPE> inner_decoder;
    faststring fs2;
    fs2.resize(fs.length() + 1);
    fs2.data()[0] = 32;
    memcpy(fs2.data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder.set_data(Slice(fs2.data(), fs2.length())));
    ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

    // read dict code
    size_t chunk_size = 4095;
    NullInfos infos;
    infos.reset_with_capacity(chunk_size);
    {
        // interleave
        infos.num_nulls = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        infos.num_ranges = chunk_size / 2;
    }
    {
        auto type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "1");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "1");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        auto type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(),
                                                filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "7");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "7");
        EXPECT_EQ(dst->size(), chunk_size);
    }

    {
        // sparse
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.nulls_data()[0] = 0;
        infos.nulls_data()[1000] = 0;
        infos.nulls_data()[2000] = 0;
        infos.nulls_data()[3000] = 0;
        infos.nulls_data()[4000] = 0;

        infos.num_nulls = chunk_size - 5;
        infos.num_ranges = chunk_size / 2;
    }
    {
        auto type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "5");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        auto type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        decoder.set_dict_size_threshold(0);
        auto type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[1] = 0;
        filter[1000] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2000), "6");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all filtered
        decoder.set_dict_size_threshold(0);
        auto type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x00, chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all null
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.num_nulls = chunk_size;
        auto type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
}

TEST(DictEncodingReadTest, BasicTest) {
    dict_encoding_test<LogicalType::TYPE_INT, LogicalType::TYPE_INT>();
}

TEST(DictEncodingReadTest, BinaryPageTest) {
    dict_encoding_test<LogicalType::TYPE_INT, LogicalType::TYPE_VARCHAR>();
}

// Build a ready-to-read DictDecoder<Slice> backed by an int-keyed dictionary, mirroring the setup
// in dict_encoding_test().
static void setup_slice_dict_decoder(DictDecoder<Slice>* decoder, FakeDictDecoder<TYPE_VARCHAR>* inner_decoder,
                                     faststring* backing) {
    faststring fs;
    RleEncoder<int32_t> encoder(&fs, 32);
    for (size_t i = 0; i < 4096; ++i) {
        encoder.Put(i % 9 + 1, 10);
    }
    backing->resize(fs.length() + 1);
    backing->data()[0] = 32;
    memcpy(backing->data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder->set_data(Slice(backing->data(), backing->length())));
    ASSERT_OK(decoder->set_dict(10, 10, inner_decoder));
}

// DictDecoder<Slice> must reject a non-binary destination column instead of blindly down-casting it
// to BinaryColumn. This guards the path that could otherwise let a temporary Int32 dict-code column
// reach a string slot. Exercises both the filtered _next_batch_value() path and the
// next_value_batch_with_nulls() path.
TEST(DictEncodingReadTest, BinaryDestinationTypeGuard) {
    constexpr size_t count = 100;

    // 1. _next_batch_value() with a filter: a binary destination succeeds.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst.get()->size(), count);
    }

    // 2. _next_batch_value() with a filter: a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }

    // 3. next_value_batch_with_nulls(): a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        NullInfos infos;
        infos.reset_with_capacity(count);
        infos.num_nulls = 0;
        for (size_t i = 0; i < count; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        // num_ranges > 2 routes to next_value_batch_with_nulls() rather than the row-by-row fallback.
        infos.num_ranges = count / 2;

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }
}

// Regression: the dict decoder must not leave the data slots of NULL rows holding
// uninitialized memory. The sparse / filter branches only write non-null positions, so the
// data column must be zero-initialized (resize, not resize_uninitialized). Otherwise a
// throw-checking narrowing cast reads that garbage (before consulting the null map) and
// spuriously rejects it (e.g. "<garbage> conflict with range of (...)").
TEST(DictEncodingReadTest, NullSlotsAreZeroFilled) {
    using TARGET_CXX_TYPE = int64_t;
    constexpr LogicalType TARGET_TYPE = TYPE_BIGINT;

    // dict codes 1..9, dictionary values 0..9 (set up exactly like dict_encoding_test()).
    faststring fs;
    int fs_len = 0;
    {
        RleEncoder<int32_t> encoder(&fs, 32);
        for (size_t i = 0; i < 4096; ++i) {
            encoder.Put(i % 9 + 1, 10);
        }
        // Flush the final pending run into fs; RleEncoder has no flushing destructor,
        // so without this the trailing run is truncated (see DictEncoder::build()).
        fs_len = encoder.Flush();
    }
    DictDecoder<TARGET_CXX_TYPE> decoder;
    FakeDictDecoder<TARGET_TYPE> inner_decoder;
    faststring fs2;
    fs2.resize(fs_len + 1);
    fs2.data()[0] = 32;
    memcpy(fs2.data() + 1, fs.data(), fs_len);
    ASSERT_OK(decoder.set_data(Slice(fs2.data(), fs2.length())));
    ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

    constexpr size_t chunk_size = 4095;
    NullInfos infos;
    infos.reset_with_capacity(chunk_size);
    // mostly NULL with a few scattered non-nulls -> sparse / filter branch, and num_ranges > 2
    // so it stays on the dict path instead of the row-by-row fallback.
    for (size_t i = 0; i < chunk_size; ++i) {
        infos.nulls_data()[i] = 1;
    }
    for (size_t idx : {0, 1000, 2000, 3000, 4000}) {
        infos.nulls_data()[idx] = 0;
    }
    infos.num_nulls = chunk_size - 5;
    infos.num_ranges = chunk_size / 2;

    constexpr TARGET_CXX_TYPE kPoison = static_cast<TARGET_CXX_TYPE>(0xA5A5A5A5A5A5A5A5ULL);

    auto run = [&](const uint8_t* filter) {
        auto dst = ColumnHelper::create_column(TypeDescriptor(TARGET_TYPE), true);
        auto* nullable = down_cast<NullableColumn*>(dst.get());
        auto* data_col = down_cast<Int64Column*>(nullable->data_column_raw_ptr());
        // Poison the backing store, then shrink to 0 so the decoder's resize reuses these bytes.
        data_col->resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            data_col->get_data()[i] = kPoison;
        }
        data_col->resize(0);
        nullable->null_column_raw_ptr()->resize(0);

        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter));

        // Every NULL row's data slot must be deterministically zeroed, not the poison.
        for (size_t i = 0; i < chunk_size; ++i) {
            if (infos.nulls_data()[i]) {
                ASSERT_EQ(data_col->get_data()[i], 0) << "null slot " << i << " not zero-filled";
            }
        }
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    };

    // sparse branch (no filter)
    run(nullptr);

    // filter branch: lower the dict-size threshold so the filter is forwarded to the dict path.
    decoder.set_dict_size_threshold(0);
    auto filter = std::make_unique<uint8_t[]>(chunk_size);
    memset(filter.get(), 0x01, chunk_size);
    run(filter.get());
}
} // namespace starrocks::parquet