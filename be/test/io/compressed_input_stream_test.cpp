// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io/compressed_input_stream.h"

#include <gtest/gtest.h>

#include <memory>

#include "fs/fs_posix.h"
#include "io/string_input_stream.h"
#include "io_test_base.h"
#include "testutil/assert.h"
#include "util/compression/block_compression.h"
#include "util/compression/compression_context_pool_singletons.h"
#include "util/compression/stream_compression.h"
namespace starrocks::io {

class CompressedInputStreamTest : public ::testing::Test {
protected:
    struct TestCase {
        Slice data;
        size_t read_buff_len;
        size_t compressed_buff_len;
    };

    struct ReadContext {
        size_t read_buffer_size = 1024;
        size_t decompressor_buffer_size = 8 * 1024 * 1024;
    };

    std::shared_ptr<InputStream> LZ4F_compress_to_file(const Slice& content) {
        const BlockCompressionCodec* codec = nullptr;
        CHECK(get_block_compression_codec(LZ4_FRAME, &codec).ok());
        size_t max_compressed_len = codec->max_compressed_len(content.size);
        std::string compressed_data(max_compressed_len, '\0');
        Slice buff(compressed_data);
        LOG(ERROR) << "BEFORE_COMPRESS: " << content.size;;
        CHECK(codec->compress(content, &buff).ok());
        LOG(ERROR) << "AFTER_COMPRESS: " << buff.size;
        compressed_data.resize(buff.size);
        return std::shared_ptr<InputStream>(new StringInputStream(std::move(compressed_data)));
    }

    std::shared_ptr<StreamCompression> LZ4F_decompressor() {
        std::unique_ptr<StreamCompression> dec;
        CHECK(StreamCompression::create_decompressor(CompressionTypePB::LZ4_FRAME, &dec).ok());
        return std::shared_ptr<StreamCompression>(dec.release());
    }

    void printStringAsHex(const std::string& str) {
        // 遍历字符串中的每个字符
        for (char c : str) {
            // 1. 转换为unsigned char，避免符号扩展
            // 2. std::hex：启用十六进制输出
            // 3. std::setw(2)：每个十六进制占2位
            // 4. std::setfill('0')：不足2位时补0（比如0x0a而不是0xa）
            std::cout << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<unsigned int>(static_cast<unsigned char>(c))
                      << " "; // 加空格分隔，提升可读性
        }
        // 恢复默认输出格式（避免后续输出受hex影响）
        std::cout << std::dec << std::endl;
    }

    void test_lz4f_cases(const TestCase& t) {
        //const std::string STR_9 = random_string(9);
        //auto small = LZ4F_compress_to_file(STR_9);
        const char s9Buffer[9] = {0};
        char d9Buffer[sizeof(s9Buffer)];
        size_t const c9SizeBound = LZ4F_compressFrameBound(sizeof(s9Buffer), NULL);
        void* const c9Buffer = malloc(c9SizeBound);
        /* First compress a valid frame */
        LZ4F_preferences_t pref = LZ4F_INIT_PREFERENCES;
        size_t c9Size;
        pref.frameInfo.contentSize = sizeof(s9Buffer);
        {
            c9Size = LZ4F_compressFrame(c9Buffer, c9SizeBound, s9Buffer, sizeof(s9Buffer), &pref);
            std::cout << "compressFrame: " << c9Size << std::endl;
        }

        std::string compressed_data(reinterpret_cast<char*>(c9Buffer), c9Size);
        printStringAsHex(compressed_data);

        auto large = std::shared_ptr<InputStream>(new StringInputStream(std::move(compressed_data)));

        /*
        std::string STR_100;
        STR_100.resize(9);
        STR_100.data()[0] = '\0';
        auto large = LZ4F_compress_to_file(STR_100);
        */

        auto f = std::make_shared<CompressedInputStream>(large, LZ4F_decompressor(), 9);
        std::string decompressed_data;
        std::string own_buff(128 * 1024, '\0');
        decompressed_data.reserve(t.data.size);

        //ASSIGN_OR_ABORT(auto nread, f->read(own_buff.data(), own_buff.size()));
        //decompressed_data.append(own_buff.data(), nread);
        int64_t nread = 0;
        auto ret = f->read(own_buff.data(), 5);
        LOG(ERROR) << "read size: " << ret.value();
        //ASSIGN_OR_ABORT(nread, f->read(own_buff.data(), 20));
        //LOG(ERROR) << "read size: " << nread;
        //ASSIGN_OR_ABORT(nread, f->read(own_buff.data(), 20));
        //LOG(ERROR) << "read size: " << nread;
        f.reset();

        //auto f2 = std::make_shared<CompressedInputStream>(small, LZ4F_decompressor(), 9);
        //ASSIGN_OR_ABORT(nread, f2->read(own_buff.data(), 9));
        //LOG(ERROR) << "read size: " << nread;

        /*
        while (nread > 0) {
            decompressed_data.append(own_buff.data(), nread);
            ASSIGN_OR_ABORT(nread, f->read(own_buff.data(), own_buff.size()));
        }
        */
        //ASSERT_EQ(t.data, decompressed_data);
    }

    void read_compressed_file_ctx(CompressionTypePB type, const char* path, std::string& out, const ReadContext& ctx) {
        auto fs = new_fs_posix();
        auto st = fs->new_random_access_file(path);
        ASSERT_TRUE(st.ok()) << st.status().message();
        auto file = std::move(st.value());

        using DecompressorPtr = std::shared_ptr<StreamCompression>;
        std::unique_ptr<StreamCompression> dec;
        StreamCompression::create_decompressor(type, &dec);

        auto compressed_input_stream = std::make_shared<io::CompressedInputStream>(
                file->stream(), DecompressorPtr(dec.release()), ctx.decompressor_buffer_size);

        std::vector<char> vec_buf(ctx.read_buffer_size + 1);
        char* buf = vec_buf.data();

        for (;;) {
            auto st = compressed_input_stream->read(buf, ctx.read_buffer_size);
            ASSERT_TRUE(st.ok()) << st.status().message();
            uint64_t sz = st.value();
            if (sz == 0) break;
            buf[sz] = 0;
            out += buf;
        }
    }

    void read_compressed_file(CompressionTypePB type, const char* path, std::string& out) {
        ReadContext ctx;
        read_compressed_file_ctx(type, path, out, ctx);
    }
};

// NOLINTNEXTLINE
TEST_F(CompressedInputStreamTest, test_LZ4F) {
    const size_t K1 = 1024;
    const size_t M1 = 1024 * 1024;
    const std::string STR_1K = random_string(K1);
    const std::string STR_1M = random_string(M1);
    const std::string STR_10M = random_string(10 * M1);
    const std::string STR_100M = random_string(100 * M1);

    TestCase case1 = {STR_10M, K1, 2 * K1};
    test_lz4f_cases(case1);
}

TEST_F(CompressedInputStreamTest, test_LZO0) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test0.csv.lzo";
    std::string out;
    read_compressed_file(CompressionTypePB::LZO, path, out);
    std::string expected = R"(Alice,1
Bob,2
CharlieX,3
)";
    std::cout << out << "\n";
    ASSERT_EQ(out, expected);
}

TEST_F(CompressedInputStreamTest, test_LZO1) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test1.csv.lzo";

    std::string head = R"(0,1
1,2
2,3
3,4
4,5
5,6
6,)";

    std::string tail = R"(9998
99998,99999
99999,100000
)";

    std::vector<size_t> decompressor_buffer_sizes = {
            1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    std::vector<size_t> read_buffer_sizes = {
            32, 1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    for (size_t decompressor_buffer_size : decompressor_buffer_sizes) {
        for (size_t read_buffer_size : read_buffer_sizes) {
            std::cout << "test lzo1: read_buffer_size: " << read_buffer_size
                      << ", decompressor_buffer_size: " << decompressor_buffer_size << std::endl;
            std::string out;
            ReadContext ctx{.read_buffer_size = read_buffer_size, .decompressor_buffer_size = decompressor_buffer_size};
            read_compressed_file_ctx(CompressionTypePB::LZO, path, out, ctx);
            ASSERT_EQ(out.size(), 1177785);
            ASSERT_EQ(out.substr(0, head.size()), head);
            ASSERT_EQ(out.substr(out.size() - tail.size(), tail.size()), tail);
        }
    }
}

TEST_F(CompressedInputStreamTest, test_Snappy0) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test0.csv.snappy";
    std::string out;
    read_compressed_file(CompressionTypePB::SNAPPY, path, out);
    std::string expected = R"(Alice,1
Bob,2
CharlieX,3
)";
    std::cout << out << "\n";
    ASSERT_EQ(out, expected);
}

TEST_F(CompressedInputStreamTest, test_Snappy1) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test1.csv.snappy";

    std::string head = R"(0,1
1,2
2,3
3,4
4,5
5,6
6,)";

    std::string tail = R"(9998
99998,99999
99999,100000
)";

    std::vector<size_t> buffer_sizes = {
            1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    for (size_t read_buffer_size : buffer_sizes) {
        std::string out;
        ReadContext ctx{.read_buffer_size = read_buffer_size};
        read_compressed_file_ctx(CompressionTypePB::SNAPPY, path, out, ctx);
        ASSERT_EQ(out.size(), 1177785);
        ASSERT_EQ(out.substr(0, head.size()), head);
        ASSERT_EQ(out.substr(out.size() - tail.size(), tail.size()), tail);
    }
}

} // namespace starrocks::io
