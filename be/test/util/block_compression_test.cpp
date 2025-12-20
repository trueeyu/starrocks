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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/block_compression_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/compression/block_compression.h"

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "common/object_pool.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_pool.h"
#include "testutil/assert.h"
#include "util/compression/compression_context_pool_singletons.h"
#include "util/faststring.h"
#include "util/random.h"
#include "util/raw_container.h"

namespace starrocks {

static std::string random_string(int len) {
    static starrocks::Random rand(20200722);
    std::string s;
    s.reserve(len * 5);
    for (int i = 0; i < len; i++) {
        char c = 'a' + rand.Next() % ('z' - 'a' + 1);
        std::string tmp_str =
                std::to_string(c) + std::to_string(c) + std::to_string(c) + std::to_string(c) + std::to_string(c);
        s.append(tmp_str);
    }
    return s;
}

class BlockCompressionTest : public testing::Test {
public:
    BlockCompressionTest() = default;
    ~BlockCompressionTest() override = default;

    Slice compress();
    void decompress_fail(Slice src_slice);
    void decompress_0(Slice src_slice);

    ObjectPool _pool;
};

static std::string generate_str(size_t len) {
    static char charset[] =
            "01234"
            "01234"
            "01234";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[rand() % sizeof(charset)];
    }
    return result;
}

Slice BlockCompressionTest::compress() {
    const BlockCompressionCodec* codec = nullptr;
    EXPECT_OK(get_block_compression_codec(LZ4_FRAME, &codec));

    std::string src_str;
    src_str.resize(9);
    src_str[0] = '\0';
    src_str[1] = '1';
    src_str[2] = '2';
    src_str[3] = '3';
    src_str[4] = '4';
    src_str[5] = '5';
    src_str[6] = '6';
    src_str[7] = '7';
    src_str[8] = '8';
    std::string* dst_str = _pool.add(new std::string());
    size_t dst_size = codec->max_compressed_len(9);
    dst_str->resize(dst_size);
    Slice src_slice(src_str);
    Slice dst_slice(*dst_str);
    Status st = codec->compress(src_slice, &dst_slice);
    std::cout << "compress: " << st << ":" << dst_slice.size << std::endl;

    return dst_slice;
}

void BlockCompressionTest::decompress_fail(Slice src_slice) {
    const BlockCompressionCodec* codec = nullptr;
    EXPECT_OK(get_block_compression_codec(LZ4_FRAME, &codec));

    std::string dst_str;
    dst_str.resize(9);

    Slice src_slice2(src_slice.data, 15);
    Slice dst_slice(dst_str);
    Status st = codec->decompress(src_slice2, &dst_slice);
    std::cout << "decompress: " << st << ":" << dst_slice.size << std::endl;
}

void BlockCompressionTest::decompress_0(Slice src_slice) {
    const BlockCompressionCodec* codec = nullptr;
    EXPECT_OK(get_block_compression_codec(LZ4_FRAME, &codec));

    size_t src_size = codec->max_compressed_len(0);
    const char* src_buf = (const char*)malloc(src_size);
    char dst_buf[1];
    Slice input(src_buf, src_size);
    Slice output(dst_buf, sizeof(dst_buf));
    Status st = codec->decompress(input, &output);
    std::cout << "decompress: " << st << ":" << output.size << std::endl;
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

TEST_F(BlockCompressionTest, lxh_test2) {
    LZ4F_dctx* dctx;
    unsigned ret = LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION));
    std::cout << "decompress: " << ret << std::endl;

    /* first session */
    {
        const char s9Buffer[9] = {0};
        char d9Buffer[sizeof(s9Buffer)];
        size_t const c9SizeBound = LZ4F_compressFrameBound(sizeof(s9Buffer), NULL);
        void* c9Buffer = malloc(c9SizeBound);
        /* First compress a valid frame */
        LZ4F_preferences_t pref = LZ4F_INIT_PREFERENCES;
        pref.frameInfo.contentSize = sizeof(s9Buffer);
        {
            size_t const c9Size = LZ4F_compressFrame(c9Buffer, c9SizeBound, s9Buffer, sizeof(s9Buffer), &pref);
            std::cout << "compressFrame: " << c9Size << std::endl;
            assert(c9Size > 15);
            /* decompress it, but do not complete the process - state not terminated correctly */
            {
                size_t dstSize = sizeof(d9Buffer);
                size_t srcSize = 17;
                LOG(ERROR) << "real decompress before: " << srcSize << ":" << dstSize << std::endl;
                std::string tmp_str((char*)c9Buffer, srcSize);
                printStringAsHex(tmp_str);
                size_t const d9Size = LZ4F_decompress(dctx, d9Buffer, &dstSize, c9Buffer, &srcSize, NULL);
                LOG(ERROR) << "real decompress after: " << srcSize << ":" << dstSize << std::endl;
                if (LZ4F_isError(d9Size)) {
                    std::cout << "decompress_1: " << d9Size << ":" << LZ4F_getErrorName(d9Size) << ":" << dstSize << std::endl;
                } else {
                    std::cout << "decompress_1 success: " << std::endl;
                }
            }
        }
        free(c9Buffer);
    }

    LZ4F_resetDecompressionContext(dctx);

    /*
    {
        size_t const c0SizeBound = LZ4F_compressFrameBound(0, NULL);
        LOG(ERROR) << "REAL_SIZE: " << c0SizeBound << std::endl;
        void* const c0Buffer = malloc(c0SizeBound);
        char d0Buffer[1];
        {
            size_t const c0Size = LZ4F_compressFrame(c0Buffer, c0SizeBound, NULL, 0, NULL);
            {
                size_t dstSize = sizeof(d0Buffer);
                size_t srcSize = c0Size;
                size_t const d0Size = LZ4F_decompress(dctx, d0Buffer, &dstSize, c0Buffer, &srcSize, NULL);
                if (LZ4F_isError(d0Size)) {
                    std::cout << "decompress_2: " << LZ4F_getErrorName(d0Size) << ":" << dstSize << std::endl;
                } else {
                    std::cout << "decompress_2 success: " << std::endl;
                }
            }
        }
        free(c0Buffer);
    }

    LZ4F_resetDecompressionContext(dctx);
    */

    {
        const char s9Buffer[9] = {0};
        char d9Buffer[sizeof(s9Buffer)];
        size_t const c9SizeBound = LZ4F_compressFrameBound(sizeof(s9Buffer), NULL);
        void* const c9Buffer = malloc(c9SizeBound);
        /* First compress a valid frame */
        LZ4F_preferences_t pref = LZ4F_INIT_PREFERENCES;
        pref.frameInfo.contentSize = sizeof(s9Buffer);
        {
            size_t const c9Size = LZ4F_compressFrame(c9Buffer, c9SizeBound, s9Buffer, sizeof(s9Buffer), &pref);
            std::cout << "compressFrame: " << c9Size << std::endl;
            assert(c9Size > 15);
            /* decompress it, but do not complete the process - state not terminated correctly */
            {
                size_t dstSize = sizeof(d9Buffer);
                size_t srcSize = 15;
                size_t const d9Size = LZ4F_decompress(dctx, d9Buffer, &dstSize, c9Buffer, &srcSize, NULL);
                if (LZ4F_isError(d9Size)) {
                    std::cout << "decompress_3: " << d9Size << ":" << LZ4F_getErrorName(d9Size) << ":" << dstSize << std::endl;
                } else {
                    std::cout << "decompress_3 success: " << std::endl;
                }
            }
        }
    }


    LZ4F_resetDecompressionContext(dctx);

    {
        const char s9Buffer[9] = {0};
        char d9Buffer[sizeof(s9Buffer)];
        size_t const c9SizeBound = LZ4F_compressFrameBound(sizeof(s9Buffer), NULL);
        void* const c9Buffer = malloc(c9SizeBound);
        /* First compress a valid frame */
        LZ4F_preferences_t pref = LZ4F_INIT_PREFERENCES;
        pref.frameInfo.contentSize = sizeof(s9Buffer);
        {
            size_t const c9Size = LZ4F_compressFrame(c9Buffer, c9SizeBound, s9Buffer, sizeof(s9Buffer), &pref);
            std::cout << "compressFrame: " << c9Size << std::endl;
            assert(c9Size > 15);
            /* decompress it, but do not complete the process - state not terminated correctly */
            {
                size_t dstSize = sizeof(d9Buffer);
                size_t srcSize = 20;
                size_t const d9Size = LZ4F_decompress(dctx, d9Buffer, &dstSize, c9Buffer, &srcSize, NULL);
                if (LZ4F_isError(d9Size)) {
                    std::cout << "decompress_4: " << d9Size << ":" << LZ4F_getErrorName(d9Size) << ":" << dstSize << std::endl;
                } else {
                    std::cout << "decompress_4 success: " << std::endl;
                }
            }
        }
    }
}

TEST_F(BlockCompressionTest, lxh_test1) {
    Slice compressed_slice = compress();
    decompress_fail(compressed_slice);
    decompress_0(compressed_slice);
    /*
    const BlockCompressionCodec* codec2 = nullptr;
    st = get_block_compression_codec(LZ4_FRAME, &codec2);
    ASSERT_TRUE(st.ok());
    size_t tmp_len2 = codec2->max_compressed_len(0);
    const char* c0_buf = (const char*)malloc(tmp_len2);
    char buf[1];
    Slice input(c0_buf, tmp_len2);
    Slice output(buf, sizeof(buf));
    st = codec2->decompress(input, &output);
    LOG(ERROR) << "decompress: " << st << std::endl;

    const BlockCompressionCodec* codec3 = nullptr;
    st = get_block_compression_codec(LZ4_FRAME, &codec3);
    ASSERT_TRUE(st.ok());
    uncompressed.resize(20000);
    Slice uncompressed_slice3(uncompressed);
    st = codec3->decompress(compressed_slice, &uncompressed_slice3);
    std::cout << "decompress: " << st << ":" << uncompressed_slice3.size << std::endl;
    */

    /*
    size_t tmp_len = codec->max_compressed_len(0);
    std::cout << "LXH: tmp_len: " << tmp_len << std::endl;
    */
}

void test_single_slice(CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    for (auto size : test_sizes) {
        auto orig = generate_str(size);
        size_t max_len = codec->max_compressed_len(size);
        std::string compressed;
        compressed.resize(max_len);
        {
            Slice compressed_slice(compressed);
            st = codec->compress(orig, &compressed_slice);
            ASSERT_TRUE(st.ok());

            std::string uncompressed;
            uncompressed.resize(size);
            {
                Slice uncompressed_slice(uncompressed);
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_TRUE(st.ok());

                ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
            }

            if (type == starrocks::CompressionTypePB::LZ4) {
                Slice uncompressed_slice(uncompressed);
                const BlockCompressionCodec* lz4_hadoop_codec = nullptr;
                st = get_block_compression_codec(starrocks::CompressionTypePB::LZ4_HADOOP, &lz4_hadoop_codec);
                ASSERT_TRUE(st.ok());
                st = lz4_hadoop_codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_TRUE(st.ok());

                ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
            }

            // buffer not enough for decompress
            // snappy has no return value if given buffer is not enough
            // NOTE: For ZLIB, we even get OK with a insufficient output
            // when uncompressed size is 1
            if ((type == starrocks::CompressionTypePB::ZLIB && uncompressed.size() > 1) &&
                type != starrocks::CompressionTypePB::SNAPPY && uncompressed.size() > 0) {
                Slice uncompressed_slice(uncompressed);
                uncompressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_FALSE(st.ok());
            }
            // corrupt compressed data
            // we use inflate for gzip decompressor, it will return Z_OK for this case
            if (type != starrocks::CompressionTypePB::SNAPPY && type != starrocks::CompressionTypePB::GZIP) {
                Slice uncompressed_slice(uncompressed);
                compressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_FALSE(st.ok());
                compressed_slice.size += 1;
            }
        }
        // buffer not enough for compress
        if (type != starrocks::CompressionTypePB::SNAPPY && size > 0) {
            Slice compressed_slice(compressed);
            compressed_slice.size = 1;
            st = codec->compress(orig, &compressed_slice);
            ASSERT_FALSE(st.ok());
        }
    }
}

TEST_F(BlockCompressionTest, single) {
    test_single_slice(starrocks::CompressionTypePB::LZ4);
    test_single_slice(starrocks::CompressionTypePB::LZ4_FRAME);
}

void test_multi_slices(starrocks::CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    std::vector<std::string> orig_strs;
    for (auto size : test_sizes) {
        orig_strs.emplace_back(generate_str(size));
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    std::string compressed;
    compressed.resize(max_len);
    {
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        ASSERT_TRUE(st.ok());

        std::string uncompressed;
        uncompressed.resize(total_size);
        // normal case
        {
            Slice uncompressed_slice(uncompressed);
            st = codec->decompress(compressed_slice, &uncompressed_slice);
            ASSERT_TRUE(st.ok());

            ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
        }

        if (type == starrocks::CompressionTypePB::LZ4) {
            Slice uncompressed_slice(uncompressed);
            const BlockCompressionCodec* lz4_hadoop_codec = nullptr;
            st = get_block_compression_codec(starrocks::CompressionTypePB::LZ4_HADOOP, &lz4_hadoop_codec);
            ASSERT_TRUE(st.ok());
            st = lz4_hadoop_codec->decompress(compressed_slice, &uncompressed_slice);
            ASSERT_TRUE(st.ok());

            ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
        }
    }

    // buffer not enough failed
    if (type != starrocks::CompressionTypePB::SNAPPY) {
        Slice compressed_slice(compressed);
        compressed_slice.size = 10;
        st = codec->compress(orig, &compressed_slice);
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(BlockCompressionTest, multi) {
    test_multi_slices(starrocks::CompressionTypePB::SNAPPY);
    test_multi_slices(starrocks::CompressionTypePB::ZLIB);
    test_multi_slices(starrocks::CompressionTypePB::LZ4);
    test_multi_slices(starrocks::CompressionTypePB::LZ4_FRAME);
    test_multi_slices(starrocks::CompressionTypePB::ZSTD);
    test_multi_slices(starrocks::CompressionTypePB::GZIP);
}

static const size_t kBenchmarkCompressionTimes = 1000;
static const size_t kBenchmarkCompressionConcurrentThreads = 32;
static const size_t kBenchmarkCompressionMultiSliceNum = 2;
static const size_t str_length = 1024 * 64;

void benchmark_single_slice_compression(starrocks::CompressionTypePB type, std::string& str) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    const std::string& orig = str;
    Slice orig_slices(orig);

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    for (int i = 0; i < kBenchmarkCompressionTimes; i++) {
        std::string compressed;
        compressed.resize(max_len);
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        ASSERT_TRUE(st.ok());
        compressed.resize(compressed_slice.size);
    }
}

TEST_F(BlockCompressionTest, LZ4F_compression_LARGE_PAGE_TEST) {
    std::string str = random_string(1024 * 5);
    CompressionTypePB type = starrocks::CompressionTypePB::LZ4_FRAME;

    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    std::vector<std::string> orig_strs;
    for (int i = 0; i < kBenchmarkCompressionMultiSliceNum; i++) {
        orig_strs.emplace_back(str);
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    raw::RawString compressed;
    Slice compressed_slice;
    st = codec->compress(orig_slices, &compressed_slice, true, total_size, nullptr, &compressed);
    ASSERT_TRUE(st.ok());
}

TEST_F(BlockCompressionTest, test_multi_thread_get_ctx) {
    for (int j = 0; j < 10; j++) {
        std::vector<std::thread> workers;
        for (int cnt = 0; cnt < 30; cnt++) {
            workers.emplace_back([]() {
                for (uint64_t i = 1; i < 1000; i++) {
                    StatusOr<compression::LZ4F_CCtx_Pool::Ref> ref = compression::getLZ4F_CCtx();
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
    }
}

} // namespace starrocks
