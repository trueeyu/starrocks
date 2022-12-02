// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <benchmark/benchmark.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

class IegPerf {
public:
    void SetUp() {
        config::vector_chunk_size = 4096;

        init_types();
    }

    void TearDown() {}

    void init_types();
    void init_src_chunks();
    ColumnPtr init_src_column(const TypeDescriptor& type);
    void do_hash(const Int32Column& col, std::vector<uint32_t>* idxs);
    void do_shuffle(const Chunk& src_chunk, Chunk& dest_chunk, std::vector<uint32_t>& idxs,
                           std::vector<uint32_t>& node_idxs, int idx);
    void do_bench(benchmark::State& state);

private:
    int _column_count = 100;
    int _chunk_count = 400;
    int _node_count = 100;
    int _chunk_size = 4096;
    std::vector<TypeDescriptor> _types;
    std::vector<ChunkPtr> _src_chunks;
};

void IegPerf::init_types() {
    _types.resize(_column_count);
    for (int i = 0; i < _column_count; i++) {
        _types[i] = TypeDescriptor(TYPE_INT);
    }
}

ColumnPtr IegPerf::init_src_column(const TypeDescriptor& type) {
    auto c1 = ColumnHelper::create_column(type, true);
    c1->resize(_chunk_size);
    auto* nullable_col = down_cast<NullableColumn*>(c1.get());
    auto* int_col = down_cast<Int32Column*>(nullable_col->data_column().get());
    for (int k = 0; k < _chunk_size; k++) {
        int_col->get_data()[k] = rand();
    }
}

void IegPerf::init_src_chunks() {
    for (int i = 0; i < _chunk_count; i++) {
        _src_chunks.emplace_back(std::make_unique<Chunk>());
    }

    for (int i = 0; i < _chunk_count; i++) {
        for (int j = 0; j < _column_count; j++) {
            auto col = init_src_column(_types[i]);
            _src_chunks[i]->append_column(col, j);
        }
    }
}

void IegPerf::do_hash(const Int32Column& col, std::vector<uint32_t>* idxs) {
    col.crc32_hash(&(*idxs)[0], 0, _chunk_size);
    for (int i = 0; i < _chunk_size; i++) {
        (*idxs)[i] = (*idxs)[i] % 100;
    }
}

void IegPerf::do_shuffle(const Chunk& src_chunk, Chunk& dest_chunk, std::vector<uint32_t>& idxs,
                         std::vector<uint32_t>& node_idxs, int be_idx) {
    for (int i = 0; i < idxs.size(); i++) {
        if (idxs[i] == be_idx) {
            node_idxs.emplace_back(i);
        }
    }
    dest_chunk.append_selective(src_chunk, node_idxs.data(), 0, node_idxs.size());
}

void IegPerf::do_bench(benchmark::State& state) {
    IegPerf suite;
    suite.SetUp();

    init_src_chunks();

    std::vector<ChunkPtr> chunks;
    for (int i = 0; i < _chunk_count; i++) {
        chunks.emplace_back(std::make_unique<Chunk>());
    }

    for (int i = 0; i < _chunk_count; i++) {
        for (int j = 0; j < _column_count; j++) {
            auto c1 = ColumnHelper::create_column(_types[j], true);
            c1->resize(config::vector_chunk_size);
            auto* nullable_col = down_cast<NullableColumn*>(c1.get());
            auto* int_col = down_cast<Int32Column*>(nullable_col->data_column().get());
            for (int k = 0; k < config::vector_chunk_size; k++) {
                int_col->get_data()[k] = rand();
            }
            chunks[i]->append_column(c1, j);
        }
    }

    state.ResumeTiming();
    std::vector<ChunkPtr> dest_chunk;
    dest_chunk.resize(_node_count);

    for (int i = 0; i < _node_count; i++) {
        dest_chunk[i] = nullptr;
    }

    std::vector<uint32_t> idxs;
    idxs.resize(config::vector_chunk_size);
    std::vector<uint32_t> select_idxs;
    select_idxs.reserve(config::vector_chunk_size);
    for (int i = 0; i < _chunk_count; i++) {
        auto* nullable_col = down_cast<NullableColumn*>(chunks[i]->columns()[i].get());
        auto* int_col = down_cast<Int32Column*>(nullable_col->data_column().get());
        do_hash(*int_col, &idxs);
        select_idxs.resize(0);
        for (int j = 0; j < _node_count; j++) {
            if (dest_chunk[j] == nullptr) {
                dest_chunk[j] = std::make_unique<Chunk>();
            }
            do_shuffle(*chunks[i], *dest_chunk[j], idxs, select_idxs, j);
            if (dest_chunk[j]->num_rows() >= config::vector_chunk_size) {
                dest_chunk[j].reset();
            }
        }
    }
    state.PauseTiming();

    suite.TearDown();
}

static void bench_func(benchmark::State& state) {
    IegPerf perf;
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    // num_chunks
    for (int i = 0; i <= 1; i++) {
        b->Args({i, i});
    }
}

// Full sort
BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks::vectorized

BENCHMARK_MAIN();
