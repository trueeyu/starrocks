// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/chunk_cursor.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

inline int kTestChunkSize = 4096;

class IegPerf {
public:
    void SetUp() {
        config::vector_chunk_size = 4096;
    }

    void TearDown() { }
};

static void do_bench(benchmark::State& state, int num_chunks, int num_columns) {
    IegPerf suite;
    suite.SetUp();
    int column_count = 100;
    int chunk_count = 400;

    std::vector<TypeDescriptor> types;
    types.resize(column_count);
    for (int i = 0; i < column_count; i++) {
        types[i] = TypeDescriptor(TYPE_INT);
    }

    std::vector<ChunkPtr> chunks;
    for (int i = 0; i < chunk_count; i++) {
        chunks.emplace_back(std::make_unique<Chunk>());
    }

    state.ResumeTiming();
    for (int i = 0; i < chunk_count; i++) {
        for (int j = 0; j < column_count; j++) {
            auto c1 = ColumnHelper::create_column(types[j], true);
            c1->resize(config::vector_chunk_size);
            auto* nullable_col = down_cast<NullableColumn*>(c1.get());
            auto* int_col = down_cast<Int32Column*>(nullable_col->data_column().get());
            for (int k = 0; k < config::vector_chunk_size; k++) {
                int_col->get_data()[k] = rand();
            }
            chunks[i]->append_column(c1, j);
        }
    }
    state.PauseTiming();

    std::vector<ChunkPtr> dest_chunk;

    suite.TearDown();
}

static void bench_func(benchmark::State& state) {
    do_bench(state, state.range(0), state.range(1));
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
