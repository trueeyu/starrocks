// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <benchmark/benchmark.h>
#include <testutil/assert.h>

#include <memory>

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
    void SetUp() {}
    void TearDown() {}

    IegPerf(int type) : _type(type) {}

    void init_types();
    void init_src_chunks();
    void init_dest_chunks();
    ChunkPtr init_dest_chunk();
    ColumnPtr init_src_column(const TypeDescriptor& type);
    ColumnPtr init_dest_column(const TypeDescriptor& type);
    void do_hash(const ColumnPtr& col);
    void do_shuffle(const Chunk& src_chunk, Chunk& dest_chunk, int be_idx);
    void do_bench(benchmark::State& state);

private:
    int _type = 0;
    int _column_count = 400;
    int _chunk_count = 400;
    int _node_count = 140;
    int _chunk_size = 4096;
    std::vector<TypeDescriptor> _types;
    std::vector<ChunkPtr> _src_chunks;
    std::vector<ChunkPtr> _dest_chunks;
    std::vector<uint32_t> _shuffle_idxs;
    std::vector<uint32_t> _select_idxs;
};

void IegPerf::init_types() {
    _types.resize(_column_count);
    if (_type == 0) {
        for (int i = 0; i < _column_count; i++) {
            _types[i] = TypeDescriptor(TYPE_INT);
        }
    } else {
        for (int i = 0; i < _column_count; i++) {
            _types[i] = TypeDescriptor::create_varchar_type(1024);
        }
    }
}

ColumnPtr IegPerf::init_src_column(const TypeDescriptor& type) {
    if (type.is_string_type()) {
        auto c1 = ColumnHelper::create_column(type, true);
        c1->reserve(_chunk_size);
        auto* nullable_col = down_cast<NullableColumn*>(c1.get());
        auto* int_col = down_cast<BinaryColumn*>(nullable_col->data_column().get());
        for (int k = 0; k < _chunk_size; k++) {
            int_col->append_string("str123" + std::to_string(rand()));
        }
        return c1;
    } else {
        auto c1 = ColumnHelper::create_column(type, true);
        c1->resize(_chunk_size);
        auto* nullable_col = down_cast<NullableColumn*>(c1.get());
        auto* int_col = down_cast<Int32Column*>(nullable_col->data_column().get());
        for (int k = 0; k < _chunk_size; k++) {
            int_col->get_data()[k] = rand();
        }
        return c1;
    }
}

ColumnPtr IegPerf::init_dest_column(const TypeDescriptor& type) {
    auto c1 = ColumnHelper::create_column(type, true);
    if (c1->is_binary()) {
        //auto* nullable_column = down_cast<NullableColumn*>(c1.get());
        //auto* binary_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
        c1->reserve(_chunk_size);
        //binary_column->reserve(_chunk_size, 30 * 4096);
    } else {
        c1->reserve(_chunk_size);
    }
    return c1;
}

void IegPerf::init_src_chunks() {
    for (int i = 0; i < _chunk_count; i++) {
        _src_chunks.emplace_back(std::make_unique<Chunk>());
    }

    for (int i = 0; i < _chunk_count; i++) {
        for (int j = 0; j < _column_count; j++) {
            auto col = init_src_column(_types[j]);
            _src_chunks[i]->append_column(col, j);
        }
    }
}

void IegPerf::init_dest_chunks() {
    _dest_chunks.resize(_node_count);
    for (int i = 0; i < _node_count; i++) {
        _dest_chunks[i] = nullptr;
    }
}

ChunkPtr IegPerf::init_dest_chunk() {
    auto chunk = std::make_unique<Chunk>();
    for (int i = 0; i < _column_count; i++) {
        auto col = init_dest_column(_types[i]);
        chunk->append_column(col, i);
    }
    return chunk;
}

void IegPerf::do_hash(const ColumnPtr& col) {
    _shuffle_idxs.resize(_chunk_size);

    col->crc32_hash(&_shuffle_idxs[0], 0, _chunk_size);
    for (int i = 0; i < _chunk_size; i++) {
        _shuffle_idxs[i] = _shuffle_idxs[i] % _node_count;
    }
}

void IegPerf::do_shuffle(const Chunk& src_chunk, Chunk& dest_chunk, int be_idx) {
    for (int i = 0; i < _shuffle_idxs.size(); i++) {
        if (_shuffle_idxs[i] == be_idx) {
            _select_idxs.emplace_back(i);
        }
    }
    dest_chunk.append_selective(src_chunk, _select_idxs.data(), 0, _select_idxs.size());
}

void IegPerf::do_bench(benchmark::State& state) {
    init_types();
    init_src_chunks();
    init_dest_chunks();

    state.ResumeTiming();

    for (int i = 0; i < _chunk_count; i++) {
        do_hash(_src_chunks[i]->columns()[0]);
        for (int j = 0; j < _node_count; j++) {
            _select_idxs.resize(0);
            if (_dest_chunks[j] == nullptr) {
                _dest_chunks[j] = init_dest_chunk();
            }
            do_shuffle(*_src_chunks[i], *_dest_chunks[j], j);
            if (_dest_chunks[j]->num_rows() >= _chunk_size) {
                _dest_chunks[j].reset();
            }
        }
    }
    state.PauseTiming();
}

static void bench_func(benchmark::State& state) {
    auto arg1 = state.range(0);

    IegPerf perf(arg1);
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    b->Args({0, 0});
    b->Args({1, 1});
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks::vectorized

BENCHMARK_MAIN();
