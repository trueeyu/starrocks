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

#include "runtime/descriptors.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors_ext.h"

namespace starrocks {

class HiveTableDescriptorTest : public ::testing::Test {
public:
    void SetUp() override {
        _query_pool = std::make_unique<ObjectPool>();

        TTableDescriptor tdesc;
        tdesc.id = 1;
        tdesc.tableType = TTableType::HDFS_TABLE;
        _table_desc = _query_pool->add(new HdfsTableDescriptor(tdesc, _query_pool.get()));
    }

    static THdfsPartition _make_partition_value() {
        THdfsPartition p;
        p.file_format = THdfsFileFormat::TEXT;
        p.location.suffix = "";
        return p;
    }

protected:
    std::unique_ptr<ObjectPool> _query_pool;
    HdfsTableDescriptor* _table_desc = nullptr;
};

// Regression test for a heap-use-after-free in HiveTableDescriptor::add_partition_value.
//
// _partition_id_to_desc_map is shared across all fragment instances of a query. The
// previous implementation allocated HdfsPartitionDescriptor from a per-fragment
// ObjectPool, so when fragment A finished and tore down its pool, the map kept a
// dangling pointer. A sibling fragment then hit UAF on the duplicate-check
// comparison `partition->thrift_partition_key_exprs() != old_partition->...`.
//
// The fix allocates the descriptor from the query-level ObjectPool (passed in
// here as `_query_pool`). The descriptor stores only thrift (no opened
// ExprContexts), so there is no fragment-scoped state on the descriptor to
// trip on cross-fragment teardown. This test asserts: two sequential
// "fragments" call add_partition_value with the same partition_id, the second
// hits the dedup path, and after both are gone the entry is still accessible.
TEST_F(HiveTableDescriptorTest, AddPartitionValueLifetimeAcrossFragments) {
    constexpr int64_t partition_id = 42;
    THdfsPartition thrift_partition = _make_partition_value();

    // Fragment instance A: register the partition into the shared query-level pool.
    ASSERT_OK(_table_desc->add_partition_value(_query_pool.get(), partition_id, thrift_partition));

    // Fragment instance B: same partition_id. add_partition_value finds the existing
    // entry, compares thrift_partition_key_exprs against the (still-alive) old entry,
    // and returns OK without inserting a duplicate.
    ASSERT_OK(_table_desc->add_partition_value(_query_pool.get(), partition_id, thrift_partition));

    // The partition descriptor is owned by the query-level pool and must remain
    // accessible regardless of fragment lifetime.
    HdfsPartitionDescriptor* partition = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, partition);
    ASSERT_EQ(thrift_partition.partition_key_exprs, partition->thrift_partition_key_exprs());
}

// Conflicting thrift values for the same partition_id must surface as an error
// rather than silently replace or coexist.
TEST_F(HiveTableDescriptorTest, AddPartitionValueRejectsConflict) {
    constexpr int64_t partition_id = 7;
    THdfsPartition base = _make_partition_value();

    THdfsPartition other = base;
    TExpr expr;
    expr.nodes.emplace_back();
    other.partition_key_exprs.push_back(expr);

    ASSERT_OK(_table_desc->add_partition_value(_query_pool.get(), partition_id, base));
    ASSERT_FALSE(_table_desc->add_partition_value(_query_pool.get(), partition_id, other).ok());
}

} // namespace starrocks