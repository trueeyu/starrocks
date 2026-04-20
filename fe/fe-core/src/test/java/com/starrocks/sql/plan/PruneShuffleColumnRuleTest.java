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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PruneShuffleColumnRuleTest extends PlanTestBase {

    /**
     * Regression test for the bug where PruneShuffleColumnRule prunes Exchange nodes
     * but leaves the intermediate join's outputProperty stale, causing a column-count
     * mismatch between left and right required distributions at the outer join node.
     *
     * Plan structure:
     *   outer JOIN[shuffle] (node22)
     *   ├── inner JOIN[shuffle] (node14)  ← outputProperty is an independent copy
     *   │   ├── Exchange_t0: SHUFFLE(v1, v2)
     *   │   └── Exchange_t1: SHUFFLE(v4, v5)
     *   └── Exchange_t2: SHUFFLE(v7, v8)
     *
     * v1/v4/v7 have NDV=500K on 1M rows (ratio=0.5 > 0.1 threshold, NDV > 200K threshold)
     * → PruneShuffleColumnRule prunes all three exchanges to 1 column.
     *
     * Bug (before fix):
     *   Exchange_t0.desc  = [v1]   (mutated in-place)
     *   Exchange_t1.desc  = [v4]   (mutated in-place)
     *   Exchange_t2.desc  = [v7]   (mutated in-place)
     *   node14.outputProperty.desc = [v1, v2]  ← NOT updated (independent copy!)
     *   node22.requiredProperties[0] → node14.outputProperty → size=2
     *   node22.requiredProperties[1] → Exchange_t2.spec      → size=1
     *   → size mismatch causes incorrect probePartitionByExprs in PlanFragmentBuilder
     *
     * Fix (Option D in PruneShuffleColumnRule):
     *   Track intermediate join OptExpressions in DistributionContext.joinOptExprs.
     *   After pruning Exchange nodes, replace outputProperty on each tracked join
     *   with a new single-column DistributionProperty.
     *   → node14.outputProperty.desc = [v1], sizes match.
     */
    @Test
    public void testNestedShuffleJoinOutputPropertyConsistencyAfterPrune() throws Exception {
        final long rowCount = 1_000_000L;

        GlobalStateMgr mgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) mgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) mgr.getLocalMetastore().getDb("test").getTable("t1");
        OlapTable t2 = (OlapTable) mgr.getLocalMetastore().getDb("test").getTable("t2");

        setTableStatistics(t0, rowCount);
        setTableStatistics(t1, rowCount);
        setTableStatistics(t2, rowCount);

        StatisticStorage original = mgr.getStatisticStorage();
        // v1/v4/v7 (index-0 join columns): NDV=500K → ratio=0.5 > threshold 0.1, NDV > 200K limit
        // v2/v5/v8 (index-1 join columns): NDV=1K   → ratio=0.001 < threshold, will not be chosen
        // PruneShuffleColumnRule will select maxColumnIndex=0 and prune all exchanges to 1 col.
        mgr.setStatisticStorage(new EmptyStatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                boolean highCard = (table.getName().equals("t0") && column.equals("v1"))
                        || (table.getName().equals("t1") && column.equals("v4"))
                        || (table.getName().equals("t2") && column.equals("v7"));
                return ColumnStatistic.builder()
                        .setDistinctValuesCount(highCard ? 500_000.0 : 1_000.0)
                        .setAverageRowSize(8)
                        .build();
            }
        });

        try {
            // runningUnitTest=true disables ReorderJoinRule, so the join tree is left-deep:
            //   (t0 ⋈ t1) ⋈ t2, exactly the structure needed to trigger the bug.
            String sql = "SELECT * FROM t0"
                    + " JOIN[shuffle] t1 ON t0.v1 = t1.v4 AND t0.v2 = t1.v5"
                    + " JOIN[shuffle] t2 ON t0.v1 = t2.v7 AND t0.v2 = t2.v8";

            OptExpression root = UtFrameUtils.getPlanAndFragment(connectContext, sql)
                    .second.getPhysicalPlan();

            // After PruneShuffleColumnRule, every hash join's left and right distribution
            // must have the same column count.  Before the fix the outer join violates this
            // (left=2, right=1); after the fix both are 1.
            assertDistributionSizesConsistent(root);
        } finally {
            mgr.setStatisticStorage(original);
        }
    }

    private void assertDistributionSizesConsistent(OptExpression node) {
        if (node.getOp().getOpType() == OperatorType.PHYSICAL_HASH_JOIN) {
            var leftSpec = node.getRequiredProperties().get(0).getDistributionProperty().getSpec();
            var rightSpec = node.getRequiredProperties().get(1).getDistributionProperty().getSpec();
            if (leftSpec instanceof HashDistributionSpec leftHash
                    && rightSpec instanceof HashDistributionSpec rightHash) {
                int leftSize = leftHash.getHashDistributionDesc().getDistributionCols().size();
                int rightSize = rightHash.getHashDistributionDesc().getDistributionCols().size();
                assertEquals(leftSize, rightSize,
                        "PruneShuffleColumnRule left/right distribution col size mismatch: "
                                + "left=" + leftSize + " right=" + rightSize);
            }
        }
        for (OptExpression child : node.getInputs()) {
            assertDistributionSizesConsistent(child);
        }
    }
}