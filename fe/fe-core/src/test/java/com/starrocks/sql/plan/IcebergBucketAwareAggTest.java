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

import com.starrocks.common.FeConstants;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the bucket-aware-execution-on-lake correctness fix.
 *
 * <p>An Iceberg table bucketed on {@code id} reports its scan output as LOCAL-distributed on {@code id}
 * (see {@code OutputPropertyDeriver#visitPhysicalIcebergScan}). The optimizer used to trust that for
 * aggregations too and elide the shuffle, producing a one-phase local-distinct + gather-sum plan. But lake
 * execution does not pin a bucket to a single pipeline driver (the {@code && isNative} gate in
 * {@code LocalFragmentAssignmentStrategy}), so with pipeline_dop > 1 a bucket's rows are spread across
 * drivers and {@code count(distinct id)} over-counts.
 *
 * <p>The fix only trusts the bucket-aware LOCAL distribution for join-sourced requirements (SHUFFLE_JOIN),
 * not aggregation-sourced ones (SHUFFLE_AGG). Consequences asserted here:
 * <ul>
 *   <li>an aggregation on the bucket column plans identically whether bucket-aware is on or off
 *       (i.e. it always redistributes by the key -- the over-count is gone);</li>
 *   <li>a join on the bucket column still benefits from bucket-aware (its plan changes when the
 *       feature is toggled), proving the fix is targeted and did not disable joins.</li>
 * </ul>
 * Plans are compared by equality rather than by matching a hard-coded shuffle marker, so the test is
 * robust to column-ref numbering and to the cost model's stage choice on the small mock table.
 */
public class IcebergBucketAwareAggTest extends ConnectorPlanTestBase {
    private static final String BUCKET_TABLE = "iceberg0.partitioned_transforms_db.t0_bucket_id";

    @BeforeAll
    public static void beforeClass() throws Exception {
        // Only mock the Iceberg catalog (doInit mocks all connectors, e.g. Kudu, which is unrelated here).
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @AfterAll
    public static void resetVar() {
        connectContext.getSessionVariable().setEnableBucketAwareExecutionOnLake(true);
    }

    private String planWithBucketAware(String sql, boolean enable) throws Exception {
        connectContext.getSessionVariable().setEnableBucketAwareExecutionOnLake(enable);
        return getFragmentPlan(sql);
    }

    @Test
    public void testCountDistinctOnBucketColumnUnaffectedByBucketAware() throws Exception {
        String sql = "select count(distinct id) from " + BUCKET_TABLE;
        // Before the fix these differed (bucket-aware elided the entity-key shuffle); after the fix the
        // aggregation always redistributes, so the plan is identical with the feature on or off.
        Assertions.assertEquals(planWithBucketAware(sql, false), planWithBucketAware(sql, true));
    }

    @Test
    public void testGroupByBucketColumnUnaffectedByBucketAware() throws Exception {
        String sql = "select count(*) from " + BUCKET_TABLE + " group by id";
        Assertions.assertEquals(planWithBucketAware(sql, false), planWithBucketAware(sql, true));
    }

    @Test
    public void testJoinOnBucketColumnStillUsesBucketAware() throws Exception {
        // The fix only gates aggregation-sourced requirements; join-sourced ones keep bucket-aware, so
        // toggling the feature still changes a join plan (colocate/bucket-shuffle vs a normal shuffle).
        String sql = "select count(*) from " + BUCKET_TABLE + " a join " + BUCKET_TABLE + " b on a.id = b.id";
        Assertions.assertNotEquals(planWithBucketAware(sql, false), planWithBucketAware(sql, true));
    }
}