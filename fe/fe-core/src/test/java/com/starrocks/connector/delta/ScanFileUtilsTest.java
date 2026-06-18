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

package com.starrocks.connector.delta;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ScanFileUtilsTest {
    private static final String ROOT = "oss://starrocks-ci-test/dla_test_data/delta/delta_lake_par_col_timestamp";

    // delta-kernel >= 4.2.0 URL-decodes the path in getAddFileStatus(); the helper must splice the
    // raw encoded add.path back so the scan range matches the physical (percent-encoded) object key.
    @Test
    public void testRestoreEncodedTimestampPartitionPath() {
        String decoded = ROOT + "/col_timestamp=2023-01-01 01:01:01/part-0.parquet";
        String rawEncoded = "col_timestamp=2023-01-01%2001%3A01%3A01/part-0.parquet";
        assertEquals(ROOT + "/col_timestamp=2023-01-01%2001%3A01%3A01/part-0.parquet",
                ScanFileUtils.restoreEncodedFilePath(decoded, rawEncoded));
    }

    @Test
    public void testNoSpecialCharsUnchanged() {
        String full = ROOT + "/col_date=2024-04-24/part-1.parquet";
        assertEquals(full, ScanFileUtils.restoreEncodedFilePath(full, "col_date=2024-04-24/part-1.parquet"));
    }

    @Test
    public void testEncodedSlashInsideSegmentPreserved() {
        // decoded form gains an extra '/' from %2F, so naive segment counting would be wrong
        String decoded = ROOT + "/k=a/b/v=x y/part-2.parquet";
        String rawEncoded = "k=a%2Fb/v=x%20y/part-2.parquet";
        assertEquals(ROOT + "/k=a%2Fb/v=x%20y/part-2.parquet",
                ScanFileUtils.restoreEncodedFilePath(decoded, rawEncoded));
    }

    @Test
    public void testNonPartitionedFile() {
        String full = ROOT + "/part-3.parquet";
        assertEquals(full, ScanFileUtils.restoreEncodedFilePath(full, "part-3.parquet"));
    }

    @Test
    public void testAbsoluteAddPathReturnedAsIs() {
        // delta shallow clone stores an absolute add.path; the kernel uses it directly
        assertEquals("s3://other/x%20y/f.parquet",
                ScanFileUtils.restoreEncodedFilePath("s3://other/x y/f.parquet", "s3://other/x%20y/f.parquet"));
    }

    @Test
    public void testEmptyRawPathFallsBackToDecoded() {
        String full = ROOT + "/part-4.parquet";
        assertEquals(full, ScanFileUtils.restoreEncodedFilePath(full, ""));
        assertEquals(full, ScanFileUtils.restoreEncodedFilePath(full, null));
    }
}