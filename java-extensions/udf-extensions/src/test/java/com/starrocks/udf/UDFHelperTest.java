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

package com.starrocks.udf;

import org.junit.jupiter.api.Test;

import java.sql.Time;

import static com.starrocks.utils.NativeMethodHelper.getAddrs;

public class UDFHelperTest {
    @Test
    public void testGetResultFromBoxedArray() {
        java.sql.Time[] values = new java.sql.Time[3];
        values[0] = Time.valueOf("09:00:00");
        values[1] = Time.valueOf("14:30:00");
        values[2] = Time.valueOf("20:00:00");
        final long[] addrs = getAddrs(0);
        //long keyAddr = addrs[2];
        long valueAddr = addrs[3];
        UDFHelper.getResultFromBoxedArray(UDFHelper.TYPE_TIME, 3, (Object)values, valueAddr);
    }
}
