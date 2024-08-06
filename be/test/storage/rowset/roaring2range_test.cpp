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

#include "storage/rowset/bitmap_range_iterator.h"

#include "gtest/gtest.h"
#include "storage/range.h"
#include "storage/roaring2range.h"

namespace starrocks {

using Roaring = roaring::Roaring;

class Roaring2RangeTest : public ::testing::Test {};

TEST_F(Roaring2RangeTest, roaring2range) {
    Roaring roaring_1;
    auto r_1 = roaring2range(roaring_1);
    ASSERT_EQ(r_1.to_string(), "()");

    Roaring roaring_2;
    roaring_2.addRange(5, 100);
}

}