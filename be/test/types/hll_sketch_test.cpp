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

#include "types/hll_sketch.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "util/hash_util.hpp"
#include "util/slice.h"

namespace starrocks {

// Build a serialized HLL sketch (as ds_hll_count_distinct stores in a varbinary
// column) holding `count` distinct hashed values starting from `start`.
// Disjoint [start, start+count) ranges produce disjoint sketches.
static std::string make_serialized_sketch(uint8_t log_k, datasketches::target_hll_type tgt, int64_t start,
                                          int64_t count) {
    int64_t mem = 0;
    DataSketchesHll hll(log_k, tgt, &mem);
    for (int64_t i = start; i < start + count; ++i) {
        uint64_t h = HashUtil::murmur_hash64A(&i, sizeof(i), HashUtil::MURMUR_SEED);
        hll.update(h);
    }
    std::string buf;
    buf.resize(hll.serialize_size());
    size_t n = hll.serialize(reinterpret_cast<uint8_t*>(buf.data()));
    buf.resize(n);
    return buf;
}

// Merge serialized sketches in `order` into one union and return the estimate.
// This mirrors HllSketchAggregateFunction::merge() exactly:
//   deserialize each slice into a temp, lazily create the state union from the
//   first incoming sketch's config, then merge.
static int64_t merge_in_order(const std::vector<std::string>& sketches, const std::vector<size_t>& order) {
    int64_t mem = 0;
    std::unique_ptr<DataSketchesHll> state;
    for (size_t idx : order) {
        Slice s(sketches[idx]);
        DataSketchesHll incoming(s, &mem);
        if (state == nullptr) {
            state = std::make_unique<DataSketchesHll>(incoming.get_lg_config_k(), incoming.get_target_type(), &mem);
        }
        state->merge(incoming);
    }
    return state == nullptr ? 0 : state->estimate_cardinality();
}

// The estimate must depend ONLY on the merged data, never on the order in which
// the per-group sketches are merged. In a distributed plan that merge order is
// non-deterministic across runs, so an order-dependent estimate shows up as a
// jittering, non-reproducible count.
//
// REPRODUCES THE BUG: on the current code this FAILS once the union promotes to
// dense HLL mode, because DataSketches derives the estimate from the
// incrementally-accumulated, order-sensitive floating-point fields kxq0_/kxq1_.
// After routing estimate_cardinality() through a canonical register-order
// recomputation (e.g. get_result(HLL_6)), all orders agree and this PASSES.
TEST(DataSketchesHllTest, MergeOrderMustNotAffectEstimateDenseMode) {
    const uint8_t log_k = 17;
    const auto tgt = datasketches::HLL_6;

    // 32 disjoint sketches x 1000 distinct = 32000 distinct, well past the
    // SET->HLL promotion threshold (~2^(17-3)=16384), so the union is dense.
    const size_t num_sketches = 32;
    const int64_t per_sketch = 1000;

    std::vector<std::string> sketches;
    sketches.reserve(num_sketches);
    for (size_t i = 0; i < num_sketches; ++i) {
        sketches.push_back(make_serialized_sketch(log_k, tgt, static_cast<int64_t>(i) * per_sketch, per_sketch));
    }

    std::vector<size_t> identity(num_sketches);
    std::iota(identity.begin(), identity.end(), 0);

    std::vector<int64_t> estimates;
    // forward
    estimates.push_back(merge_in_order(sketches, identity));
    // reverse
    {
        auto rev = identity;
        std::reverse(rev.begin(), rev.end());
        estimates.push_back(merge_in_order(sketches, rev));
    }
    // deterministic shuffles (fixed seeds => test itself is reproducible)
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        auto perm = identity;
        std::shuffle(perm.begin(), perm.end(), std::mt19937(seed));
        estimates.push_back(merge_in_order(sketches, perm));
    }

    const int64_t mn = *std::min_element(estimates.begin(), estimates.end());
    const int64_t mx = *std::max_element(estimates.begin(), estimates.end());

    EXPECT_EQ(mn, mx) << "ds_hll merge estimate is order-dependent (dense HLL): min=" << mn << " max=" << mx
                      << " jitter=" << (mx - mn)
                      << ". The estimate must be a deterministic function of the merged registers.";
}

// Below the promotion threshold the union stays in exact coupon mode, where the
// estimate is already a deterministic function of the coupon set. This mirrors
// the "single day = 1000, rock stable" observation and should PASS both before
// and after the fix.
TEST(DataSketchesHllTest, MergeOrderStableInCouponMode) {
    const uint8_t log_k = 17;
    const auto tgt = datasketches::HLL_6;

    // 4 disjoint sketches x 1000 = 4000 distinct, stays in coupon (exact) mode.
    const size_t num_sketches = 4;
    const int64_t per_sketch = 1000;

    std::vector<std::string> sketches;
    for (size_t i = 0; i < num_sketches; ++i) {
        sketches.push_back(make_serialized_sketch(log_k, tgt, static_cast<int64_t>(i) * per_sketch, per_sketch));
    }

    std::vector<size_t> identity(num_sketches);
    std::iota(identity.begin(), identity.end(), 0);

    int64_t baseline = merge_in_order(sketches, identity);
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        auto perm = identity;
        std::shuffle(perm.begin(), perm.end(), std::mt19937(seed));
        EXPECT_EQ(baseline, merge_in_order(sketches, perm)) << "coupon-mode estimate should be order-independent";
    }
}

} // namespace starrocks