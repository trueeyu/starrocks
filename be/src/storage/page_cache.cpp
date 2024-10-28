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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/page_cache.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/page_cache.h"

#include <malloc.h>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/lru_cache.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_lookup_count, MetricUnit::OPERATIONS);

METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_base_hit_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_base_capacity, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_base_usage, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_extent_usage, MetricUnit::BYTES);

METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_extent_hit_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_extent_write_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_extent_cost, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_extent_capacity, MetricUnit::BYTES);

METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_miss_cost, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_page_cache_total_scan_time, MetricUnit::OPERATIONS);

StoragePageCache* StoragePageCache::_s_instance = nullptr;

void StoragePageCache::create_global_cache(MemTracker* mem_tracker, size_t base_capacity, size_t extent_capacity) {
    if (_s_instance == nullptr) {
        _s_instance = new StoragePageCache(mem_tracker, base_capacity, extent_capacity);
    }
}

void StoragePageCache::release_global_cache() {
    if (_s_instance != nullptr) {
        delete _s_instance;
        _s_instance = nullptr;
    }
}

void StoragePageCache::prune() {
    _cache->prune();
}

static void init_metrics() {
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_lookup_count",
                                                             &lxh_page_cache_lookup_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_lookup_count", []() {
        lxh_page_cache_lookup_count.set_value(StoragePageCache::instance()->get_lookup_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_base_hit_count",
                                                             &lxh_page_cache_base_hit_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_base_hit_count", []() {
        lxh_page_cache_base_hit_count.set_value(StoragePageCache::instance()->get_base_hit_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_extent_hit_count",
                                                             &lxh_page_cache_extent_hit_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_extent_hit_count", []() {
        lxh_page_cache_extent_hit_count.set_value(StoragePageCache::instance()->get_extent_hit_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_extent_write_count",
                                                             &lxh_page_cache_extent_write_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_extent_write_count", []() {
        lxh_page_cache_extent_write_count.set_value(StoragePageCache::instance()->get_extent_write_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_extent_cost",
                                                             &lxh_page_cache_extent_cost);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_extent_cost", []() {
        lxh_page_cache_extent_cost.set_value(StoragePageCache::instance()->get_extent_cost());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_base_capacity",
                                                             &lxh_page_cache_base_capacity);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_base_capacity", []() {
        lxh_page_cache_base_capacity.set_value(StoragePageCache::instance()->get_base_capacity());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_extent_capacity",
                                                             &lxh_page_cache_extent_capacity);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_extent_capacity", []() {
        lxh_page_cache_extent_capacity.set_value(StoragePageCache::instance()->get_extent_capacity());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_base_usage",
                                                             &lxh_page_cache_base_usage);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_base_usage", []() {
        lxh_page_cache_base_usage.set_value(StoragePageCache::instance()->get_base_usage());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_extent_usage",
                                                             &lxh_page_cache_extent_usage);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_extent_usage", []() {
        lxh_page_cache_extent_usage.set_value(StoragePageCache::instance()->get_extent_usage());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_miss_cost",
                                                             &lxh_page_cache_miss_cost);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_miss_cost", []() {
        lxh_page_cache_miss_cost.set_value(GlobalEnv::GetInstance()->_total_page_cache_miss_time);
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_page_cache_total_scan_time",
                                                             &lxh_page_cache_total_scan_time);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_page_cache_total_scan_time", []() {
        lxh_page_cache_total_scan_time.set_value(GlobalEnv::GetInstance()->_total_page_cache_io_time);
    });
}

StoragePageCache::StoragePageCache(MemTracker* mem_tracker, size_t base_capacity, size_t extent_capacity)
        : _mem_tracker(mem_tracker), _cache(new_lru_cache(base_capacity, extent_capacity, ChargeMode::MEMSIZE)) {
    init_metrics();
}

StoragePageCache::~StoragePageCache() = default;

void StoragePageCache::set_capacity(size_t base_capacity, size_t extent_capacity) {
#ifndef BE_TEST
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
#endif
    _cache->set_capacity(base_capacity, extent_capacity);
}

size_t StoragePageCache::get_base_capacity() {
    return _cache->get_base_capacity();
}

size_t StoragePageCache::get_extent_capacity() {
    return _cache->get_extent_capacity();
}

uint64_t StoragePageCache::get_lookup_count() {
    return _cache->get_lookup_count();
}

uint64_t StoragePageCache::get_base_hit_count() {
    return _cache->get_base_hit_count();
}

uint64_t StoragePageCache::get_extent_hit_count() {
    return _cache->get_extent_hit_count();
}

uint64_t StoragePageCache::get_extent_write_count() {
    return _cache->get_extent_write_count();
}

uint64_t StoragePageCache::get_extent_cost() {
    return _cache->get_extent_cost();
}

bool StoragePageCache::adjust_capacity(int64_t delta) {
#ifndef BE_TEST
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
#endif
    return _cache->adjust_capacity(delta);
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle) {
    VLOG(3) << "LXH: lookup key: " << key.fname << ":" << key.offset;
    auto* lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(_cache.get(), lru_handle);
    return true;
}

void StoragePageCache::insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle, bool in_memory,
                              size_t cost) {
    VLOG(3) << "LXH: insert key: " << key.fname << ":" << key.offset << ":size(" << data.size
            << "):cost(" << cost << ")";
    // mem size should equals to data size when running UT
    int64_t mem_size = data.size;
#ifndef BE_TEST
    mem_size = malloc_usable_size(data.data);
    tls_thread_status.mem_release(mem_size);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
    tls_thread_status.mem_consume(mem_size);
#endif

    auto deleter = [](const starrocks::CacheKey& key, void* value) {
        if (value != nullptr) {
            delete[] (uint8_t*)value;
        }
    };

    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }
    // Use mem size managed by memory allocator as this record charge size. At the same time, we should record this record size
    // for data fetching when lookup.
    auto* lru_handle = _cache->insert(key.encode(), data.data, mem_size, deleter, priority, data.size, cost);
    *handle = PageCacheHandle(_cache.get(), lru_handle);
}

} // namespace starrocks
