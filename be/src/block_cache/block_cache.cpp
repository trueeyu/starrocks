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

#include "block_cache/block_cache.h"

#include <fmt/format.h>

#include "block_cache/starcache_wrapper.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "util/starrocks_metrics.h"
#include "runtime/exec_env.h"

namespace starrocks {

namespace fs = std::filesystem;

// For starcache, in theory we doesn't have a hard limitation for block size, but a very large
// block_size may cause heavy read amplification. So, we also limit it to 2 MB as an empirical value.
const size_t BlockCache::MAX_BLOCK_SIZE = 2 * 1024 * 1024;

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

BlockCache::~BlockCache() {
    (void)shutdown();
}

METRIC_DEFINE_UINT_GAUGE(lxh_datacache_lookup_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_write_success_count, MetricUnit::OPERATIONS);

METRIC_DEFINE_UINT_GAUGE(lxh_datacache_base_quota, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_base_usage, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_base_hit_count, MetricUnit::OPERATIONS);

METRIC_DEFINE_UINT_GAUGE(lxh_datacache_extent_quota, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_extent_usage, MetricUnit::BYTES);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_extent_write_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_extent_cost, MetricUnit::OPERATIONS);

METRIC_DEFINE_UINT_GAUGE(lxh_datacache_mem_meta, MetricUnit::BYTES);

METRIC_DEFINE_UINT_GAUGE(lxh_datacache_miss_time, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_datacache_io_time, MetricUnit::OPERATIONS);


METRIC_DEFINE_UINT_GAUGE(lxh_interface_write_buffer_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_interface_write_object_count, MetricUnit::OPERATIONS);

METRIC_DEFINE_UINT_GAUGE(lxh_interface_read_buffer_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(lxh_interface_read_object_count, MetricUnit::OPERATIONS);

Status BlockCache::init(const CacheOptions& options) {
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
    auto cache_options = options;
    if (cache_options.engine == "starcache") {
        _kv_cache = std::make_unique<StarCacheWrapper>();
        _disk_space_monitor = std::make_unique<DiskSpaceMonitor>(this);
        _disk_space_monitor->adjust_spaces(&cache_options.disk_spaces);
        LOG(INFO) << "init starcache engine, block_size: " << _block_size;
    }
    if (!_kv_cache) {
        LOG(ERROR) << "unsupported block cache engine: " << cache_options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
    RETURN_IF_ERROR(_kv_cache->init(cache_options));
    _refresh_quota();
    _initialized.store(true, std::memory_order_relaxed);
    if (_disk_space_monitor) {
        _disk_space_monitor->start();
    }

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_lookup_count", &lxh_datacache_lookup_count);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_write_success_count",
                                                             &lxh_datacache_write_success_count);

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_base_quota", &lxh_datacache_base_quota);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_base_usage", &lxh_datacache_base_usage);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_base_hit_count", &lxh_datacache_base_hit_count);

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_extent_quota", &lxh_datacache_extent_quota);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_extent_usage", &lxh_datacache_extent_usage);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_extent_write_count", &lxh_datacache_extent_write_count);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_extent_cost", &lxh_datacache_extent_cost);

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_mem_meta", &lxh_datacache_mem_meta);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_miss_time", &lxh_datacache_miss_time);
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_datacache_io_time", &lxh_datacache_io_time);

    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_lookup_count", [this]() {
      DataCacheMetrics datacache_metrics = cache_metrics(1);
      lxh_datacache_lookup_count.set_value(datacache_metrics.detail_l1->hit_count + datacache_metrics.detail_l1->miss_count);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_write_success_count", [this]() {
      DataCacheMetrics datacache_metrics = cache_metrics(2);
      lxh_datacache_write_success_count.set_value(datacache_metrics.detail_l2->write_success_count);
    });

    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_base_quota", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(0);
        lxh_datacache_base_quota.set_value(datacache_metrics.mem_quota_bytes);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_base_usage", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(0);
        lxh_datacache_base_usage.set_value(datacache_metrics.mem_used_bytes);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_base_hit_count", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(1);
        lxh_datacache_base_hit_count.set_value(datacache_metrics.detail_l1->hit_count);
    });

    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_extent_quota", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(0);
        lxh_datacache_extent_quota.set_value(datacache_metrics.mem_extent_quota_bytes);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_extent_usage", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(0);
        lxh_datacache_extent_usage.set_value(datacache_metrics.mem_extent_used_bytes);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_extent_write_count", [this]() {
        DataCacheMetrics datacache_metrics = cache_metrics(0);
        lxh_datacache_extent_write_count.set_value(datacache_metrics.extent_write_count);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_extent_cost", [this]() {
      DataCacheMetrics datacache_metrics = cache_metrics(0);
      lxh_datacache_extent_cost.set_value(datacache_metrics.extent_cost);
    });

    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_mem_meta", [this]() {
      DataCacheMetrics datacache_metrics = cache_metrics(0);
      lxh_datacache_mem_meta.set_value(datacache_metrics.meta_used_bytes);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_miss_time", [this]() {
        lxh_datacache_miss_time.set_value(GlobalEnv::GetInstance()->_total_block_cache_miss_time);
    });
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_datacache_io_time", [this]() {
        lxh_datacache_io_time.set_value(GlobalEnv::GetInstance()->_total_data_cache_io_time);
    });

    StarRocksMetrics::instance()->metrics()->register_metric("lxh_interface_writer_buffer_count",
                                                             &lxh_interface_write_buffer_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_interface_write_buffer_count", [this]() {
        lxh_interface_write_buffer_count.set_value(_lxh_interface_write_buffer_count);
    });
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_interface_read_buffer_count",
                                                             &lxh_interface_read_buffer_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_interface_read_buffer_count", [this]() {
        lxh_interface_read_buffer_count.set_value(_lxh_interface_read_buffer_count);
    });
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_interface_write_object_count",
                                                             &lxh_interface_write_object_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_interface_write_object_count", [this]() {
        lxh_interface_write_object_count.set_value(_lxh_interface_write_object_count);
    });
    StarRocksMetrics::instance()->metrics()->register_metric("lxh_interface_read_object_count",
                                                             &lxh_interface_read_object_count);
    StarRocksMetrics::instance()->metrics()->register_hook("lxh_interface_read_object_count", [this]() {
        lxh_interface_read_object_count.set_value(_lxh_interface_read_object_count);
    });

    return Status::OK();
}

Status BlockCache::write_buffer(const CacheKey& cache_key, off_t offset, const IOBuffer& buffer,
                                WriteCacheOptions* options) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "write block key: " << cache_key << " with invalid args, offset: " << offset;
        return Status::InvalidArgument(strings::Substitute("offset must be aligned by block size $0", _block_size));
    }
    if (buffer.empty()) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    _lxh_interface_write_buffer_count++;
    VLOG(3) << "WRITE_BUFFER_SIZE: " << crc_hash_64(block_key.data(), block_key.size(), CRC_HASH_SEED1)
            << ":" << buffer.size() << ":" << block_key;
    return _kv_cache->write_buffer(block_key, buffer, options);
}

static void empty_deleter(void*) {}

Status BlockCache::write_buffer(const CacheKey& cache_key, off_t offset, size_t size, const char* data,
                                WriteCacheOptions* options) {
    if (!data) {
        return Status::InvalidArgument("invalid data buffer");
    }

    IOBuffer buffer;
    buffer.append_user_data((void*)data, size, empty_deleter);
    return write_buffer(cache_key, offset, buffer, options);
}

Status BlockCache::write_object(const CacheKey& cache_key, const void* ptr, size_t size, DeleterFunc deleter,
                                DataCacheHandle* handle, WriteCacheOptions* options) {
    if (!ptr) {
        return Status::InvalidArgument("invalid object pointer");
    }
    _lxh_interface_write_object_count++;
    return _kv_cache->write_object(cache_key, ptr, size, std::move(deleter), handle, options);
}

Status BlockCache::read_buffer(const CacheKey& cache_key, off_t offset, size_t size, IOBuffer* buffer,
                               ReadCacheOptions* options) {
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    _lxh_interface_read_buffer_count++;
    VLOG(3) << "READ_BUFFER_SIZE: " << crc_hash_64(block_key.data(), block_key.size(), CRC_HASH_SEED1)
        << ":" << size << ":" << block_key;
    return _kv_cache->read_buffer(block_key, offset - index * _block_size, size, buffer, options);
}

StatusOr<size_t> BlockCache::read_buffer(const CacheKey& cache_key, off_t offset, size_t size, char* data,
                                         ReadCacheOptions* options) {
    IOBuffer buffer;
    _lxh_interface_read_buffer_count++;
    RETURN_IF_ERROR(read_buffer(cache_key, offset, size, &buffer, options));
    buffer.copy_to(data);
    return buffer.size();
}

Status BlockCache::read_object(const CacheKey& cache_key, DataCacheHandle* handle, ReadCacheOptions* options) {
    _lxh_interface_read_object_count++;
    return _kv_cache->read_object(cache_key, handle, options);
}

bool BlockCache::exist(const starcache::CacheKey& cache_key, off_t offset, size_t size) const {
    if (size == 0) {
        return true;
    }
    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->exist(block_key);
}

Status BlockCache::remove(const CacheKey& cache_key, off_t offset, size_t size) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "remove block key: " << cache_key << " with invalid args, offset: " << offset
                     << ", size: " << size;
        return Status::InvalidArgument(
                strings::Substitute("offset and size must be aligned by block size $0", _block_size));
    }
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->remove(block_key);
}

Status BlockCache::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    Status st = _kv_cache->update_mem_quota(quota_bytes, flush_to_disk);
    _refresh_quota();
    return st;
}

Status BlockCache::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    Status st = _kv_cache->update_disk_spaces(spaces);
    _refresh_quota();
    return st;
}

Status BlockCache::adjust_disk_spaces(const std::vector<DirSpace>& spaces) {
    if (_disk_space_monitor) {
        auto adjusted_spaces = spaces;
        _disk_space_monitor->adjust_spaces(&adjusted_spaces);
        return _disk_space_monitor->adjust_cache_quota(adjusted_spaces);
    }
    return update_disk_spaces(spaces);
}

void BlockCache::record_read_remote(size_t size, int64_t lateny_us) {
    _kv_cache->record_read_remote(size, lateny_us);
}

void BlockCache::record_read_cache(size_t size, int64_t lateny_us) {
    _kv_cache->record_read_cache(size, lateny_us);
}

const DataCacheMetrics BlockCache::cache_metrics(int level) const {
    return _kv_cache->cache_metrics(level);
}

Status BlockCache::shutdown() {
    if (!_initialized.load(std::memory_order_relaxed)) {
        return Status::OK();
    }
    Status st = _kv_cache->shutdown();
    if (_disk_space_monitor) {
        _disk_space_monitor->stop();
    }
    _initialized.store(false, std::memory_order_relaxed);
    return st;
}

DataCacheEngineType BlockCache::engine_type() {
    return _kv_cache->engine_type();
}

void BlockCache::_refresh_quota() {
    DataCacheMetrics metrics = _kv_cache->cache_metrics(0);
    LOG(ERROR) << "Metrics_1: " << metrics.mem_quota_bytes;
    LOG(ERROR) << "Metrics_2: " << metrics.disk_quota_bytes;
    _mem_quota.store(metrics.mem_quota_bytes, std::memory_order_relaxed);
    _disk_quota.store(metrics.disk_quota_bytes, std::memory_order_relaxed);
}

} // namespace starrocks
