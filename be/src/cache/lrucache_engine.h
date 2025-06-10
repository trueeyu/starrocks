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

#pragma once

#include "cache/local_cache.h"
#include <atomic>

namespace starrocks {

class SharedLRUCache;

class LRUCacheEngine : public LocalCache {
public:
    LRUCacheEngine() = default;
    ~LRUCacheEngine() override = default;

    Status init(const CacheOptions& options) override;
    bool is_initialized() const override { return _initialized.load(std::memory_order_relaxed); }

    Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) override;
    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer, ReadCacheOptions* options) override;
    bool exist(const std::string& key) const override;
    Status remove(const std::string& key) override;
    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) override;
    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override;
    const DataCacheMetrics cache_metrics(int level) const override;

private:
    std::shared_ptr<SharedLRUCache> _lru_cache;
    std::atomic<bool> _initialized = false;
};
}
