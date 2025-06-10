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

#include "cache/lrucache_engine.h"
#include "util/lru_cache.h"

namespace starrocks {

Status LRUCacheEngine::init(const CacheOptions& options) {
    _lru_cache = std::make_shared<ShardedLRUCache>(options.mem_space_size);
    return Status::OK();
}

Status LRUCacheEngine::write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) {
    return Status::InternalError("LRUCache engine don't support interface for write block");
}

Status LRUCacheEngine::read(const std::string& key, size_t off, size_t size, IOBuffer* buffer, ReadCacheOptions* options) {
    return Status::InternalError("LRUCache engine don't support interface for read block");
}

bool LRUCacheEngine::exist(const std::string& key) const {
    //TODO
    return false;
}

Status LRUCacheEngine::remove(const std::string& key) {
    //TODO
    return Status::OK();
}

Status LRUCacheEngine::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    //TODO
    return Status::OK();
}

Status LRUCacheEngine::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    return Status::InternalError("LRUCache engine don't support manage disk space");
}

const DataCacheMetrics LRUCacheEngine::cache_metrics(int level) const {
    //TODO
    DataCacheMetrics metrics;
    return metrics;
}

}