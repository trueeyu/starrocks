// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_server.cpp

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

#include <gperftools/profiler.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <ctime>
#include <memory>
#include <string>
#include <unordered_set>

#include "common/status.h"
#include "storage/compaction.h"
#include "storage/compaction_manager.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "util/gc_helper.h"
#include "util/thread.h"
#include "util/time.h"

using std::string;

namespace starrocks {

// TODO(yingchun): should be more graceful in the future refactor.
#define SLEEP_IN_BG_WORKER(seconds)                                                   \
    int64_t left_seconds = (seconds);                                                 \
    while (!_bg_worker_stopped.load(std::memory_order_consume) && left_seconds > 0) { \
        sleep(1);                                                                     \
        --left_seconds;                                                               \
    }                                                                                 \
    if (_bg_worker_stopped.load(std::memory_order_consume)) {                         \
        break;                                                                        \
    }

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

Status StorageEngine::start_bg_threads() {
    _update_cache_expire_thread = std::thread([this] { _update_cache_expire_thread_callback(nullptr); });
    Thread::set_thread_name(_update_cache_expire_thread, "cache_expire");

    _update_cache_evict_thread = std::thread([this] { _update_cache_evict_thread_callback(nullptr); });
    Thread::set_thread_name(_update_cache_expire_thread, "evict_update_cache");

    _unused_rowset_monitor_thread = std::thread([this] { _unused_rowset_monitor_thread_callback(nullptr); });
    Thread::set_thread_name(_unused_rowset_monitor_thread, "rowset_monitor");

    // start thread for monitoring the tablet with io error
    _disk_stat_monitor_thread = std::thread([this] { _disk_stat_monitor_thread_callback(nullptr); });
    Thread::set_thread_name(_disk_stat_monitor_thread, "disk_monitor");

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }
    const auto data_dir_num = static_cast<int32_t>(data_dirs.size());

    if (!config::enable_event_based_compaction_framework) {
        // base and cumulative compaction threads
        int32_t base_compaction_num_threads_per_disk =
                std::max<int32_t>(1, config::base_compaction_num_threads_per_disk);
        int32_t cumulative_compaction_num_threads_per_disk =
                std::max<int32_t>(1, config::cumulative_compaction_num_threads_per_disk);
        int32_t base_compaction_num_threads = base_compaction_num_threads_per_disk * data_dir_num;
        int32_t cumulative_compaction_num_threads = cumulative_compaction_num_threads_per_disk * data_dir_num;

        // calc the max concurrency of compaction tasks
        int32_t max_compaction_concurrency = config::max_compaction_concurrency;
        if (max_compaction_concurrency < 0 ||
            max_compaction_concurrency > base_compaction_num_threads + cumulative_compaction_num_threads) {
            max_compaction_concurrency = base_compaction_num_threads + cumulative_compaction_num_threads;
        }
        vectorized::Compaction::init(max_compaction_concurrency);

        _base_compaction_threads.reserve(base_compaction_num_threads);
        // The config::tablet_map_shard_size is preferably a multiple of `base_compaction_num_threads_per_disk`,
        // otherwise the compaction thread will be distributed unevenly.
        int32_t base_step = config::tablet_map_shard_size / base_compaction_num_threads_per_disk +
                            (config::tablet_map_shard_size % base_compaction_num_threads_per_disk != 0);
        for (int32_t i = 0; i < base_compaction_num_threads_per_disk; i++) {
            std::pair<int32_t, int32_t> tablet_shards_range;
            if (config::tablet_map_shard_size >= base_compaction_num_threads_per_disk) {
                tablet_shards_range.first = std::min(config::tablet_map_shard_size, base_step * i);
                tablet_shards_range.second = std::min(config::tablet_map_shard_size, base_step * (i + 1));
            } else {
                tablet_shards_range.first = 0;
                tablet_shards_range.second = config::tablet_map_shard_size;
            }
            for (int32_t j = 0; j < data_dir_num; j++) {
                _base_compaction_threads.emplace_back([this, data_dirs, j, tablet_shards_range] {
                    _base_compaction_thread_callback(nullptr, data_dirs[j], tablet_shards_range);
                });
                Thread::set_thread_name(_base_compaction_threads.back(), "base_compact");
            }
        }

        _cumulative_compaction_threads.reserve(cumulative_compaction_num_threads);
        int32_t cumulative_step = config::tablet_map_shard_size / cumulative_compaction_num_threads_per_disk +
                                  (config::tablet_map_shard_size % cumulative_compaction_num_threads_per_disk != 0);
        for (int32_t i = 0; i < cumulative_compaction_num_threads_per_disk; i++) {
            std::pair<int32_t, int32_t> tablet_shards_range;
            if (config::tablet_map_shard_size >= cumulative_compaction_num_threads_per_disk) {
                tablet_shards_range.first = std::min(config::tablet_map_shard_size, cumulative_step * i);
                tablet_shards_range.second = std::min(config::tablet_map_shard_size, cumulative_step * (i + 1));
            } else {
                tablet_shards_range.first = 0;
                tablet_shards_range.second = config::tablet_map_shard_size;
            }
            for (int32_t j = 0; j < data_dir_num; j++) {
                _cumulative_compaction_threads.emplace_back([this, data_dirs, j, tablet_shards_range] {
                    _cumulative_compaction_thread_callback(nullptr, data_dirs[j], tablet_shards_range);
                });
                Thread::set_thread_name(_cumulative_compaction_threads.back(), "cumulat_compact");
            }
        }
    } else {
        int32_t max_task_num = 0;
        // new compaction framework
        if (config::base_compaction_num_threads_per_disk >= 0 &&
            config::cumulative_compaction_num_threads_per_disk >= 0) {
            max_task_num = static_cast<int32_t>(StorageEngine::instance()->get_store_num() *
                                                (config::cumulative_compaction_num_threads_per_disk +
                                                 config::base_compaction_num_threads_per_disk));
        } else {
            // When cumulative_compaction_num_threads_per_disk or config::base_compaction_num_threads_per_disk is less than 0,
            // there is no limit to _max_task_num if max_compaction_concurrency is also less than 0, and here we set maximum value to be 20.
            max_task_num = std::min(20, static_cast<int32_t>(StorageEngine::instance()->get_store_num() * 5));
        }
        if (config::max_compaction_concurrency > 0 && config::max_compaction_concurrency < max_task_num) {
            max_task_num = config::max_compaction_concurrency;
        }

        vectorized::Compaction::init(max_task_num);
    }

    int32_t update_compaction_num_threads_per_disk =
            config::update_compaction_num_threads_per_disk >= 0 ? config::update_compaction_num_threads_per_disk : 1;
    int32_t update_compaction_num_threads = update_compaction_num_threads_per_disk * data_dir_num;
    _update_compaction_threads.reserve(update_compaction_num_threads);
    for (uint32_t i = 0; i < update_compaction_num_threads; ++i) {
        _update_compaction_threads.emplace_back([this, data_dir_num, data_dirs, i] {
            _update_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
        });
        Thread::set_thread_name(_update_compaction_threads.back(), "update_compact");
    }

    LOG(INFO) << "All backgroud threads of storage engine have started.";
    return Status::OK();
}

void evict_pagecache(StoragePageCache* cache, int64_t bytes_to_dec, std::atomic<bool>& stoped) {
    if (bytes_to_dec > 0) {
        int64_t bytes = bytes_to_dec;
        while (bytes >= GCBYTES_ONE_STEP) {
            // Evicting 1GB of data takes about 1 second, check if process have been canceled.
            if (UNLIKELY(stoped)) {
                return;
            }
            cache->adjust_capacity(-GCBYTES_ONE_STEP, kcacheMinSize);
            bytes -= GCBYTES_ONE_STEP;
        }
        if (bytes > 0) {
            cache->adjust_capacity(-bytes, kcacheMinSize);
        }
    }
}

void* StorageEngine::_base_compaction_thread_callback(void* arg, DataDir* data_dir,
                                                      std::pair<int32_t, int32_t> tablet_shards) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    //string last_base_compaction_fs;
    //TTabletId last_base_compaction_tablet_id = -1;
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->capacity_limit_reached(0)) {
            status = _perform_base_compaction(data_dir, tablet_shards);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::base_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "base compaction check interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_update_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->capacity_limit_reached(0)) {
            status = _perform_update_compaction(data_dir);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::update_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "update compaction check interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_repair_compaction_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        std::pair<int64_t, vector<uint32_t>> task(-1, vector<uint32>());
        {
            std::lock_guard lg(_repair_compaction_tasks_lock);
            if (!_repair_compaction_tasks.empty()) {
                task = _repair_compaction_tasks.back();
                _repair_compaction_tasks.pop_back();
            }
        }
        if (task.first != -1) {
            auto tablet = _tablet_manager->get_tablet(task.first);
            if (!tablet) {
                LOG(WARNING) << "repair compaction failed, tablet not found: " << task.first;
                continue;
            }
            if (tablet->updates() == nullptr) {
                LOG(ERROR) << "repair compaction failed, tablet not primary key tablet found: " << task.first;
                continue;
            }
            vector<pair<uint32_t, string>> rowset_results;
            for (auto rowsetid : task.second) {
                auto st = tablet->updates()->compaction(ExecEnv::GetInstance()->compaction_mem_tracker(), {rowsetid});
                if (!st.ok()) {
                    LOG(WARNING) << "repair compaction failed tablet: " << task.first << " rowset: " << rowsetid << " "
                                 << st;
                } else {
                    LOG(INFO) << "repair compaction succeed tablet: " << task.first << " rowset: " << rowsetid << " "
                              << st;
                }
                rowset_results.emplace_back(rowsetid, st.to_string());
            }
            _executed_repair_compaction_tasks.emplace_back(task.first, std::move(rowset_results));
        }
        do {
            // do a compaction per 10min, to reduce potential memory pressure
            SLEEP_IN_BG_WORKER(config::repair_compaction_interval_seconds);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }
    return nullptr;
}

struct pair_hash {
public:
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U>& x) const {
        return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
    }
};

void StorageEngine::submit_repair_compaction_tasks(
        const std::vector<std::pair<int64_t, std::vector<uint32_t>>>& tasks) {
    std::lock_guard lg(_repair_compaction_tasks_lock);
    std::unordered_set<int64_t> all_tasks;
    for (const auto& t : _repair_compaction_tasks) {
        all_tasks.insert(t.first);
    }
    for (const auto& task : tasks) {
        if (all_tasks.find(task.first) == all_tasks.end()) {
            all_tasks.insert(task.first);
            _repair_compaction_tasks.push_back(task);
            LOG(INFO) << "submit repair compaction task tablet: " << task.first << " #rowset:" << task.second.size()
                      << " current tasks: " << _repair_compaction_tasks.size();
        }
    }
}

std::vector<std::pair<int64_t, std::vector<std::pair<uint32_t, std::string>>>>
StorageEngine::get_executed_repair_compaction_tasks() {
    std::lock_guard lg(_repair_compaction_tasks_lock);
    return _executed_repair_compaction_tasks;
}

void* StorageEngine::_disk_stat_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        _start_disk_stat_monitor();

        int32_t interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_cumulative_compaction_thread_callback(void* arg, DataDir* data_dir,
                                                            const std::pair<int32_t, int32_t>& tablet_shards_range) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->capacity_limit_reached(0)) {
            status = _perform_cumulative_compaction(data_dir, tablet_shards_range);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::cumulative_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "cumulative compaction check interval config is illegal:" << interval
                         << "will be forced set to one";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_update_cache_expire_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        int32_t expire_sec = config::update_cache_expire_sec;
        if (expire_sec <= 0) {
            LOG(WARNING) << "update_cache_expire_sec config is illegal: " << expire_sec << ", force set to 360";
            expire_sec = 360;
        }
        _update_manager->set_cache_expire_ms(expire_sec * 1000);
        int32_t sleep_sec = std::max(1, expire_sec / 2);
        SLEEP_IN_BG_WORKER(sleep_sec);
        _update_manager->expire_cache();
    }

    return nullptr;
}

void* StorageEngine::_update_cache_evict_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        SLEEP_IN_BG_WORKER(config::update_cache_evict_internal_sec);
        if (!config::enable_auto_evict_update_cache) {
            continue;
        }

        // Check config valid
        int64_t memory_urgent_level = config::memory_urgent_level;
        int64_t memory_high_level = config::memory_high_level;
        if (UNLIKELY(!(memory_urgent_level > memory_high_level && memory_high_level >= 1 &&
                       memory_urgent_level <= 100))) {
            LOG(ERROR) << "memory water level config is illegal: memory_urgent_level=" << memory_urgent_level
                       << " memory_high_level=" << memory_high_level;
            continue;
        }
        _update_manager->evict_cache(memory_urgent_level, memory_high_level);
    }
    return nullptr;
}

void* StorageEngine::_unused_rowset_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        double deleted_pct = delete_unused_rowset();
        // delete 20% means we nead speedup 5x which make interval 1/5 before
        int32_t interval = config::unused_rowset_monitor_interval * deleted_pct;
        if (interval <= 0) {
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_tablet_checkpoint_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        LOG(INFO) << "begin to do tablet meta checkpoint:" << ((DataDir*)arg)->path();
        int64_t start_time = UnixMillis();
        _tablet_manager->do_tablet_meta_checkpoint((DataDir*)arg);
        int64_t used_time = (UnixMillis() - start_time) / 1000;
        if (used_time < config::tablet_meta_checkpoint_min_interval_secs) {
            int64_t interval = config::tablet_meta_checkpoint_min_interval_secs - used_time;
            SLEEP_IN_BG_WORKER(interval);
        } else {
            sleep(1);
        }
    }

    return nullptr;
}

} // namespace starrocks
