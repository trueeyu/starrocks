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
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/daemon.cpp

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

#include "common/daemon.h"

#include <gflags/gflags.h>

#include "block_cache/block_cache.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "common/minidump.h"
#include "exec/workgroup/work_group.h"
#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "fs/encrypt_file.h"
#include "gutil/cpu.h"
#include "jemalloc/jemalloc.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "service/backend_options.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/gc_helper.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/misc.h"
#include "util/monotime.h"
#include "util/network_util.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/timezone_utils.h"

namespace starrocks {
DEFINE_bool(cn, false, "start as compute node");

// NOTE: when BE receiving SIGTERM, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
std::atomic<bool> k_starrocks_exit = false;

// NOTE: when call `/api/_stop_be` http interface, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
// The difference between k_starrocks_exit and the flag is that
// k_starrocks_exit not only require waiting for all existing fragment to complete,
// but also waiting for all threads to exit gracefully.
std::atomic<bool> k_starrocks_exit_quick = false;

/*
 * This thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 * 6. datacache memory usage
 */
void calculate_metrics(void* arg_this) {
    int64_t last_ts = -1L;
    int64_t lst_push_bytes = -1;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        StarRocksMetrics::instance()->metrics()->trigger_hook();

        if (last_ts == -1L) {
            last_ts = MonotonicSeconds();
            lst_push_bytes = StarRocksMetrics::instance()->push_request_write_bytes.value();
            lst_query_bytes = StarRocksMetrics::instance()->query_scan_bytes.value();
            StarRocksMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
            StarRocksMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                                &lst_net_receive_bytes);
        } else {
            int64_t current_ts = MonotonicSeconds();
            long interval = (current_ts - last_ts);
            last_ts = current_ts;

            // 1. push bytes per second.
            int64_t current_push_bytes = StarRocksMetrics::instance()->push_request_write_bytes.value();
            int64_t pps = (current_push_bytes - lst_push_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->push_request_write_bytes_per_second.set_value(pps < 0 ? 0 : pps);
            lst_push_bytes = current_push_bytes;

            // 2. query bytes per second.
            int64_t current_query_bytes = StarRocksMetrics::instance()->query_scan_bytes.value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->query_scan_bytes_per_second.set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 3. max disk io util.
            StarRocksMetrics::instance()->max_disk_io_util_percent.set_value(
                    StarRocksMetrics::instance()->system_metrics()->get_max_io_util(lst_disks_io_time, 15));
            // Update lst map.
            StarRocksMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

            // 4. max network traffic.
            int64_t max_send = 0;
            int64_t max_receive = 0;
            StarRocksMetrics::instance()->system_metrics()->get_max_net_traffic(
                    lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
            StarRocksMetrics::instance()->max_network_send_bytes_rate.set_value(max_send);
            StarRocksMetrics::instance()->max_network_receive_bytes_rate.set_value(max_receive);
            // update lst map
            StarRocksMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                                &lst_net_receive_bytes);
        }

        // update datacache mem_tracker
        int64_t datacache_mem_bytes = 0;
        auto datacache_mem_tracker = GlobalEnv::GetInstance()->datacache_mem_tracker();
        if (datacache_mem_tracker) {
            BlockCache* block_cache = BlockCache::instance();
            if (block_cache->is_initialized()) {
                auto datacache_metrics = block_cache->cache_metrics();
                datacache_mem_bytes = datacache_metrics.mem_used_bytes + datacache_metrics.meta_used_bytes;
            }
#ifdef USE_STAROS
            datacache_mem_bytes += staros::starlet::fslib::star_cache_get_memory_usage();
#endif
            datacache_mem_tracker->set(datacache_mem_bytes);
        }

        auto* mem_metrics = StarRocksMetrics::instance()->system_metrics()->memory_metrics();

        LOG(INFO) << fmt::format(
                "Current memory statistics: process({}), query_pool({}), load({}), "
                "metadata({}), compaction({}), schema_change({}), "
                "page_cache({}), update({}), chunk_allocator({}), clone({}), consistency({}), "
                "datacache({}), jit({})",
                mem_metrics->process_mem_bytes.value(), mem_metrics->query_mem_bytes.value(),
                mem_metrics->load_mem_bytes.value(), mem_metrics->metadata_mem_bytes.value(),
                mem_metrics->compaction_mem_bytes.value(), mem_metrics->schema_change_mem_bytes.value(),
                mem_metrics->storage_page_cache_mem_bytes.value(), mem_metrics->update_mem_bytes.value(),
                mem_metrics->chunk_allocator_mem_bytes.value(), mem_metrics->clone_mem_bytes.value(),
                mem_metrics->consistency_mem_bytes.value(), datacache_mem_bytes,
                mem_metrics->jit_cache_mem_bytes.value());

        nap_sleep(15, [daemon] { return daemon->stopped(); });
    }
}

struct JemallocStats {
    int64_t allocated = 0;
    int64_t active = 0;
    int64_t metadata = 0;
    int64_t resident = 0;
    int64_t mapped = 0;
    int64_t retained = 0;
};

static void retrieve_jemalloc_stats(JemallocStats* stats) {
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    int64_t value = 0;
    sz = sizeof(value);
    if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
        stats->allocated = value;
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        stats->active = value;
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        stats->metadata = value;
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        stats->resident = value;
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        stats->mapped = value;
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        stats->retained = value;
    }
}

// Tracker the memory usage of jemalloc
void jemalloc_tracker_daemon(void* arg_this) {
    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        JemallocStats stats;
        retrieve_jemalloc_stats(&stats);

        // metadata
        if (GlobalEnv::GetInstance()->jemalloc_metadata_traker() && stats.metadata > 0) {
            auto tracker = GlobalEnv::GetInstance()->jemalloc_metadata_traker();
            int64_t delta = stats.metadata - tracker->consumption();
            tracker->consume(delta);
        }

        // fragmentation
        if (GlobalEnv::GetInstance()->jemalloc_fragmentation_traker()) {
            if (stats.resident > 0 && stats.allocated > 0 && stats.metadata > 0) {
                int64_t fragmentation = stats.resident - stats.allocated - stats.metadata;
                fragmentation *= config::jemalloc_fragmentation_ratio;

                // In case that released a lot of memory but not get purged, we would not consider it as fragmentation
                bool released_a_lot = stats.allocated < (stats.resident * 0.5);
                if (released_a_lot) {
                    fragmentation = 0;
                }

                if (fragmentation >= 0) {
                    auto tracker = GlobalEnv::GetInstance()->jemalloc_fragmentation_traker();
                    int64_t delta = fragmentation - tracker->consumption();
                    tracker->consume(delta);
                }
            }
        }

        nap_sleep(1, [daemon] { return daemon->stopped(); });
    }
}

static void init_starrocks_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    paths.reserve(store_paths.size());
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, status=" << st.message();
            return;
        }
        st = get_inet_interfaces(&network_interfaces, BackendOptions::is_bind_ipv6());
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, status=" << st.message();
            return;
        }
    }
    StarRocksMetrics::instance()->initialize(paths, init_system_metrics, disk_devices, network_interfaces);
}

void sigterm_handler(int signo, siginfo_t* info, void* context) {
    if (info == nullptr) {
        LOG(ERROR) << "got signal: " << strsignal(signo) << "from unknown pid, is going to exit";
    } else {
        LOG(ERROR) << "got signal: " << strsignal(signo) << " from pid: " << info->si_pid << ", is going to exit";
    }
    k_starrocks_exit.store(true);
}

int install_signal(int signo, void (*handler)(int sig, siginfo_t* info, void* context)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_sigaction = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        PLOG(ERROR) << "install signal failed, signo=" << signo;
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
}

void init_minidump() {
#ifdef __x86_64__
    if (config::sys_minidump_enable) {
        LOG(INFO) << "Minidump is enabled";
        Minidump::init();
    } else {
        LOG(INFO) << "Minidump is disabled";
    }
#else
    LOG(INFO) << "Minidump is disabled on non-x86_64 arch";
#endif
}

struct PageCacheStats {
    size_t lookup_count = 0;

    size_t base_capacity = 0;
    size_t base_usage = 0;
    size_t base_hit_count = 0;

    size_t extent_capacity = 0;
    size_t extent_usage = 0;
    size_t extent_hit_count = 0;
    size_t extent_cost = 0;

    size_t scan_time = 0;

    std::string to_string() {
        return strings::Substitute("lookup_count{$0}, base_capacity{$1}, base_usage{$2}, base_hit_count{$3}, "
                "extent_capacity{$4}, extent_usage{$5}, extent_hit_count{$6}, extent_cost{$7}, scan_time{$8}",
                lookup_count, base_capacity, base_usage, base_hit_count, extent_capacity,
                extent_usage, extent_hit_count, extent_cost, scan_time);
    }

    bool extent_exceed() {
        if (extent_usage >= extent_capacity * 0.9) {
            return true;
        } else {
            return false;
        }
    }

    bool base_exceed() {
        if (base_usage >= base_capacity * 0.9) {
            return true;
        } else {
            return false;
        }
    }

    bool reach_min() {
        if (base_usage - config::cache_transfer_size <= config::cache_min_size) {
            return true;
        } else {
            return false;
        }
    }

    double extent_cache_opt() {
        return extent_cost * 0.1 / (extent_capacity / 1024 / 1024);
    }

    void fill(MetricRegistry* metric) {
        lookup_count = ((UIntGauge*)metric->get_metric("lxh_page_cache_lookup_count"))->value();
        base_hit_count = ((UIntGauge*)metric->get_metric("lxh_page_cache_base_hit_count"))->value();
        extent_hit_count = ((UIntGauge*)metric->get_metric("lxh_page_cache_extent_write_count"))->value();

        base_capacity = ((UIntGauge*)metric->get_metric("lxh_page_cache_base_capacity"))->value();
        extent_capacity = ((UIntGauge*)metric->get_metric("lxh_page_cache_extent_capacity"))->value();
        base_usage = ((UIntGauge*)metric->get_metric("lxh_page_cache_base_usage"))->value();
        extent_usage = ((UIntGauge*)metric->get_metric("lxh_page_cache_extent_usage"))->value();

        extent_cost = ((UIntGauge*)metric->get_metric("lxh_page_cache_extent_cost"))->value();
        scan_time = GlobalEnv::GetInstance()->_total_page_cache_io_time;
    }

    static PageCacheStats calc_inc_metric(PageCacheStats* start, PageCacheStats* end) {
        PageCacheStats stat;
        stat.lookup_count = end->lookup_count - start->lookup_count;
        stat.base_hit_count = end->base_hit_count - start->base_hit_count;
        stat.extent_hit_count = end->extent_hit_count - start->extent_hit_count;
        stat.base_capacity = end->base_capacity - start->base_capacity;
        stat.extent_capacity = end->extent_capacity - start->extent_capacity;
        stat.base_usage = end->base_usage - start->base_usage;
        stat.extent_usage = end->extent_usage - start->extent_usage;
        stat.scan_time = end->scan_time - start->scan_time;

        return stat;
    }
};

struct BlockCacheStats {
    size_t lookup_count = 0;

    size_t base_capacity = 0;
    size_t base_usage = 0;
    size_t base_hit_count = 0;

    size_t extent_capacity = 0;
    size_t extent_usage = 0;
    size_t extent_hit_count = 0;
    size_t extent_cost = 0;

    size_t scan_time = 0;

    void fill(MetricRegistry* metric) {
        lookup_count = ((UIntGauge*)metric->get_metric("lxh_datacache_lookup_count"))->value();
        base_hit_count = ((UIntGauge*)metric->get_metric("lxh_datacache_base_hit_count"))->value();
        extent_hit_count = ((UIntGauge*)metric->get_metric("lxh_datacache_extent_write_count"))->value();

        base_capacity = ((UIntGauge*)metric->get_metric("lxh_datacache_base_quota"))->value();
        extent_capacity = ((UIntGauge*)metric->get_metric("lxh_datacache_extent_quota"))->value();
        base_usage = ((UIntGauge*)metric->get_metric("lxh_datacache_base_usage"))->value();
        extent_usage = ((UIntGauge*)metric->get_metric("lxh_datacache_extent_usage"))->value();

        extent_cost = ((UIntGauge*)metric->get_metric("lxh_datacache_extent_cost"))->value();

        scan_time = GlobalEnv::GetInstance()->_total_data_cache_io_time;
    }

    std::string to_string() {
        return strings::Substitute("lookup_count{$0}, base_capacity{$1}, base_usaeg{$2}, base_hit_count{$3},"
                "extent_capacity{$4}, extent_usage{$5}, extent_hit_count{$6}, extent_cost{$7}, scan_time{$8}",
                lookup_count, base_capacity, base_usage, base_hit_count, extent_capacity,
                extent_usage, extent_hit_count, extent_cost, scan_time);
    }

    static BlockCacheStats calc_inc_metric(BlockCacheStats* start, BlockCacheStats* end) {
        BlockCacheStats stat;
        stat.lookup_count = end->lookup_count - start->lookup_count;
        stat.base_hit_count = end->base_hit_count - start->base_hit_count;
        stat.extent_hit_count = end->extent_hit_count - start->extent_hit_count;
        stat.base_capacity = end->base_capacity - start->base_capacity;
        stat.extent_capacity = end->extent_capacity - start->extent_capacity;
        stat.base_usage = end->base_usage - start->base_usage;
        stat.extent_usage = end->extent_usage - start->extent_usage;
        stat.scan_time = end->scan_time - start->scan_time;

        return stat;
    }

    bool reach_min() {
        if (base_usage - config::cache_transfer_size <= config::cache_min_size) {
            return true;
        } else {
            return false;
        }
    }

    double extent_cache_opt() {
        return extent_cost * 0.1 / (extent_capacity / 1024 / 1024);
    }

    bool base_exceed() {
        if (base_usage >= base_capacity * 0.85) {
            return true;
        } else {
            return false;
        }
    }

    bool extent_exceed() {
        if (extent_usage >= extent_capacity * 0.9) {
            return true;
        } else {
            return false;
        }
    }
};

bool extent_exceed(int64_t extent_capacity, int64_t extent_usage) {
    if (extent_usage >= extent_capacity * 0.9) {
        return true;
    } else {
        return false;
    }
}

int64_t process_page_cache_exceed(PageCacheStats* page_cache_start, PageCacheStats* page_cache_end) {
    int64_t page_cache_extent_write_count = page_cache_end->extent_hit_count - page_cache_start->extent_hit_count;
    if (page_cache_extent_write_count <= 0) {
        return 0;
    } else {
        return config::cache_transfer_size;
    }
}

int64_t process_block_cache_exceed(BlockCacheStats* block_cache_start, BlockCacheStats* block_cache_end) {
    int64_t block_cache_extent_write_count = block_cache_end->extent_hit_count - block_cache_start->extent_hit_count;
    if (block_cache_extent_write_count <= 0) {
        return 0;
    } else {
        return config::cache_transfer_size;
    }
}

int64_t process_all_exceed(PageCacheStats* page_cache_start, PageCacheStats* page_cache_end,
                           BlockCacheStats* block_cache_start, BlockCacheStats* block_cache_end) {
    int64_t page_cache_extent_write_count = page_cache_end->extent_hit_count - page_cache_start->extent_hit_count;
    int64_t block_cache_extent_write_count = block_cache_end->extent_hit_count - block_cache_start->extent_hit_count;

    int64_t page_cache_extent_cost = page_cache_end->extent_cost - page_cache_start->extent_cost;
    int64_t block_cache_extent_cost = block_cache_end->extent_cost - block_cache_start->extent_cost;

    if (page_cache_extent_write_count <= 0 && block_cache_extent_write_count <= 0) {
        return 0;
    } else if (page_cache_extent_write_count <= 0) {
        if (page_cache_end->reach_min()) {
            return 0;
        } else {
            return -config::cache_transfer_size;
        }
    } else if (block_cache_extent_write_count <= 0) {
        if (block_cache_end->reach_min()) {
            return 0;
        } else {
            return config::cache_transfer_size;
        }
    } else {
        double page_cache_opt_time = page_cache_extent_cost * 1.0 / (page_cache_end->extent_capacity / 1024 / 1024);
        double block_cache_opt_time = block_cache_extent_cost * 1.0 / (block_cache_end->extent_capacity / 1024 / 1024);
        LOG(ERROR) << "CACHE_DAEMON: opt_time" << page_cache_opt_time << "," << block_cache_opt_time
                   << "," << page_cache_extent_cost << "," << block_cache_extent_cost
                   << "," << page_cache_end->extent_capacity/1024/1024
                   << "," << block_cache_end->extent_capacity/1024/1024;
        if (page_cache_opt_time > block_cache_opt_time) {
            if (page_cache_opt_time * 1.0 / block_cache_opt_time > 1.2) {
                if (block_cache_end->reach_min()) {
                    return 0;
                } else {
                    return config::cache_transfer_size;
                }
            } else {
                return 0;
            }
        } else {
            if (block_cache_opt_time * 1.0 / page_cache_opt_time > 1.2) {
                if (page_cache_end->reach_min()) {
                    return 0;
                } else {
                    return -config::cache_transfer_size;
                }
            } else {
                return 0;
            }
        }
    }
}

void inc_page_cache_size(int64_t size) {
    int64_t cache_size = GlobalEnv::GetInstance()->get_cache_size();
    int64_t cur_base_capacity = StoragePageCache::instance()->get_base_capacity();
    int64_t base_capacity = cur_base_capacity + size;
    int64_t extent_capacity = DataCacheUtils::calc_extent_size(cache_size, base_capacity,
                                                               config::block_cache_extent_percent,
                                                               config::block_cache_extent_lower_percent,
                                                               config::block_cache_extent_upper_percent);
    StoragePageCache::instance()->set_capacity(base_capacity, extent_capacity);
    LOG(ERROR) << "CACHE_DAEMON: inc page cache size: " << size;
}

void dec_page_cache_size(int64_t size) {
    int64_t cache_size = GlobalEnv::GetInstance()->get_cache_size();
    int64_t cur_base_capacity = StoragePageCache::instance()->get_base_capacity();
    int64_t base_capacity = cur_base_capacity - size;
    int64_t extent_capacity = DataCacheUtils::calc_extent_size(cache_size, base_capacity,
                                                               config::block_cache_extent_percent,
                                                               config::block_cache_extent_lower_percent,
                                                               config::block_cache_extent_upper_percent);
    StoragePageCache::instance()->set_capacity(base_capacity, extent_capacity);
    LOG(ERROR) << "CACHE_DAEMON: dec page cache size: " << size;
}

void inc_block_cache_size(int64_t size) {
    int64_t cache_size = GlobalEnv::GetInstance()->get_cache_size();
    int64_t cur_base_capacity = BlockCache::instance()->mem_quota();
    int64_t base_capacity = cur_base_capacity + size;
    int64_t extent_capacity = DataCacheUtils::calc_extent_size(cache_size, base_capacity,
                                                               config::block_cache_extent_percent,
                                                               config::block_cache_extent_lower_percent,
                                                               config::block_cache_extent_upper_percent);
    auto st = BlockCache::instance()->update_mem_quota(base_capacity, extent_capacity, false);
    LOG(ERROR) << "CACHE_DAEMON: inc block cache size: " << size << ":" << st;
}

void dec_block_cache_size(int64_t size) {
    int64_t cache_size = GlobalEnv::GetInstance()->get_cache_size();
    int64_t cur_base_capacity = BlockCache::instance()->mem_quota();
    int64_t base_capacity = cur_base_capacity - size;
    int64_t extent_capacity = DataCacheUtils::calc_extent_size(cache_size, base_capacity,
                                                               config::block_cache_extent_percent,
                                                               config::block_cache_extent_lower_percent,
                                                               config::block_cache_extent_upper_percent);
    auto st = BlockCache::instance()->update_mem_quota(base_capacity, extent_capacity, false);
    LOG(ERROR) << "CACHE_DAEMON: dec block cache size: " << size << ":" << st;
}

void cache_daemon(void* arg_this) {
    int64_t interval = config::cache_transfer_interval;
    int64_t transfer_times = config::cache_transfer_times;
    int64_t one_time_interval = interval / config::cache_transfer_times;
    std::vector<PageCacheStats> page_cache_stats(transfer_times);
    std::vector<BlockCacheStats> block_cache_stats(transfer_times);
    int64_t cur_index = 0;
    int64_t total_count = 0;
    //int64_t transfer_size = config::cache_transfer_size;
    //int64_t transfer_extent_percent = config::cache_transfer_extent_percent;

    while(true) {
        sleep(one_time_interval);
        BlockCache* block_cache = BlockCache::instance();
        if (block_cache == nullptr) {
            continue;
        }
        StoragePageCache* page_cache = StoragePageCache::instance();
        if (page_cache == nullptr) {
            continue;
        }
        auto* metric = StarRocksMetrics::instance()->metrics();
        if (metric == nullptr) {
            continue;
        }

        total_count++;
        page_cache_stats[cur_index].fill(metric);
        block_cache_stats[cur_index].fill(metric);

        auto* page_cache_stat = &page_cache_stats[cur_index];
        auto* block_cache_stat = &block_cache_stats[cur_index];
        int64_t end_index = cur_index;
        int64_t start_index = 0;
        int64_t inc_index_1 = 0;
        int64_t inc_index_2 = 0;
        if (cur_index == 0) {
            inc_index_1 = transfer_times - 1;
            inc_index_2 = cur_index;
        } else {
            inc_index_1 = cur_index - 1;
            inc_index_2 = cur_index;
        }
        if (cur_index + 1 >= transfer_times) {
            cur_index = 0;
        } else {
            cur_index++;
        }
        start_index = cur_index;

        LOG(ERROR) << "CACHE_DAEMON: PAGE_CACHE_METRICS: " << start_index << "," << end_index << "," << page_cache_stat->to_string();
        LOG(ERROR) << "CACHE_DAEMON: BLOCK_CACHE_METRICS: " << start_index << "," << end_index << "," << block_cache_stat->to_string();

        if (total_count <= transfer_times) {
            LOG(ERROR) << "CACHE_DAEMON: start times: " << total_count << ", " << transfer_times;
            continue;
        }

        PageCacheStats inc_page_cache_stat = PageCacheStats::calc_inc_metric(&page_cache_stats[inc_index_1], &page_cache_stats[inc_index_2]);
        BlockCacheStats inc_block_cache_stat = BlockCacheStats::calc_inc_metric(&block_cache_stats[inc_index_1], &block_cache_stats[inc_index_2]);

        LOG(ERROR) << "CACHE_DAEMON: INC PAGE: " << inc_page_cache_stat.to_string();
        LOG(ERROR) << "CACHE_DAEMON: INC BLOCK: " << inc_block_cache_stat.to_string();

        if (!page_cache_stat->extent_exceed() && !block_cache_stat->extent_exceed()) {
            LOG(ERROR) << "CACHE_DAEMON: all cache not exceed, continue";
            continue;
        } else if (page_cache_stat->extent_exceed() && block_cache_stat->extent_exceed()) {
            auto* start_page_cache_stat = &page_cache_stats[start_index];
            auto* end_page_cache_stat = &page_cache_stats[end_index];

            auto* start_block_cache_stat = &block_cache_stats[start_index];
            auto* end_block_cache_stat = &block_cache_stats[end_index];
            int64_t transfer_size = process_all_exceed(start_page_cache_stat, end_page_cache_stat,
                                                       start_block_cache_stat, end_block_cache_stat);
            LOG(ERROR) << "CACHE_DAEMON: all cache exceed: " << transfer_size;
            if (transfer_size == 0) {
                continue;
            } else if (transfer_size > 0) {
                if (page_cache_stat->base_exceed()) {
                    inc_page_cache_size(transfer_size);
                    dec_block_cache_size(transfer_size);
                } else {
                    LOG(ERROR) << "CACHE_DAEMON: page cache base not exceed";
                    continue;
                }
            } else {
                if (block_cache_stat->base_exceed()) {
                    dec_page_cache_size(-transfer_size);
                    inc_block_cache_size(-transfer_size);
                } else {
                    LOG(ERROR) << "CACHE_DAEMON: block cache base not exceed";
                    continue;
                }
            }
        } else if (page_cache_stat->extent_exceed()) {
            if (page_cache_stat->base_exceed()) {
                auto* start_page_cache_stat = &page_cache_stats[start_index];
                auto* end_page_cache_stat = &page_cache_stats[end_index];
                int64 transfer_size = process_page_cache_exceed(start_page_cache_stat, end_page_cache_stat);
                LOG(ERROR) << "CACHE_DAEMON: page cache exceed: " << transfer_size;
                if (transfer_size == 0) {
                    continue;
                } else {
                    inc_page_cache_size(transfer_size);
                    dec_block_cache_size(transfer_size);
                }
            } else {
                LOG(ERROR) << "CACHE_DAEMON: page cache base not exceed";
                continue;
            }
        } else {
            if (block_cache_stat->extent_exceed()) {
                auto* start_block_cache_stat = &block_cache_stats[start_index];
                auto* end_block_cache_stat = &block_cache_stats[end_index];
                int64 transfer_size = process_block_cache_exceed(start_block_cache_stat, end_block_cache_stat);
                LOG(ERROR) << "CACHE_DAEMON: block cache exceed: " << transfer_size;
                if (transfer_size == 0) {
                    continue;
                } else {
                    inc_block_cache_size(transfer_size);
                    dec_page_cache_size(transfer_size);
                }
            } else {
                LOG(ERROR) << "CACHE_DAEMON: block cache base not exceed";
                continue;
            }
        }
    }
}

void Daemon::init(bool as_cn, const std::vector<StorePath>& paths) {
    if (as_cn) {
        init_glog("cn", true);
    } else {
        init_glog("be", true);
    }

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    LOG(INFO) << base::CPU::instance()->debug_string();
    LOG(INFO) << "openssl aesni support: " << openssl_supports_aesni();

    CHECK(UserFunctionCache::instance()->init(config::user_function_dir).ok());

    date::init_date_cache();

    TimezoneUtils::init_time_zones();

    init_starrocks_metrics(paths);

    if (config::enable_metric_calculator) {
        std::thread calculate_metrics_thread(calculate_metrics, this);
        Thread::set_thread_name(calculate_metrics_thread, "metrics_daemon");
        _daemon_threads.emplace_back(std::move(calculate_metrics_thread));
    }

    if (config::enable_jemalloc_memory_tracker) {
        std::thread jemalloc_tracker_thread(jemalloc_tracker_daemon, this);
        Thread::set_thread_name(jemalloc_tracker_thread, "jemalloc_tracker_daemon");
        _daemon_threads.emplace_back(std::move(jemalloc_tracker_thread));
    }

    if (config::enable_cache_transfer) {
        std::thread cache_tmp_thread(cache_daemon, this);
        Thread::set_thread_name(cache_tmp_thread, "cache tmp thread");
        _daemon_threads.emplace_back(std::move(cache_tmp_thread));
    }

    init_signals();
    init_minidump();
}

void Daemon::stop() {
    _stopped.store(true, std::memory_order_release);
    size_t thread_size = _daemon_threads.size();
    for (size_t i = 0; i < thread_size; ++i) {
        if (_daemon_threads[i].joinable()) {
            _daemon_threads[i].join();
        }
    }
}

bool Daemon::stopped() {
    return _stopped.load(std::memory_order_consume);
}

} // namespace starrocks
