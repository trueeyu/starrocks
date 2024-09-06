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

#include <cstdint>
#include <string>

#include "fmt/format.h"
#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace starrocks {

class TUniqueId;

inline thread_local MemTracker* tls_mem_tracker = nullptr;
inline thread_local MemTracker* tls_operator_mem_tracker = nullptr;
inline thread_local bool tls_is_thread_status_init = false;

class CurrentThread {
private:
    class MemCacheManager {
    public:
        MemCacheManager(std::function<MemTracker*()>&& loader) : _loader(std::move(loader)) {}
        MemCacheManager(const MemCacheManager&) = delete;
        MemCacheManager(MemCacheManager&&) = delete;

        void consume(int64_t size) {
            size = _consume_from_reserved(size);
            _cache_size += size;
            _total_consumed_bytes += size;
            if (_cache_size >= BATCH_SIZE) {
                commit(false);
            }
        }

        bool try_mem_consume_with_limited_tracker(int64_t size) {
            MemTracker* cur_tracker = _loader();
            _cache_size += size;
            _total_consumed_bytes += size;
            if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
                MemTracker* limit_tracker = cur_tracker->try_consume_with_limited(_cache_size);
                if (LIKELY(limit_tracker == nullptr)) {
                    _cache_size = 0;
                    return true;
                } else {
                    _cache_size -= size;
                    _try_consume_mem_size = size;
                    return false;
                }
            }
            return true;
        }

        bool try_mem_reserve(int64_t reserve_bytes) {
            DCHECK(_reserved_bytes == 0);
            DCHECK(reserve_bytes >= 0);
            if (try_mem_consume_with_limited_tracker(reserve_bytes)) {
                _reserved_bytes = reserve_bytes;
                return true;
            }
            return false;
        }

        void release_reserved() {
            if (_reserved_bytes) {
                release(_reserved_bytes);
                _reserved_bytes = 0;
            }
        }
        // release memory to reserved
        void release_to_reserved(size_t release_bytes) { _reserved_bytes += release_bytes; }

        void release(int64_t size) {
            _cache_size -= size;
            if (_cache_size <= -BATCH_SIZE) {
                commit(false);
            }
        }

        void commit(bool is_ctx_shift) {
            MemTracker* cur_tracker = _loader();
            if (cur_tracker != nullptr) {
                cur_tracker->consume(_cache_size);
            }
            _cache_size = 0;
        }

        int64_t try_consume_mem_size() {
            auto res = _try_consume_mem_size;
            _try_consume_mem_size = 0;
            return res;
        }

        int64_t get_consumed_bytes() const { return _total_consumed_bytes; }

    private:
        int64_t _consume_from_reserved(int64_t size) {
            if (_reserved_bytes > size) {
                _reserved_bytes -= size;
                size = 0;
            } else {
                size -= _reserved_bytes;
                _reserved_bytes = 0;
            }
            return size;
        }

        const static int64_t BATCH_SIZE = 2 * 1024 * 1024;

        std::function<MemTracker*()> _loader;

        int64_t _reserved_bytes = 0;

        // Allocated or delocated but not committed memory bytes, can be negative
        int64_t _cache_size = 0;
        int64_t _total_consumed_bytes = 0; // Totally consumed memory bytes
        int64_t _try_consume_mem_size = 0; // Last time tried to consumed bytes
    };

public:
    CurrentThread() : _mem_cache_manager(mem_tracker) {
        tls_is_thread_status_init = true;
    }
    ~CurrentThread();

    void mem_tracker_ctx_shift() { _mem_cache_manager.commit(true); }

    void set_query_id(const starrocks::TUniqueId& query_id) { _query_id = query_id; }
    const starrocks::TUniqueId& query_id() { return _query_id; }

    void set_fragment_instance_id(const starrocks::TUniqueId& fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }
    const starrocks::TUniqueId& fragment_instance_id() { return _fragment_instance_id; }
    void set_pipeline_driver_id(int32_t driver_id) { _driver_id = driver_id; }
    int32_t get_driver_id() const { return _driver_id; }

    void set_custom_coredump_msg(const std::string& custom_coredump_msg) { _custom_coredump_msg = custom_coredump_msg; }

    const std::string& get_custom_coredump_msg() const { return _custom_coredump_msg; }

    // Return prev memory tracker.
    starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* mem_tracker) {
        release_reserved();
        mem_tracker_ctx_shift();
        auto* prev = tls_mem_tracker;
        tls_mem_tracker = mem_tracker;
        return prev;
    }

    bool check_mem_limit() { return _check; }

    static starrocks::MemTracker* mem_tracker();
    static starrocks::MemTracker* operator_mem_tracker();

    static CurrentThread& current();

    bool set_is_catched(bool is_catched) {
        bool old = _is_catched;
        _is_catched = is_catched;
        return old;
    }

    bool is_catched() const { return _is_catched; }

    void mem_consume(int64_t size) {
        _mem_cache_manager.consume(size);
    }

    bool try_mem_reserve(int64_t size) {
        if (_mem_cache_manager.try_mem_reserve(size)) {
            _reserve_mod = true;
            return true;
        }
        return false;
    }

    void release_reserved() {
        _reserve_mod = false;
        _mem_cache_manager.release_reserved();
    }

    void mem_release(int64_t size) {
        if (_reserve_mod) {
            _mem_cache_manager.release_to_reserved(size);
        } else {
            _mem_cache_manager.release(size);
        }
    }

    static void mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->consume(size);
        }
    }

    static void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

    // get last time try consume and reset
    int64_t try_consume_mem_size() { return _mem_cache_manager.try_consume_mem_size(); }

    int64_t get_consumed_bytes() const { return _mem_cache_manager.get_consumed_bytes(); }

private:
    // In order to record operator level memory trace while keep up high performance, we need to
    // record the normal MemTracker's tree and operator's isolated MemTracker independently.
    // `tls_operator_mem_tracker` will be updated every time when `Operator::pull_chunk` or `Operator::push_chunk`
    // is invoked, the frequrency is a little bit high, but it does little harm to performance,
    // because operator's MemTracker, which is a dangling MemTracker(withouth parent), has no concurrency conflicts
    MemCacheManager _mem_cache_manager;
    // Store in TLS for diagnose coredump easier
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    std::string _custom_coredump_msg{};
    int32_t _driver_id = 0;
    bool _is_catched = false;
    bool _check = true;
    bool _reserve_mod = false;
};

inline thread_local CurrentThread tls_thread_status;

#define RELEASE_RESERVED_GUARD() \
    auto VARNAME_LINENUM(defer) = DeferOp([] { CurrentThread::current().release_reserved(); });

#define SET_TRACE_INFO(driver_id, query_id, fragment_instance_id) \
    CurrentThread::current().set_pipeline_driver_id(driver_id);   \
    CurrentThread::current().set_query_id(query_id);              \
    CurrentThread::current().set_fragment_instance_id(fragment_instance_id);

#define RESET_TRACE_INFO()                              \
    CurrentThread::current().set_pipeline_driver_id(0); \
    CurrentThread::current().set_query_id({});          \
    CurrentThread::current().set_fragment_instance_id({});

#define SCOPED_SET_TRACE_INFO(driver_id, query_id, fragment_instance_id) \
    SET_TRACE_INFO(driver_id, query_id, fragment_instance_id)            \
    auto VARNAME_LINENUM(defer) = DeferOp([] { RESET_TRACE_INFO() });

#define SCOPED_SET_CUSTOM_COREDUMP_MSG(custom_coredump_msg)                \
    CurrentThread::current().set_custom_coredump_msg(custom_coredump_msg); \
    auto VARNAME_LINENUM(defer) = DeferOp([] { CurrentThread::current().set_custom_coredump_msg({}); });

// TRY_CATCH_ALL will not set catched=true, only used for catch unexpected crash,
// cannot be used to control memory usage.
#define TRY_CATCH_ALL(result, stmt)                                                      \
    do {                                                                                 \
        try {                                                                            \
            { result = stmt; }                                                           \
        } catch (std::runtime_error const& e) {                                          \
            result = Status::RuntimeError(fmt::format("Runtime error: {}", e.what()));   \
        } catch (std::exception const& e) {                                              \
            result = Status::InternalError(fmt::format("Internal error: {}", e.what())); \
        } catch (...) {                                                                  \
            result = Status::Unknown("Unknown error");                                   \
        }                                                                                \
    } while (0)
} // namespace starrocks
