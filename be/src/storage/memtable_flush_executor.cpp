// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memtable_flush_executor.cpp

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

#include "storage/memtable_flush_executor.h"

#include <memory>

#include "runtime/current_thread.h"
#include "storage/vectorized/memtable.h"

namespace starrocks {

class MemtableFlushTask final : public Runnable {
public:
    MemtableFlushTask(FlushToken* flush_token, std::unique_ptr<vectorized::MemTable> memtable)
            : _flush_token(flush_token), _memtable(std::move(memtable)) {}

    ~MemtableFlushTask() = default;

    void run() override {
        SCOPED_THREAD_LOCAL_MEM_SETTER(_memtable->mem_tracker(), false);

        _flush_token->_stats.cur_flush_count++;
        _flush_token->_flush_memtable(_memtable.get());
        _flush_token->_stats.cur_flush_count--;
        _memtable.reset();
    }

private:
    FlushToken* _flush_token;
    std::unique_ptr<vectorized::MemTable> _memtable;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000 << ", flush count=" << stat.flush_count << ")"
       << ", flush flush_size_bytes = " << stat.flush_size_bytes;
    return os;
}

Status FlushToken::submit(std::unique_ptr<vectorized::MemTable> memtable) {
    RETURN_IF_ERROR(status());
    // Does not acount the size of MemtableFlushTask into any memory tracker
    SCOPED_THREAD_LOCAL_MEM_SETTER(nullptr, false);
    auto task = std::make_shared<MemtableFlushTask>(this, std::move(memtable));
    return _flush_token->submit(std::move(task));
}

void FlushToken::cancel2() {
    set_status(Status::Cancelled("use canceled"));
}

void FlushToken::cancel() {
    std::cout<<"FlushToken::cancel_1: "<<_id<<std::endl;
    set_status(Status::Cancelled("use canceled"));
    std::cout<<"FlushToken::cancel_2: "<<_id<<std::endl;
    _flush_token->shutdown();
    std::cout<<"FlushToken::cancel_3: "<<_id<<std::endl;
}

Status FlushToken::wait() {
    _flush_token->wait();
    std::lock_guard l(_status_lock);
    return _status;
}

void FlushToken::_flush_memtable(vectorized::MemTable* memtable) {
    // If previous flush has failed, return directly
    if (!status().ok()) {
        std::cout<<"FlushToke::_flush_memtable cancel: "<<_id<<":"<<memtable->tablet_id()<<std::endl;
        return;
    } else {
        std::cout<<"FlushToke::_flush_memtable success: "<<_id<<":"<<memtable->tablet_id()<<std::endl;
    }

    MonotonicStopWatch timer;
    timer.start();
    set_status(memtable->flush());
    std::cout<<"BEFORE_SLEEP"<<std::endl;
    sleep(10);
    std::cout<<"END_SLEEP"<<std::endl;
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_count++;
    _stats.flush_size_bytes += memtable->memory_usage();
}

Status MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = data_dir_num * min_threads;
    return ThreadPoolBuilder("mem_tab_flush") // mem table flush
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

std::unique_ptr<FlushToken> MemTableFlushExecutor::create_flush_token(ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<FlushToken>(_flush_pool->new_token(execution_mode));
}

} // namespace starrocks
