/*
* Copyright 2016-present Facebook, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#pragma once

#include <fmt/format.h>

#include "common/config.h"
#include "jemalloc/jemalloc.h"
#include "util/logging.h"
#include "common/status.h"

namespace starrocks {

struct free_delete {
    void operator()(void* x) { free(x); }
};

class NodumpMemAllocator {
public:
    NodumpMemAllocator(std::unique_ptr<extent_hooks_t>&& arena_hooks, unsigned arena_index);
    ~NodumpMemAllocator();

    void* allocate(size_t size);
    void deallocate(void* p);

    // Destroy arena on destruction of the allocator, or on failure.
    static Status destroy_arena(unsigned arena_index);

    static std::atomic<extent_alloc_t*> _original_alloc;

    // Custom alloc hook to replace jemalloc default alloc.
    static void* alloc(extent_hooks_t* extent, void* new_addr, size_t size,
                       size_t alignment, bool* zero, bool* commit,
                       unsigned arena_ind);

private:
    // A function pointer to jemalloc default alloc. Use atomic to make sure
    // NewJemallocNodumpAllocator is thread-safe.
    //
    // Hack: original_alloc_ needs to be static for Alloc() to access it.
    // alloc needs to be static to pass to jemalloc as function pointer.

    // Custom hooks has to outlive corresponding arena.
    const std::unique_ptr<extent_hooks_t> _arena_hooks;

    // Arena index.
    const unsigned _arena_index;
};

Status new_nodump_mem_allocator(std::shared_ptr<NodumpMemAllocator>* memory_allocator);

} // namespace starrocks