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

#include "runtime/memory/nodump_mem_allocator.h"

#include "sys/mman.h"

namespace starrocks {

std::atomic<extent_alloc_t*> NodumpMemAllocator::_original_alloc = nullptr;

NodumpMemAllocator::NodumpMemAllocator(std::unique_ptr<extent_hooks_t>&& arena_hooks, unsigned arena_index)
        : _arena_hooks(std::move(arena_hooks)), _arena_index(arena_index) {}

void* NodumpMemAllocator::allocate(size_t size) {
    return jemallocx(size, MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE);
}

void NodumpMemAllocator::deallocate(void* p) {
    jedallocx(p, MALLOCX_TCACHE_NONE);
}

Status NodumpMemAllocator::destroy_arena(unsigned arena_index) {
    std::string key = "arena." + std::to_string(arena_index) + ".destroy";
    int ret = jemallctl(key.c_str(), nullptr, 0, nullptr, 0);
    if (ret != 0) {
        return Status::InternalError("Failed to destroy jemalloc arena, error code: " + std::to_string(ret));
    }
    return Status::OK();
}

void* NodumpMemAllocator::alloc(extent_hooks_t* extent, void* new_addr, size_t size, size_t alignment, bool* zero,
                                bool* commit, unsigned arena_ind) {
    extent_alloc_t* original_alloc = _original_alloc.load(std::memory_order_relaxed);
    void* result = original_alloc(extent, new_addr, size, alignment, zero, commit, arena_ind);
    if (result != nullptr) {
        int ret = madvise(result, size, MADV_DONTDUMP);
        if (ret != 0) {
            LOG(FATAL) << "alloc page failed";
        }
    }
    return result;
}

NodumpMemAllocator::~NodumpMemAllocator() {
    // Destroy arena. Silently ignore error.
    (void)destroy_arena(_arena_index);
}

Status new_nodump_mem_allocator(std::shared_ptr<NodumpMemAllocator>* memory_allocator) {
    if (memory_allocator == nullptr) {
        return Status::InvalidArgument("memory_allocator must be non-null.");
    }
    *memory_allocator = nullptr;

    // Create arena.
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int ret = jemallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (ret != 0) {
        return Status::InternalError("Failed to create jemalloc arena, error code: " + std::to_string(ret));
    }

    // Read existing hooks.
    std::string key = "arena." + std::to_string(arena_index) + ".extent_hooks";
    extent_hooks_t* hooks;
    size_t hooks_size = sizeof(hooks);
    ret = jemallctl(key.c_str(), &hooks, &hooks_size, nullptr, 0);
    if (ret != 0) {
        (void)NodumpMemAllocator::destroy_arena(arena_index);
        return Status::InternalError("Failed to read existing hooks, error code: " + std::to_string(ret));
    }

    // Store existing alloc.
    extent_alloc_t* original_alloc = hooks->alloc;
    extent_alloc_t* expected = nullptr;
    bool success = NodumpMemAllocator::_original_alloc.compare_exchange_strong(expected, original_alloc);
    if (!success && original_alloc != expected) {
        (void) NodumpMemAllocator::destroy_arena(arena_index);
        return Status::InternalError("Original alloc conflict.");
    }

    // Set the custom hook.
    std::unique_ptr<extent_hooks_t> new_hooks(new extent_hooks_t(*hooks));
    new_hooks->alloc = &NodumpMemAllocator::alloc;
    extent_hooks_t* hooks_ptr = new_hooks.get();
    ret = jemallctl(key.c_str(), nullptr, nullptr, &hooks_ptr, sizeof(hooks_ptr));
    if (ret != 0) {
        (void)NodumpMemAllocator::destroy_arena(arena_index);
        return Status::InternalError("Failed to set custom hook, error code: " + std::to_string(ret));
    }

    // Create cache allocator.
    memory_allocator->reset(new NodumpMemAllocator(std::move(new_hooks), arena_index));
    return Status::OK();
}
} // namespace starrocks