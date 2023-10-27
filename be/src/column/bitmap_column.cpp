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

#include "column/bitmap_column.h"

#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

size_t BitmapColumn::byte_size(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t byte_size = 0;
    for (size_t i = 0; i < size; ++i) {
        byte_size += _pool[from + i].serialize_size();
    }
    return byte_size;
}

size_t BitmapColumn::byte_size(size_t idx) const {
    DCHECK(false) << "Don't support object column byte size";
    return 0;
}

void BitmapColumn::assign(size_t n, size_t idx) {
    if (idx != 0) {
        _pool[0] = std::move(_pool[idx]);
    }
    _pool.resize(1);
    _pool.reserve(n);

    for (size_t i = 1; i < n; ++i) {
        append(&_pool[0]);
    }

    _cache_ok = false;
}

void BitmapColumn::append(const BitmapValue* object) {
    _pool.emplace_back(*object);
    _cache_ok = false;
}

void BitmapColumn::append(BitmapValue&& object) {
    _pool.emplace_back(std::move(object));
    _cache_ok = false;
}

void BitmapColumn::append(const BitmapValue& object) {
    _pool.emplace_back(object);
    _cache_ok = false;
}

void BitmapColumn::remove_first_n_values(size_t count) {
    size_t remain_size = _pool.size() - count;
    for (size_t i = 0; i < remain_size; ++i) {
        _pool[i] = std::move(_pool[count + i]);
    }

    _pool.resize(remain_size);
    _cache_ok = false;
}

void BitmapColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    for (size_t i = offset; i < count + offset; ++i) {
        append(obj_col.get_object(i));
    }
}

void BitmapColumn::append_shallow_copy(const Column& src, size_t offset, size_t count) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    for (size_t i = offset; i < count + offset; ++i) {
        append({*obj_col.get_object(i), false});
    }
}

void BitmapColumn::append_selective(const starrocks::Column& src, const uint32_t* indexes, uint32_t from,
                                    uint32_t size) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    for (uint32_t j = 0; j < size; ++j) {
        append(obj_col.get_object(indexes[from + j]));
    }
}

void BitmapColumn::append_selective_shallow_copy(const Column& src, const uint32_t* indexes, uint32_t from,
                                                 uint32_t size) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    for (uint32_t j = 0; j < size; ++j) {
        append({*obj_col.get_object(indexes[from + j]), false});
    }
}

void BitmapColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    for (uint32_t i = 0; i < size; i++) {
        append({*obj_col.get_object(index), deep_copy});
    }
}

bool BitmapColumn::append_strings(const Buffer<starrocks::Slice>& strs) {
    _pool.reserve(_pool.size() + strs.size());
    for (const Slice& s : strs) {
        _pool.emplace_back(s);
    }

    _cache_ok = false;
    return true;
}

void BitmapColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* slice = reinterpret_cast<const Slice*>(value);
    _pool.reserve(_pool.size() + count);

    for (size_t i = 0; i < count; ++i) {
        _pool.emplace_back(*reinterpret_cast<BitmapValue*>(slice->data));
    }

    _cache_ok = false;
}

void BitmapColumn::append_default() {
    _pool.emplace_back(BitmapValue());
    _cache_ok = false;
}

void BitmapColumn::append_default(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        append_default();
    }
}

void BitmapColumn::fill_default(const Filter& filter) {
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i] == 1) {
            _pool[i] = {};
        }
    }
    _cache_ok = false;
}

void BitmapColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& obj_col = down_cast<const BitmapColumn&>(src);
    size_t replace_num = src.size();
    for (size_t i = 0; i < replace_num; i++) {
        DCHECK_LT(indexes[i], _pool.size());
        _pool[indexes[i]] = *obj_col.get_object(i);
    }
    _cache_ok = false;
}

uint32_t BitmapColumn::serialize(size_t idx, uint8_t* pos) {
    DCHECK(false) << "Don't support object column serialize";
    return 0;
}

uint32_t BitmapColumn::serialize_default(uint8_t* pos) {
    DCHECK(false) << "Don't support object column serialize";
    return 0;
}

void BitmapColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                   uint32_t max_one_row_size) {
    DCHECK(false) << "Don't support object column serialize batch";
}

const uint8_t* BitmapColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "Don't support object column deserialize and append";
    return pos;
}

void BitmapColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "Don't support object column deserialize and append";
}

uint32_t BitmapColumn::serialize_size(size_t idx) const {
    DCHECK(false) << "Don't support object column byte size";
    return 0;
}

size_t BitmapColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    size_t old_sz = size();
    size_t new_sz = from;
    for (auto i = from; i < to; ++i) {
        if (filter[i]) {
            std::swap(_pool[new_sz], _pool[i]);
            new_sz++;
        }
    }
    DCHECK_LE(new_sz, to);
    if (new_sz < to) {
        for (size_t i = to; i < old_sz; i++) {
            std::swap(_pool[new_sz], _pool[i]);
            new_sz++;
        }
    }
    _pool.resize(new_sz);
    return new_sz;
}

int BitmapColumn::compare_at(size_t left, size_t right, const starrocks::Column& rhs, int nan_direction_hint) const {
    DCHECK(false) << "Don't support object column compare_at";
    return 0;
}

void BitmapColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    std::string s;
    for (uint32_t i = from; i < to; ++i) {
        s.resize(_pool[i].serialize_size());
        //TODO: May be overflow here if the object is large then 2G.
        size_t size = _pool[i].serialize(reinterpret_cast<uint8_t*>(s.data()));
        hash[i] = HashUtil::fnv_hash(s.data(), static_cast<int32_t>(size), hash[i]);
    }
}

void BitmapColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    DCHECK(false) << "object column shouldn't call crc32_hash ";
}

int64_t BitmapColumn::xor_checksum(uint32_t from, uint32_t to) const {
    DCHECK(false) << "object column shouldn't call xor_checksum";
    return 0;
}

void BitmapColumn::put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx) const {
    buf->push_null();
}

void BitmapColumn::_build_slices() const {
    // TODO(kks): improve this
    _buffer.clear();
    _slices.clear();

    // FIXME(kks): bitmap itself compress is more effective than LZ4 compress?
    // Do we really need compress bitmap here?
    for (size_t i = 0; i < _pool.size(); ++i) {
        _pool[i].compress();
    }

    size_t size = byte_size();
    _buffer.resize(size);
    _slices.reserve(_pool.size());
    size_t old_size = 0;
    for (size_t i = 0; i < _pool.size(); ++i) {
        size_t slice_size = _pool[i].serialize(_buffer.data() + old_size);
        _slices.emplace_back(_buffer.data() + old_size, slice_size);
        old_size += slice_size;
    }
}

MutableColumnPtr BitmapColumn::clone() const {
    auto p = clone_empty();
    p->append(*this, 0, size());
    return p;
}

ColumnPtr BitmapColumn::clone_shared() const {
    auto p = clone_empty();
    p->append(*this, 0, size());
    return p;
}

std::string BitmapColumn::debug_item(size_t idx) const {
    return _pool[idx].to_string();
}

StatusOr<ColumnPtr> BitmapColumn::upgrade_if_overflow() {
    if (capacity_limit_reached()) {
        return Status::InternalError("Size of ObjectColumn exceed the limit");
    }
    return nullptr;
}

} // namespace starrocks
