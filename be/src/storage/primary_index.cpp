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

#include "storage/primary_index.h"

#include <memory>
#include <mutex>

#include "common/tracer.h"
#include "gutil/strings/substitute.h"
#include "io/io_profiler.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_dump.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"
#include "util/stack_util.h"
#include "util/xxh3.h"

namespace starrocks {

#pragma pack(push)
#pragma pack(4)
struct RowIdPack4 {
    uint64_t value;
    RowIdPack4() = default;
    RowIdPack4(uint64_t v) : value(v) {}
};
#pragma pack(pop)

template <class T>
class TraceAlloc {
public:
    using value_type = T;

    TraceAlloc() noexcept = default;
    template <class U>
    TraceAlloc(TraceAlloc<U> const&) noexcept {}

    value_type* allocate(std::size_t n) { return static_cast<value_type*>(::operator new(n * sizeof(value_type))); }

    void deallocate(value_type* p, std::size_t) noexcept { ::operator delete(p); }
};

PrimaryIndex::PrimaryIndex() = default;

PrimaryIndex::~PrimaryIndex() {
    if (_tablet_id != 0) {
        if (!_status.ok()) {
            LOG(WARNING) << "bad primary index released table:" << _table_id << " tablet:" << _tablet_id
                         << " memory: " << memory_usage();
        } else {
            LOG(INFO) << "primary index released table:" << _table_id << " tablet:" << _tablet_id
                      << " memory: " << memory_usage();
        }
    }
}

PrimaryIndex::PrimaryIndex(const Schema& pk_schema) {
    _set_schema(pk_schema);
}

void PrimaryIndex::_set_schema(const Schema& pk_schema) {
    _pk_schema = pk_schema;
    std::vector<ColumnId> sort_key_idxes(pk_schema.num_fields());
    for (ColumnId i = 0; i < pk_schema.num_fields(); ++i) {
        sort_key_idxes[i] = i;
    }
    _key_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pk_schema);
}

Status PrimaryIndex::load(Tablet* tablet) {
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded) {
        return _status;
    }
    _status = _do_load(tablet);
    _loaded = true;
    if (!_status.ok()) {
        LOG(WARNING) << "load PrimaryIndex error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                     << get_stack_trace();
    }
    return _status;
}

void PrimaryIndex::unload() {
    std::lock_guard<std::mutex> lg(_lock);
    if (!_loaded) {
        return;
    }
    LOG(INFO) << "unload primary index tablet:" << _tablet_id << " size:" << size() << " memory: " << memory_usage();
    if (_persistent_index) {
        _persistent_index.reset();
    }
    _status = Status::OK();
    _loaded = false;
    _calc_memory_usage();
}

static string int_list_to_string(const vector<uint32_t>& l) {
    string ret;
    for (size_t i = 0; i < l.size(); i++) {
        if (i > 0) {
            ret.append(",");
        }
        ret.append(std::to_string(l[i]));
    }
    return ret;
}

Status PrimaryIndex::prepare(const EditVersion& version, size_t n) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->prepare(version, n);
    }
    return Status::OK();
}

Status PrimaryIndex::commit(PersistentIndexMetaPB* index_meta) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->commit(index_meta);
    }
    _calc_memory_usage();
    return Status::OK();
}

Status PrimaryIndex::on_commited() {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->on_commited();
    }
    return Status::OK();
}

Status PrimaryIndex::abort() {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->abort();
    }
    return Status::OK();
}

Status PrimaryIndex::_do_load(Tablet* tablet) {
    CHECK_MEM_LIMIT("PrimaryIndex::_do_load");
    _table_id = tablet->belonged_table_id();
    _tablet_id = tablet->tablet_id();
    auto span = Tracer::Instance().start_trace_tablet("primary_index_load", tablet->tablet_id());
    auto scoped_span = trace::Scope(span);
    MonotonicStopWatch timer;
    timer.start();

    const TabletSchemaCSPtr tablet_schema_ptr = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema_ptr->num_key_columns());
    for (auto i = 0; i < tablet_schema_ptr->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema_ptr, pk_columns);
    _set_schema(pkey_schema);

    // load persistent index if enable persistent index meta

    if (tablet->get_enable_persistent_index()) {
        // TODO
        // PersistentIndex and tablet data are currently stored in the same directory
        // We may need to support the separation of PersistentIndex and Tablet data
        DCHECK(_persistent_index == nullptr);
        _persistent_index = std::make_shared<PersistentIndex>(tablet->schema_hash_path());
        return _persistent_index->load_from_tablet(tablet);
    }
    return Status::OK();
}

Status PrimaryIndex::_build_persistent_values(uint32_t rssid, uint32_t rowid_start, uint32_t idx_begin,
                                              uint32_t idx_end, std::vector<uint64_t>* values) const {
    uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (uint32_t i = idx_begin; i < idx_end; i++) {
        values->emplace_back(base + i);
    }
    return Status::OK();
}

Status PrimaryIndex::_build_persistent_values(uint32_t rssid, const vector<uint32_t>& rowids, uint32_t idx_begin,
                                              uint32_t idx_end, std::vector<uint64_t>* values) const {
    DCHECK(idx_end <= rowids.size());
    uint64_t base = ((uint64_t)rssid) << 32;
    for (uint32_t i = idx_begin; i < idx_end; i++) {
        values->emplace_back(base + rowids[i]);
    }
    return Status::OK();
}

const Slice* PrimaryIndex::_build_persistent_keys(const Column& pks, uint32_t idx_begin, uint32_t idx_end,
                                                  std::vector<Slice>* key_slices) const {
    if (pks.is_binary() || pks.is_large_binary()) {
        const Slice* vkeys = reinterpret_cast<const Slice*>(pks.raw_data());
        return vkeys + idx_begin;
    } else {
        DCHECK(_key_size > 0);
        const uint8_t* keys = pks.raw_data() + idx_begin * _key_size;
        for (size_t i = idx_begin; i < idx_end; i++) {
            key_slices->emplace_back(keys, _key_size);
            keys += _key_size;
        }
        return reinterpret_cast<const Slice*>(key_slices->data());
    }
}

Status PrimaryIndex::_insert_into_persistent_index(uint32_t rssid, const vector<uint32_t>& rowids, const Column& pks) {
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowids, 0, pks.size(), &values));
    const Slice* vkeys = _build_persistent_keys(pks, 0, pks.size(), &keys);
    RETURN_IF_ERROR(_persistent_index->insert(pks.size(), vkeys, reinterpret_cast<IndexValue*>(values.data()), true));
    return Status::OK();
}

Status PrimaryIndex::_upsert_into_persistent_index(uint32_t rssid, uint32_t rowid_start, const Column& pks,
                                                   uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes,
                                                   IOStat* stat) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    Status st;
    uint32_t n = idx_end - idx_begin;
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(n);
    std::vector<uint64_t> old_values(n, NullIndexValue);
    const Slice* vkeys = _build_persistent_keys(pks, idx_begin, idx_end, &keys);
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, idx_begin, idx_end, &values));
    RETURN_IF_ERROR(_persistent_index->upsert(n, vkeys, reinterpret_cast<IndexValue*>(values.data()),
                                              reinterpret_cast<IndexValue*>(old_values.data()), stat));
    for (unsigned long old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
    return st;
}

Status PrimaryIndex::_erase_persistent_index(const Column& key_col, DeletesMap* deletes) {
    Status st;
    std::vector<Slice> keys;
    std::vector<uint64_t> old_values(key_col.size(), NullIndexValue);
    const Slice* vkeys = _build_persistent_keys(key_col, 0, key_col.size(), &keys);
    st = _persistent_index->erase(key_col.size(), vkeys, reinterpret_cast<IndexValue*>(old_values.data()));
    if (!st.ok()) {
        LOG(WARNING) << "erase persistent index failed";
    }
    for (unsigned long old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
    return st;
}

Status PrimaryIndex::_get_from_persistent_index(const Column& key_col, std::vector<uint64_t>* rowids) const {
    std::vector<Slice> keys;
    const Slice* vkeys = _build_persistent_keys(key_col, 0, key_col.size(), &keys);
    Status st = _persistent_index->get(key_col.size(), vkeys, reinterpret_cast<IndexValue*>(rowids->data()));
    if (!st.ok()) {
        LOG(WARNING) << "failed get value from persistent index";
    }
    return st;
}

[[maybe_unused]] Status PrimaryIndex::_replace_persistent_index(uint32_t rssid, uint32_t rowid_start, const Column& pks,
                                                                const vector<uint32_t>& src_rssid,
                                                                vector<uint32_t>* deletes) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, 0, pks.size(), &values));
    Status st = _persistent_index->try_replace(pks.size(), _build_persistent_keys(pks, 0, pks.size(), &keys),
                                               reinterpret_cast<IndexValue*>(values.data()), src_rssid, deletes);
    if (!st.ok()) {
        LOG(WARNING) << "try replace persistent index failed";
    }
    return st;
}

Status PrimaryIndex::_replace_persistent_index(uint32_t rssid, uint32_t rowid_start, const Column& pks,
                                               const uint32_t max_src_rssid, vector<uint32_t>* deletes) {
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, 0, pks.size(), &values));
    Status st = _persistent_index->try_replace(pks.size(), _build_persistent_keys(pks, 0, pks.size(), &keys),
                                               reinterpret_cast<IndexValue*>(values.data()), max_src_rssid, deletes);
    if (!st.ok()) {
        LOG(WARNING) << "try replace persistent index failed";
    }
    return st;
}

Status PrimaryIndex::insert(uint32_t rssid, const vector<uint32_t>& rowids, const Column& pks) {
    DCHECK(_status.ok() && (_persistent_index));
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        return _insert_into_persistent_index(rssid, rowids, pks);
}

Status PrimaryIndex::insert(uint32_t rssid, uint32_t rowid_start, const Column& pks) {
    vector<uint32_t> rids(pks.size());
    for (int i = 0; i < rids.size(); i++) {
        rids[i] = rowid_start + i;
    }
    return insert(rssid, rids, pks);
}

Status PrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, DeletesMap* deletes,
                            IOStat* stat) {
    DCHECK(_status.ok() && (_persistent_index));
        return _upsert_into_persistent_index(rssid, rowid_start, pks, 0, pks.size(), deletes, stat);
}

Status PrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t idx_begin,
                            uint32_t idx_end, DeletesMap* deletes) {
    DCHECK(_status.ok() && (_persistent_index));
        return _upsert_into_persistent_index(rssid, rowid_start, pks, idx_begin, idx_end, deletes, nullptr);
}

Status PrimaryIndex::_replace_persistent_index_by_indexes(uint32_t rssid, uint32_t rowid_start,
                                                          const std::vector<uint32_t>& replace_indexes,
                                                          const Column& pks) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, 0, pks.size(), &values));
    Status st = _persistent_index->replace(pks.size(), _build_persistent_keys(pks, 0, pks.size(), &keys),
                                           reinterpret_cast<IndexValue*>(values.data()), replace_indexes);
    if (!st.ok()) {
        LOG(WARNING) << "try replace persistent index failed";
    }
    return st;
}

Status PrimaryIndex::replace(uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                             const Column& pks) {
    DCHECK(_status.ok() && (_persistent_index));
        return _replace_persistent_index_by_indexes(rssid, rowid_start, replace_indexes, pks);
}

[[maybe_unused]] Status PrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks,
                                                  const vector<uint32_t>& src_rssid, vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && (_persistent_index));
        return _replace_persistent_index(rssid, rowid_start, pks, src_rssid, deletes);
}

Status PrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks, const uint32_t max_src_rssid,
                                 vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && (_persistent_index));
        return _replace_persistent_index(rssid, rowid_start, pks, max_src_rssid, deletes);
}

Status PrimaryIndex::erase(const Column& key_col, DeletesMap* deletes) {
    DCHECK(_status.ok() && (_persistent_index));
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        return _erase_persistent_index(key_col, deletes);
}

Status PrimaryIndex::get(const Column& key_col, std::vector<uint64_t>* rowids) const {
    DCHECK(_status.ok() && (_persistent_index));
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        return _get_from_persistent_index(key_col, rowids);
}

std::size_t PrimaryIndex::memory_usage() const {
    if (_persistent_index) {
        return _persistent_index->memory_usage();
    }
    return _memory_usage.load();
}

std::size_t PrimaryIndex::size() const {
        return _persistent_index->size();
}

void PrimaryIndex::reserve(size_t s) {
}

std::string PrimaryIndex::to_string() const {
    return strings::Substitute("PrimaryIndex tablet:$0", _tablet_id);
}

std::unique_ptr<PrimaryIndex> TEST_create_primary_index(const Schema& pk_schema) {
    return std::make_unique<PrimaryIndex>(pk_schema);
}

Status PrimaryIndex::major_compaction(DataDir* data_dir, int64_t tablet_id, std::shared_timed_mutex* mutex) {
    // `_persistent_index` could be reset when call `unload()`, so we need to fetch reference first.
    std::shared_ptr<PersistentIndex> pindex;
    {
        std::lock_guard<std::mutex> lg(_lock);
        pindex = _persistent_index;
    }
    if (pindex != nullptr) {
        return pindex->major_compaction(data_dir, tablet_id, mutex);
    } else {
        return Status::OK();
    }
}

Status PrimaryIndex::reset(Tablet* tablet, EditVersion version, PersistentIndexMetaPB* index_meta) {
    std::lock_guard<std::mutex> lg(_lock);
    _table_id = tablet->belonged_table_id();
    _tablet_id = tablet->tablet_id();
    const TabletSchemaCSPtr tablet_schema_ptr = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema_ptr->num_key_columns());
    for (auto i = 0; i < tablet_schema_ptr->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema_ptr, pk_columns);
    _set_schema(pkey_schema);

        if (_persistent_index != nullptr) {
            _persistent_index.reset();
        }
        _persistent_index = std::make_shared<PersistentIndex>(tablet->schema_hash_path());
        RETURN_IF_ERROR(_persistent_index->reset(tablet, version, index_meta));
    _loaded = true;
    _calc_memory_usage();

    return Status::OK();
}

void PrimaryIndex::reset_cancel_major_compaction() {
    if (_persistent_index != nullptr) {
        _persistent_index->reset_cancel_major_compaction();
    }
}

void PrimaryIndex::_calc_memory_usage() {
}

} // namespace starrocks
