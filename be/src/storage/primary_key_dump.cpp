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

#include "storage/primary_key_dump.h"

#include <memory>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/type_traits.h"
#include "types/logical_type.h"

namespace starrocks {

PrimaryKeyDump::PrimaryKeyDump(Tablet* tablet) {
    _tablet = tablet;
    _dump_filepath = tablet->data_dir()->get_tmp_path() + "/" + std::to_string(_tablet->tablet_id()) + ".pkdump";
    _partial_pindex_kvs = std::make_unique<PartialKVsPB>();
}

// for UT only
PrimaryKeyDump::PrimaryKeyDump(const std::string& dump_filepath) {
    _dump_filepath = dump_filepath;
    _partial_pindex_kvs = std::make_unique<PartialKVsPB>();
}

Status PrimaryKeyDump::add_pindex_kvs(const std::string_view& key, uint64_t value, PrimaryIndexDumpPB* dump_pb) {
    // Avoid protobuf exceed memory limit
    if (_partial_pindex_kvs_bytes + key.size() + sizeof(uint64_t) >= MAX_PROTOBUF_SIZE) {
        std::string serialized_data;
        if (_partial_pindex_kvs->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_kvs()->CopyFrom(page);
            _partial_pindex_kvs->Clear();
            _partial_pindex_kvs_bytes = 0;
        } else {
            return Status::InternalError("dump to file, serialize error");
        }
    }
    _partial_pindex_kvs->add_keys(key.data(), key.size());
    _partial_pindex_kvs->add_values(value);
    _partial_pindex_kvs_bytes += key.size() + sizeof(uint64_t);
    return Status::OK();
}

Status PrimaryKeyDump::finish_pindex_kvs(PrimaryIndexDumpPB* dump_pb) {
    std::string serialized_data;
    if (_partial_pindex_kvs_bytes > 0) {
        if (_partial_pindex_kvs->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_kvs()->CopyFrom(page);
            _partial_pindex_kvs->Clear();
            _partial_pindex_kvs_bytes = 0;
        } else {
            return Status::InternalError("dump to file, serialize error");
        }
    }
    return Status::OK();
}

class PrimaryKeyChunkDumper {
public:
    PrimaryKeyChunkDumper(PrimaryKeyColumnPB* pk_column_pb) : _pk_column_pb(pk_column_pb) {}
    ~PrimaryKeyChunkDumper() { (void)fs::delete_file(_tmp_file); }
    Status init(const TabletSchemaCSPtr& tablet_schema, const std::string& tablet_path) {
        _tmp_file = tablet_path + "/PrimaryKeyChunkDumper_" + std::to_string(static_cast<int64_t>(pthread_self()));
        (void)fs::delete_file(_tmp_file);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(tablet_path));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, _tmp_file));
        SegmentWriterOptions writer_options;
        _writer = std::make_unique<SegmentWriter>(std::move(wfile), _pk_column_pb->segment_id(), tablet_schema,
                                                  writer_options);
        RETURN_IF_ERROR(_writer->init(false));
        return Status::OK();
    }
    Status dump_chunk(const Chunk& chunk) { return _writer->append_chunk(chunk); }
    Status finalize(WritableFile* wfile) {
        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_writer->finalize(&segment_file_size, &index_size, &footer_position));
        _page.set_offset(wfile->size());
        RETURN_IF_ERROR(fs::copy_append_file(_tmp_file, wfile));
        _page.set_size(wfile->size() - _page.offset());
        _pk_column_pb->mutable_page()->CopyFrom(_page);
        return Status::OK();
    }

private:
    PrimaryKeyColumnPB* _pk_column_pb;
    std::unique_ptr<SegmentWriter> _writer;
    PagePointerPB _page;
    std::string _tmp_file;
};

class PrimaryKeyChunkReader {
public:
    PrimaryKeyChunkReader() = default;
    ~PrimaryKeyChunkReader() { (void)fs::delete_file(_tmp_file); }

    StatusOr<ChunkIteratorPtr> read(const std::string& dump_filepath, const Schema& schema,
                                    const TabletSchemaCSPtr& tablet_schema, const PrimaryKeyColumnPB& pk_column_pb) {
        RETURN_IF_ERROR(_copy_to_tmp_file(dump_filepath, pk_column_pb));
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_tmp_file));
        SegmentReadOptions seg_options;
        seg_options.fs = fs;
        seg_options.stats = &_stats;
        seg_options.tablet_schema = tablet_schema;
        ASSIGN_OR_RETURN(auto seg_ptr,
                         Segment::open(fs, FileInfo{_tmp_file}, pk_column_pb.segment_id(), tablet_schema));
        return seg_ptr->new_iterator(schema, seg_options);
    }

private:
    Status _copy_to_tmp_file(const std::string& dump_filepath, const PrimaryKeyColumnPB& pk_column_pb) {
        _tmp_file = "./PrimaryKeyChunkReader_" + std::to_string(static_cast<int64_t>(pthread_self()));
        (void)fs::delete_file(_tmp_file);
        RETURN_IF_ERROR(fs::copy_file_by_range(dump_filepath, _tmp_file, pk_column_pb.page().offset(),
                                               pk_column_pb.page().size()));
        return Status::OK();
    }

private:
    std::string _tmp_file;
    OlapReaderStatistics _stats;
};

static std::pair<Schema, std::shared_ptr<TabletSchema>> build_pkey_schema(const TabletSchemaCSPtr& tablet_schema) {
    vector<uint32_t> pk_columns;
    vector<int32_t> pk_columns2;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
        pk_columns2.push_back((int32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    auto pkey_tschema = TabletSchema::create(tablet_schema, pk_columns2);
    return {pkey_schema, pkey_tschema};
}

} // namespace starrocks