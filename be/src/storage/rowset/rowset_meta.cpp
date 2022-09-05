// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/rowset/rowset_meta.h"

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

RowsetMeta::RowsetMeta(std::string_view pb_rowset_meta, bool* parsed) {
    *parsed = _init(pb_rowset_meta);
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), mem_usage());
}

RowsetMeta::RowsetMeta(const RowsetMetaPB& rowset_meta_pb) {
    _init_from_pb(rowset_meta_pb);
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), mem_usage());
}

RowsetMeta::~RowsetMeta() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), mem_usage());
}

bool RowsetMeta::_init(std::string_view pb_rowset_meta) {
    int64_t old_usage = mem_usage();
    bool ret = _deserialize_from_pb(pb_rowset_meta);
    if (!ret) {
        return false;
    }
    _init();
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), mem_usage() - old_usage);
    return true;
}

} // namespace starrocks