// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "storage/compaction_task.h"

#include <sstream>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "storage/compaction_manager.h"
#include "storage/storage_engine.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

CompactionTask::~CompactionTask() {
    if (_mem_tracker) {
        delete _mem_tracker;
    }
}

void CompactionTask::run() {
    LOG(INFO) << "start compaction. task_id:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id
              << ", algorithm:" << CompactionUtils::compaction_algorithm_to_string(_task_info.algorithm)
              << ", compaction_type:" << starrocks::to_string(_task_info.compaction_type)
              << ", compaction_score:" << _task_info.compaction_score
              << ", output_version:" << _task_info.output_version << ", input rowsets size:" << _input_rowsets.size();
}

bool CompactionTask::should_stop() const {
    return StorageEngine::instance()->bg_worker_stopped() || BackgroundTask::should_stop();
}

void CompactionTask::_success_callback() {
    set_compaction_task_state(COMPACTION_SUCCESS);
    // for compatible, update compaction time
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(UnixMillis());
        _tablet->set_last_cumu_compaction_failure_status(TStatusCode::OK);
        if (_tablet->cumulative_layer_point() == _input_rowsets.front()->start_version()) {
            _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
        }
    } else {
        _tablet->set_last_base_compaction_success_time(UnixMillis());
    }

    // for compatible
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        StarRocksMetrics::instance()->cumulative_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->cumulative_compaction_bytes_total.increment(_task_info.input_rowsets_size);
    } else {
        StarRocksMetrics::instance()->base_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->base_compaction_bytes_total.increment(_task_info.input_rowsets_size);
    }

    // preload the rowset
    // warm-up this rowset
    auto st = _output_rowset->load();
    if (!st.ok()) {
        // only log load failure
        LOG(WARNING) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                     << ", rowset:" << _output_rowset->rowset_id() << ", status:" << st;
    }
}

void CompactionTask::_failure_callback(const Status& st) {
    set_compaction_task_state(COMPACTION_FAILED);
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_failure_time(UnixMillis());
        _tablet->set_last_cumu_compaction_failure_status(st.code());
    } else {
        _tablet->set_last_base_compaction_failure_time(UnixMillis());
    }
    LOG(WARNING) << "compaction task:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id << " failed.";
}

void CompactionTask::_commit_compaction() {
    std::stringstream input_stream_info;
    {
        std::unique_lock wrlock(_tablet->get_header_lock());
        for (int i = 0; i < 5 && i < _input_rowsets.size(); ++i) {
            input_stream_info << _input_rowsets[i]->version() << ";";
        }
        if (_input_rowsets.size() > 5) {
            input_stream_info << ".." << (*_input_rowsets.rbegin())->version();
        }
        std::vector<RowsetSharedPtr> to_replace;
        _tablet->modify_rowsets({_output_rowset}, _input_rowsets, &to_replace);
        _tablet->save_meta();
        Rowset::close_rowsets(_input_rowsets);
        for (auto& rs : to_replace) {
            StorageEngine::instance()->add_unused_rowset(rs);
        }
    }
    VLOG(1) << "commit compaction. output version:" << _task_info.output_version
            << ", output rowset version:" << _output_rowset->version() << ", input rowsets:" << input_stream_info.str()
            << ", input rowsets size:" << _input_rowsets.size()
            << ", max_version:" << _tablet->max_continuous_version();
}

} // namespace starrocks
