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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/checksum_action.cpp

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

#include "http/action/checksum_action.h"

#include <sstream>
#include <string>

#include "boost/lexical_cast.hpp"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/task/engine_checksum_task.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {

const std::string TABLET_ID = "tablet_id";
// do not use name "VERSION",
// or will be conflict with "VERSION" in thrift/config.h
const std::string TABLET_VERSION = "version";
const std::string SCHEMA_HASH = "schema_hash";

void ChecksumAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request ";

    for (size_t i = 0; i < 1; i++) {
        std::string tmp_str = "arena." + std::to_string(i) + ".destroy2";
        int v1 = je_mallctl(tmp_str.c_str(), nullptr, nullptr, nullptr, 0);
        std::cout << "TMP_RESULT:" << tmp_str << ":" << v1 << std::endl;
    }

    LOG(INFO) << "deal with checksum request finished! tablet id: ";
}

int64_t ChecksumAction::_do_checksum(int64_t tablet_id, int64_t version) {
    MemTracker* mem_tracker = GlobalEnv::GetInstance()->consistency_mem_tracker();
    Status check_limit_st = mem_tracker->check_mem_limit("Start consistency check.");
    if (!check_limit_st.ok()) {
        LOG(WARNING) << "checksum failed: " << check_limit_st.message();
        return -1L;
    }

    Status res = Status::OK();
    uint32_t checksum;
    EngineChecksumTask engine_task(mem_tracker, tablet_id, version, &checksum);
    res = engine_task.execute();
    if (!res.ok()) {
        LOG(WARNING) << "checksum failed. status: " << res << ", signature: " << tablet_id;
        return -1L;
    } else {
        LOG(INFO) << "checksum success. status: " << res << ", signature: " << tablet_id << ". checksum: " << checksum;
    }

    return static_cast<int64_t>(checksum);
}

} // end namespace starrocks
