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

// TEMPORARY DIAGNOSTIC -- NOT FOR MERGE.
//
// Validate a chunk's columns at the point it changes hands, so a structurally broken column is
// attributed to whoever produced it. Nothing checks this in a RELEASE build: Chunk::check_or_die()
// compiles to a no-op under NDEBUG and every down_cast on the read path is unchecked, so a broken
// column survives into an expression and faults there -- e.g. char_length() dereferencing a Slice
// whose base is null because the offsets were written but the byte buffer never was.
//
// BinaryColumnBase::check_or_die() uses CHECK_EQ and still aborts in RELEASE, which is the check
// that matters here. The looser invariants are only logged: a false abort would be worse than a
// missed one.

#include <cstdint>
#include <string_view>

#include "column/chunk.h"
#include "column/column.h"
#include "column/field.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/logging.h"
#include "gutil/casts.h"

namespace starrocks::debug_probe {

inline void validate_chunk(const Chunk* chunk, std::string_view where, int64_t id) {
    if (chunk == nullptr) {
        return;
    }
    const size_t num_rows = chunk->num_rows();
    const auto& schema = chunk->schema();
    for (size_t i = 0; i < chunk->num_columns(); ++i) {
        const ColumnPtr& column = chunk->get_column_by_index(i);
        const std::string_view name = (schema != nullptr && i < schema->num_fields())
                                              ? schema->field(i)->name()
                                              : std::string_view("?");

        if (!column->is_constant() && column->size() != num_rows) {
            LOG(ERROR) << "[" << where << "] column size != chunk rows, id=" << id << " index=" << i
                       << " column=" << name << " column_type=" << column->get_name()
                       << " column_size=" << column->size() << " chunk_rows=" << num_rows;
        }
        if (column->is_nullable()) {
            const auto* nullable = down_cast<const NullableColumn*>(column.get());
            if (nullable->data_column()->size() != nullable->null_column()->size()) {
                LOG(ERROR) << "[" << where << "] nullable data/null size mismatch, id=" << id << " index=" << i
                           << " column=" << name << " data_size=" << nullable->data_column()->size()
                           << " null_size=" << nullable->null_column()->size();
            }
        }
        // Aborts here, at the producer, when the byte buffer does not match the offsets.
        column->check_or_die();
    }
}

} // namespace starrocks::debug_probe
