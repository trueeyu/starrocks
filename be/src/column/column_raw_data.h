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

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "fmt/format.h"

namespace starrocks {

// MutableRawDataVisitor dispatches to the concrete Column type via the visitor
// mechanism and calls mutable_raw_data() as a direct member function on the
// resolved concrete type, avoiding virtual dispatch for final column classes.
class MutableRawDataVisitor final : public ColumnVisitorMutableAdapter<MutableRawDataVisitor> {
    using Base = ColumnVisitorMutableAdapter<MutableRawDataVisitor>;

public:
    MutableRawDataVisitor() : Base(this) {}

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        _result = column->mutable_raw_data();
        return Status::OK();
    }

    Status do_visit(NullableColumn* column) { return column->data_column_raw_ptr()->accept_mutable(this); }

    Status do_visit(ArrayColumn* column) { return column->elements_column_raw_ptr()->accept_mutable(this); }

    Status do_visit(ConstColumn* column) { return column->data_column_raw_ptr()->accept_mutable(this); }

    // All other column types are not supported.
    template <typename T>
    Status do_visit(T* column) {
        return Status::NotSupported(
                fmt::format("mutable_raw_data not supported for column type: {}", column->get_name()));
    }

    uint8_t* result() const { return _result; }

private:
    uint8_t* _result = nullptr;
};

inline StatusOr<uint8_t*> column_mutable_raw_data(Column* column) {
    MutableRawDataVisitor visitor;
    RETURN_IF_ERROR(column->accept_mutable(&visitor));
    return visitor.result();
}

} // namespace starrocks