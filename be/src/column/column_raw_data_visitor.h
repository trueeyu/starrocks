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
#include "column/binary_column.h"
#include "column/column_visitor.h"
#include "column/const_column.h"
#include "column/fixed_length_column_base.h"
#include "column/nullable_column.h"

namespace starrocks {

// RawDataVisitor replaces Column::raw_data() for callers that hold a Column*.
// After raw_data() is removed from the Column base class, use this visitor instead:
//
//   RawDataVisitor visitor;
//   RETURN_IF_ERROR(column->accept(&visitor));
//   const uint8_t* ptr = visitor.result();
//
// Semantics per column type:
//   FixedLengthColumnBase<T>  -> pointer to the contiguous T[] backing array
//   BinaryColumnBase<SizeT>   -> pointer to the internal Slice[] cache (built lazily)
//   NullableColumn             -> raw data of the inner data column (null flags excluded)
//   ConstColumn                -> raw data of the single-element data column
//   ArrayColumn                -> raw data of the elements column
//   ObjectColumn / StructColumn / MapColumn -> not supported (returns error)
class RawDataVisitor : public ColumnVisitor {
public:
    // Returns the raw data pointer set by the last successful accept() call.
    // Undefined if the last accept() returned a non-OK status.
    const uint8_t* result() const { return _result; }

    // --- FixedLengthColumnBase overrides ---

    Status visit(const Int8Column& column) override { return _visit_fixed(column); }
    Status visit(const UInt8Column& column) override { return _visit_fixed(column); }
    Status visit(const Int16Column& column) override { return _visit_fixed(column); }
    Status visit(const UInt16Column& column) override { return _visit_fixed(column); }
    Status visit(const Int32Column& column) override { return _visit_fixed(column); }
    Status visit(const UInt32Column& column) override { return _visit_fixed(column); }
    Status visit(const Int64Column& column) override { return _visit_fixed(column); }
    Status visit(const UInt64Column& column) override { return _visit_fixed(column); }
    Status visit(const Int128Column& column) override { return _visit_fixed(column); }
    Status visit(const FloatColumn& column) override { return _visit_fixed(column); }
    Status visit(const DoubleColumn& column) override { return _visit_fixed(column); }
    Status visit(const DateColumn& column) override { return _visit_fixed(column); }
    Status visit(const TimestampColumn& column) override { return _visit_fixed(column); }
    Status visit(const DecimalColumn& column) override { return _visit_fixed(column); }
    Status visit(const Decimal32Column& column) override { return _visit_fixed(column); }
    Status visit(const Decimal64Column& column) override { return _visit_fixed(column); }
    Status visit(const Decimal128Column& column) override { return _visit_fixed(column); }
    Status visit(const Decimal256Column& column) override { return _visit_fixed(column); }
    Status visit(const FixedLengthColumn<int96_t>& column) override { return _visit_fixed(column); }
    Status visit(const FixedLengthColumn<uint24_t>& column) override { return _visit_fixed(column); }
    Status visit(const FixedLengthColumn<decimal12_t>& column) override { return _visit_fixed(column); }

    // --- BinaryColumn overrides ---

    Status visit(const BinaryColumn& column) override { return _visit_binary(column); }
    Status visit(const LargeBinaryColumn& column) override { return _visit_binary(column); }

    // --- Wrapper column overrides ---

    Status visit(const NullableColumn& column) override {
        return column.data_column()->accept(this);
    }

    Status visit(const ConstColumn& column) override {
        return column.data_column()->accept(this);
    }

    Status visit(const ArrayColumn& column) override {
        return column.elements_column()->accept(this);
    }

    // --- Unsupported types ---

    Status visit(const MapColumn& /*column*/) override {
        return Status::NotSupported("MapColumn does not support raw_data");
    }

    Status visit(const StructColumn& /*column*/) override {
        return Status::NotSupported("StructColumn does not support raw_data");
    }

    Status visit(const HyperLogLogColumn& /*column*/) override {
        return Status::NotSupported("HyperLogLogColumn does not support raw_data");
    }

    Status visit(const BitmapColumn& /*column*/) override {
        return Status::NotSupported("BitmapColumn does not support raw_data");
    }

    Status visit(const PercentileColumn& /*column*/) override {
        return Status::NotSupported("PercentileColumn does not support raw_data");
    }

    Status visit(const JsonColumn& /*column*/) override {
        return Status::NotSupported("JsonColumn does not support raw_data");
    }

    Status visit(const VariantColumn& /*column*/) override {
        return Status::NotSupported("VariantColumn does not support raw_data");
    }

private:
    template <typename T>
    Status _visit_fixed(const FixedLengthColumnBase<T>& column) {
        _result = reinterpret_cast<const uint8_t*>(column.immutable_data().data());
        return Status::OK();
    }

    template <typename SizeT>
    Status _visit_binary(const BinaryColumnBase<SizeT>& column) {
        // BinaryColumnBase::raw_data() lazily builds an internal Slice[] cache and returns
        // a pointer to it. The concrete class still owns that cache, so the returned pointer
        // is valid as long as the column is alive and unmodified.
        _result = column.raw_data();
        return Status::OK();
    }

    const uint8_t* _result = nullptr;
};

} // namespace starrocks