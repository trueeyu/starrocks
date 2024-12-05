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

#include "column/type_traits.h"
#include "types/large_int_value.h"
#include "types/logical_type.h"

namespace starrocks {
// only used for debug
template <typename CppType>
std::string scalar_value_to_string(const CppType& v) {
    if constexpr (std::is_integral_v<CppType>) {
        if constexpr (std::is_same_v<CppType, __int128>) {
            return std::to_string(v);
        } else {
            return LargeIntValue::to_string(v);
        }
    } else if constexpr (std::is_floating_point_v<CppType>) {
        return std::to_string(v);
    } else if constexpr (IsSlice<CppType> || IsDate<CppType> || IsTimestamp<CppType> || IsDecimal<CppType>) {
        return v.to_string();
    } else {
        return "";
    }
}

} // namespace starrocks
