/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "Adaptor.hh"

#include <sstream>

namespace orc {
#ifdef HAS_DOUBLE_TO_STRING
std::string to_string(double val) {
    return std::to_string(val);
}
#else
std::string to_string(double val) {
    return std::to_string(static_cast<long double>(val));
}
#endif

#ifdef HAS_INT64_TO_STRING
std::string to_string(int64_t val) {
    return std::to_string(val);
}
#else
std::string to_string(int64_t val) {
    return std::to_string(static_cast<long long int>(val));
}
#endif
} // namespace orc
