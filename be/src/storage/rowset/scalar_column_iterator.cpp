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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.cpp

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

#include "storage/rowset/scalar_column_iterator.h"

#include "common/status.h"
#include "storage/column_predicate.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/dict_page.h"
#include "storage/rowset/encoding_info.h"
#include "types/logical_type.h"
#include "util/bitmap.h"

namespace starrocks {

ScalarColumnIterator::ScalarColumnIterator(ColumnReader* reader) : _reader(reader) {}

ScalarColumnIterator::~ScalarColumnIterator() = default;

Status ScalarColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;

    IndexReadOptions index_opts;
    index_opts.use_page_cache = !opts.temporary_data && opts.use_page_cache && !config::disable_storage_page_cache &&
                                config::enable_ordinal_index_memory_page_cache;
    index_opts.lake_io_opts = opts.lake_io_opts;
    index_opts.read_file = _opts.read_file;
    index_opts.stats = _opts.stats;
    RETURN_IF_ERROR(_reader->load_ordinal_index(index_opts));
    _opts.stats->total_columns_data_page_count += _reader->num_data_pages();

    if (_reader->encoding_info()->encoding() != DICT_ENCODING) {
        return Status::OK();
    }
    LogicalType column_type = delegate_type(_reader->column_type());
    DCHECK(supports_dict_encoding(column_type))
            << strings::Substitute("dict encoding with unsupported $0 field type", column_type);
    switch (column_type) {
    case TYPE_CHAR:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_CHAR>;
        break;
    case TYPE_VARCHAR:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_VARCHAR>;
        break;
    case TYPE_SMALLINT:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_SMALLINT>;
        break;
    case TYPE_INT:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_INT>;
        break;
    case TYPE_BIGINT:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_BIGINT>;
        break;
    case TYPE_LARGEINT:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_LARGEINT>;
        break;
    case TYPE_FLOAT:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_FLOAT>;
        break;
    case TYPE_DOUBLE:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_DOUBLE>;
        break;
    case TYPE_DATE:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_DATE>;
        break;
    case TYPE_DATETIME:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_DATETIME>;
        break;
    case TYPE_DECIMALV2:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_DECIMALV2>;
        break;
    case TYPE_JSON:
        _init_dict_decoder_func = &ScalarColumnIterator::_do_init_dict_decoder<TYPE_JSON>;
        break;
    default:
        return Status::NotSupported(strings::Substitute("dict encoding with unsupported $0 field type", column_type));
    }

    // TODO: The following logic is primarily used for optimizing queries for VARCHAR/CHAR types during
    // dictionary encoding. Can we also optimize queries for non-TYPE_VARCHAR types?
    if (column_type != TYPE_VARCHAR && column_type != TYPE_CHAR && column_type != TYPE_JSON) {
        return Status::OK();
    }
    if (opts.check_dict_encoding) {
        if (_reader->has_all_dict_encoded()) {
            _all_dict_encoded = _reader->all_dict_encoded();
            // if _all_dict_encoded is true, load dictionary page into memory for `dict_lookup`.
            RETURN_IF(!_all_dict_encoded, Status::OK());
            if (column_type == TYPE_VARCHAR) {
                RETURN_IF_ERROR(_load_dict_page<TYPE_VARCHAR>());
            } else if (column_type == TYPE_JSON) {
                RETURN_IF_ERROR(_load_dict_page<TYPE_JSON>());
            } else {
                RETURN_IF_ERROR(_load_dict_page<TYPE_CHAR>());
            }
        } else if (_reader->num_rows() > 0) {
            // old version segment file dost not have `all_dict_encoded`, in order to check
            // whether all data pages are using dict encoding, must load the last data page
            // and check its encoding.
            ordinal_t last_row = _reader->num_rows() - 1;
            RETURN_IF_ERROR(seek_to_ordinal(last_row));
            _all_dict_encoded = _page->encoding_type() == DICT_ENCODING;
        }
    }

    if (_all_dict_encoded && column_type == TYPE_CHAR) {
        _decode_dict_codes_func = &ScalarColumnIterator::_do_decode_dict_codes<TYPE_CHAR>;
        _dict_lookup_func = &ScalarColumnIterator::_do_dict_lookup<TYPE_CHAR>;
        _next_dict_codes_func = &ScalarColumnIterator::_do_next_dict_codes<TYPE_CHAR>;
        _next_batch_dict_codes_func = &ScalarColumnIterator::_do_next_batch_dict_codes<TYPE_CHAR>;
        _fetch_all_dict_words_func = &ScalarColumnIterator::_fetch_all_dict_words<TYPE_CHAR>;
    } else if (_all_dict_encoded && column_type == TYPE_VARCHAR) {
        _decode_dict_codes_func = &ScalarColumnIterator::_do_decode_dict_codes<TYPE_VARCHAR>;
        _dict_lookup_func = &ScalarColumnIterator::_do_dict_lookup<TYPE_VARCHAR>;
        _next_dict_codes_func = &ScalarColumnIterator::_do_next_dict_codes<TYPE_VARCHAR>;
        _next_batch_dict_codes_func = &ScalarColumnIterator::_do_next_batch_dict_codes<TYPE_VARCHAR>;
        _fetch_all_dict_words_func = &ScalarColumnIterator::_fetch_all_dict_words<TYPE_VARCHAR>;
    } else if (_all_dict_encoded && column_type == TYPE_JSON) {
        _decode_dict_codes_func = &ScalarColumnIterator::_do_decode_dict_codes<TYPE_JSON>;
        _dict_lookup_func = &ScalarColumnIterator::_do_dict_lookup<TYPE_JSON>;
        _next_dict_codes_func = &ScalarColumnIterator::_do_next_dict_codes<TYPE_JSON>;
        _next_batch_dict_codes_func = &ScalarColumnIterator::_do_next_batch_dict_codes<TYPE_JSON>;
        _fetch_all_dict_words_func = &ScalarColumnIterator::_fetch_all_dict_words<TYPE_JSON>;
    }
    return Status::OK();
}

bool ScalarColumnIterator::is_nullable() {
    return _reader->is_nullable();
}

Status ScalarColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));
    RETURN_IF_ERROR(_read_data_page(_page_iter));

    RETURN_IF_ERROR(_seek_to_pos_in_page(_page.get(), 0));
    _current_ordinal = 0;
    return Status::OK();
}

Status ScalarColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord)) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    RETURN_IF_ERROR(_seek_to_pos_in_page(_page.get(), ord - _page->first_ordinal()));
    _current_ordinal = ord;
    return Status::OK();
}

Status ScalarColumnIterator::seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord)) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    _array_size.resize(0);
    _element_ordinal = static_cast<int64_t>(_page->corresponding_element_ordinal());
    _current_ordinal = _page->first_ordinal();
    RETURN_IF_ERROR(_seek_to_pos_in_page(_page.get(), 0));
    size_t size_to_read = ord - _current_ordinal;
    RETURN_IF_ERROR(_page->read(&_array_size, &size_to_read));
    _current_ordinal += size_to_read;
    CHECK_EQ(ord, _current_ordinal);
    for (auto e : _array_size.get_data()) {
        _element_ordinal += e;
    }
    return Status::OK();
}

Status ScalarColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) {
    if (page->offset() == offset_in_page) {
        // fast path, do nothing
        return Status::OK();
    }
    return page->seek(offset_in_page);
}

Status ScalarColumnIterator::next_batch(size_t* n, Column* dst) {
    size_t remaining = *n;
    size_t prev_bytes = dst->byte_size();
    bool contain_deleted_row = (dst->delete_state() != DEL_NOT_SATISFIED);
    while (remaining > 0) {
        if (_page->remaining() == 0) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                // release shareBufferStream
                if (config::io_coalesce_lake_read_enable && _opts.is_io_coalesce) {
                    auto shared_buffer_stream = dynamic_cast<io::SharedBufferedInputStream*>(_opts.read_file);
                    if (shared_buffer_stream != nullptr) {
                        shared_buffer_stream->release();
                    }
                }
                break;
            }
        }

        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        // number of rows to be read from this page
        size_t nread = remaining;
        RETURN_IF_ERROR(_page->read(dst, &nread));
        _current_ordinal += nread;
        remaining -= nread;
    }
    dst->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    *n -= remaining;
    _opts.stats->bytes_read += static_cast<int64_t>(dst->byte_size() - prev_bytes);
    return Status::OK();
}

Status ScalarColumnIterator::null_count(size_t* count) {
    if (!_reader->is_nullable()) {
        *count = 0;
        return Status::OK();
    }
    bool eos = false;
    while (!eos) {
        *count += _page->read_null_count();
        _current_ordinal += _page->num_rows();
        RETURN_IF_ERROR(_load_next_page(&eos));
        if (eos) {
            // release shareBufferStream
            if (config::io_coalesce_lake_read_enable && _opts.is_io_coalesce) {
                auto shared_buffer_stream = dynamic_cast<io::SharedBufferedInputStream*>(_opts.read_file);
                if (shared_buffer_stream != nullptr) {
                    shared_buffer_stream->release();
                }
            }
            break;
        }
    }
    _opts.stats->bytes_read += static_cast<int64_t>(*count);
    return Status::OK();
}

Status ScalarColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    size_t prev_bytes = dst->byte_size();
    SparseRangeIterator<> iter = range.new_iterator();
    size_t end_ord = _page->first_ordinal() + _page->num_rows();
    bool contain_deleted_row = (dst->delete_state() != DEL_NOT_SATISFIED);
    SparseRange<> read_range;
    // range is empty should only occur when array column is nullable
    DCHECK(range.empty() || (range.begin() == _current_ordinal));

    // read data from discontinuous ranges in multiple pages
    // data in the same data page will be read together in one function call
    // to reduce the overhead of multiple function calls
    while (iter.has_more()) {
        if (_page->remaining() == 0 && iter.begin() == end_ord) {
            // next row is the first row of next page
            // do _load_next_page directly to avoid seek_page
            _opts.stats->block_seek_num += 1;
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                // release shareBufferStream
                if (config::io_coalesce_lake_read_enable && _opts.is_io_coalesce) {
                    auto shared_buffer_stream = dynamic_cast<io::SharedBufferedInputStream*>(_opts.read_file);
                    if (shared_buffer_stream != nullptr) {
                        shared_buffer_stream->release();
                    }
                }
                break;
            }
            end_ord = _page->first_ordinal() + _page->num_rows();
        } else if (iter.begin() >= end_ord) {
            // next row is not in current page
            // seek data page first
            _opts.stats->block_seek_num += 1;
            RETURN_IF_ERROR(seek_to_ordinal(iter.begin()));
            end_ord = _page->first_ordinal() + _page->num_rows();
        }

        _current_ordinal = iter.begin();
        if (end_ord > _current_ordinal) {
            // the data of current_range is in current page
            // add current_range into read_range
            Range<> r = iter.next(end_ord - _current_ordinal);
            read_range.add(Range<>(r.begin() - _page->first_ordinal(), r.end() - _page->first_ordinal()));
            _current_ordinal += r.span_size();
        }

        if (iter.begin() >= end_ord) {
            // next row is not in current page which means all ranges in
            // current page have been added in read range
            // read current page data first
            contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
            RETURN_IF_ERROR(_page->read(dst, read_range));
            read_range.clear();
        }
    }

    if (!read_range.empty()) {
        // read data left if read range is not empty
        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        RETURN_IF_ERROR(_page->read(dst, read_range));
        read_range.clear();
    }
    dst->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    _opts.stats->bytes_read += (dst->byte_size() - prev_bytes);

    return Status::OK();
}

Status ScalarColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_data_page(_page_iter));
    RETURN_IF_ERROR(_seek_to_pos_in_page(_page.get(), 0));
    *eos = false;
    return Status::OK();
}

template <LogicalType Type>
Status ScalarColumnIterator::_load_dict_page() {
    DCHECK(_dict_decoder == nullptr);
    // read dictionary page
    Slice dict_data;
    PageFooterPB dict_footer;
    RETURN_IF_ERROR(
            _reader->read_page(_opts, _reader->get_dict_page_pointer(), &_dict_page_handle, &dict_data, &dict_footer));
    // ignore dict_footer.dict_page_footer().encoding() due to only
    // PLAIN_ENCODING is supported for dict page right now
    if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_JSON) {
        _dict_decoder = std::make_unique<BinaryPlainPageDecoder<Type>>(dict_data);
    } else {
        _dict_decoder = std::make_unique<BitShufflePageDecoder<Type>>(dict_data);
    }
    return _dict_decoder->init();
}

template <LogicalType Type>
Status ScalarColumnIterator::_do_init_dict_decoder() {
    if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_JSON) {
        auto dict_page_decoder = down_cast<BinaryDictPageDecoder<Type>*>(_page->data_decoder());
        if (dict_page_decoder->encoding_type() == DICT_ENCODING) {
            if (_dict_decoder == nullptr) {
                RETURN_IF_ERROR(_load_dict_page<Type>());
            }
            dict_page_decoder->set_dict_decoder(_dict_decoder.get());
        }
    } else {
        auto dict_page_decoder = down_cast<DictPageDecoder<Type>*>(_page->data_decoder());
        if (dict_page_decoder->encoding_type() == DICT_ENCODING) {
            if (_dict_decoder == nullptr) {
                RETURN_IF_ERROR(_load_dict_page<Type>());
            }
            dict_page_decoder->set_dict_decoder(_dict_decoder.get());
        }
    }
    return Status::OK();
}

Status ScalarColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_reader->read_page(_opts, iter.page(), &handle, &page_body, &footer));
    RETURN_IF_ERROR(parse_page(&_page, std::move(handle), page_body, footer.data_page_footer(),
                               _reader->encoding_info(), iter.page(), iter.page_index()));

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    if (_init_dict_decoder_func != nullptr) {
        RETURN_IF_ERROR((this->*_init_dict_decoder_func)());
    }
    return Status::OK();
}

Status ScalarColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                        const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                                        CompoundNodeType pred_relation) {
    DCHECK(row_ranges->empty());
    if (_reader->has_zone_map()) {
        if (!_delete_partial_satisfied_pages.has_value()) {
            _delete_partial_satisfied_pages.emplace();
        }

        IndexReadOptions opts;
        opts.use_page_cache = !_opts.temporary_data && _opts.use_page_cache && !config::disable_storage_page_cache &&
                              config::enable_zonemap_index_memory_page_cache;
        opts.lake_io_opts = _opts.lake_io_opts;
        opts.read_file = _opts.read_file;
        opts.stats = _opts.stats;
        RETURN_IF_ERROR(_reader->zone_map_filter(predicates, del_predicate, &_delete_partial_satisfied_pages.value(),
                                                 row_ranges, opts, pred_relation));
    } else {
        row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    }
    return Status::OK();
}

bool ScalarColumnIterator::has_original_bloom_filter_index() const {
    return _reader->has_original_bloom_filter_index();
}

bool ScalarColumnIterator::has_ngram_bloom_filter_index() const {
    return _reader->has_ngram_bloom_filter_index();
}

Status ScalarColumnIterator::get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                            SparseRange<>* row_ranges) {
    RETURN_IF(!_reader->has_bloom_filter_index(), Status::OK());

    bool support_original_bloom_filter = false;
    bool support_ngram_bloom_filter = false;
    // bloom filter index can only be either original bloom filter or ngram bloom filter
    if (_reader->has_original_bloom_filter_index()) {
        support_original_bloom_filter =
                std::ranges::any_of(predicates, [](const auto* pred) { return pred->support_original_bloom_filter(); });
    } else if (_reader->has_ngram_bloom_filter_index()) {
        support_ngram_bloom_filter =
                std::ranges::any_of(predicates, [](const auto* pred) { return pred->support_ngram_bloom_filter(); });
    }

    if (!support_original_bloom_filter && !support_ngram_bloom_filter) {
        return Status::OK();
    }

    IndexReadOptions opts;
    opts.use_page_cache = !_opts.temporary_data && !config::disable_storage_page_cache && _opts.use_page_cache;
    opts.kept_in_memory = false;
    opts.lake_io_opts = _opts.lake_io_opts;
    opts.read_file = _opts.read_file;
    opts.stats = _opts.stats;
    // filter data using bloom filter or ngram bloom filter
    if (support_original_bloom_filter) {
        RETURN_IF_ERROR(_reader->original_bloom_filter(predicates, row_ranges, opts));
    } else {
        RETURN_IF_ERROR(_reader->ngram_bloom_filter(predicates, row_ranges, opts));
    }
    return Status::OK();
}

int ScalarColumnIterator::dict_lookup(const Slice& word) {
    DCHECK(all_page_dict_encoded());
    return (this->*_dict_lookup_func)(word);
}

Status ScalarColumnIterator::next_dict_codes(size_t* n, Column* dst) {
    DCHECK(all_page_dict_encoded());
    return (this->*_next_dict_codes_func)(n, dst);
}

Status ScalarColumnIterator::next_dict_codes(const SparseRange<>& range, Column* dst) {
    DCHECK(all_page_dict_encoded());
    return (this->*_next_batch_dict_codes_func)(range, dst);
}

Status ScalarColumnIterator::decode_dict_codes(const int32_t* codes, size_t size, Column* words) {
    DCHECK(all_page_dict_encoded());
    return (this->*_decode_dict_codes_func)(codes, size, words);
}

Status ScalarColumnIterator::fetch_all_dict_words(std::vector<Slice>* words) const {
    DCHECK(all_page_dict_encoded());
    return (this->*_fetch_all_dict_words_func)(words);
}

template <LogicalType Type>
Status ScalarColumnIterator::_fetch_all_dict_words(std::vector<Slice>* words) const {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    uint32_t words_count = dict->count();
    words->reserve(words_count);
    for (uint32_t i = 0; i < words_count; i++) {
        if constexpr (Type != TYPE_CHAR) {
            words->emplace_back(dict->string_at_index(i));
        } else {
            Slice s = dict->string_at_index(i);
            s.size = strnlen(s.data, s.size);
            words->emplace_back(s);
        }
    }
    return Status::OK();
}

template <LogicalType Type>
int ScalarColumnIterator::_do_dict_lookup(const Slice& word) {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    return dict->find(word);
}

template <LogicalType Type>
Status ScalarColumnIterator::_do_next_dict_codes(size_t* n, Column* dst) {
    size_t remaining = *n;
    bool contain_delted_row = false;
    while (remaining > 0) {
        if (_page->remaining() == 0) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }
        DCHECK(_page->encoding_type() == DICT_ENCODING);

        contain_delted_row = contain_delted_row || _contains_deleted_row(_page->page_index());
        // number of rows to be read from this page
        size_t nread = remaining;
        RETURN_IF_ERROR(_page->read_dict_codes(dst, &nread));
        _current_ordinal += nread;
        remaining -= nread;
        _opts.stats->bytes_read += static_cast<int64_t>(nread * sizeof(int32_t));
    }
    dst->set_delete_state(contain_delted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    *n -= remaining;
    return Status::OK();
}

template <LogicalType Type>
Status ScalarColumnIterator::_do_next_batch_dict_codes(const SparseRange<>& range, Column* dst) {
    bool contain_deleted_row = false;
    SparseRangeIterator<> iter = range.new_iterator();
    size_t end_ord = _page->first_ordinal() + _page->num_rows();
    SparseRange<> read_range;

    DCHECK(range.empty() || range.begin() == _current_ordinal);
    // similar to ScalarColumnIterator::next_batch
    while (iter.has_more()) {
        if (_page->remaining() == 0 && iter.begin() == end_ord) {
            _opts.stats->block_seek_num += 1;
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
            end_ord = _page->first_ordinal() + _page->num_rows();
        } else if (iter.begin() >= end_ord) {
            _opts.stats->block_seek_num += 1;
            RETURN_IF_ERROR(seek_to_ordinal(iter.begin()));
            end_ord = _page->first_ordinal() + _page->num_rows();
        }

        _current_ordinal = iter.begin();
        if (end_ord > _current_ordinal) {
            Range<> r = iter.next(end_ord - _current_ordinal);
            read_range.add(Range<>(r.begin() - _page->first_ordinal(), r.end() - _page->first_ordinal()));
            _current_ordinal += r.span_size();
        }

        if (iter.begin() >= end_ord) {
            contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
            RETURN_IF_ERROR(_page->read_dict_codes(dst, read_range));
            read_range.clear();
        }
    }

    if (!read_range.empty()) {
        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        RETURN_IF_ERROR(_page->read_dict_codes(dst, read_range));
        read_range.clear();
    }
    dst->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);

    return Status::OK();
}

template <LogicalType Type>
Status ScalarColumnIterator::_do_decode_dict_codes(const int32_t* codes, size_t size, Column* words) {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    std::vector<Slice> slices;
    slices.reserve(size);
    for (size_t i = 0; i < size; i++) {
        if (codes[i] >= 0) {
            if constexpr (Type != TYPE_CHAR) {
                slices.emplace_back(dict->string_at_index(codes[i]));
            } else {
                Slice s = dict->string_at_index(codes[i]);
                s.size = strnlen(s.data, s.size);
                slices.emplace_back(s);
            }
        } else {
            slices.emplace_back("");
        }
    }
    [[maybe_unused]] bool ok = words->append_strings(slices);
    DCHECK(ok);
    _opts.stats->bytes_read += static_cast<int64_t>(words->byte_size() + BitmapSize(slices.size()));
    return Status::OK();
}

template <typename PageParseFunc>
Status ScalarColumnIterator::_fetch_by_rowid(const rowid_t* rowids, size_t size, Column* values,
                                             PageParseFunc&& page_parse) {
    DCHECK(std::is_sorted(rowids, rowids + size));
    RETURN_IF(size == 0, Status::OK());
    size_t prev_bytes = values->byte_size();
    const rowid_t* const end = rowids + size;
    bool contain_deleted_row = (values->delete_state() != DEL_NOT_SATISFIED);
    do {
        RETURN_IF_ERROR(seek_to_ordinal(*rowids));
        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        auto last_rowid = implicit_cast<rowid_t>(_page->first_ordinal() + _page->num_rows());
        const rowid_t* next_page_rowid = std::lower_bound(rowids, end, last_rowid);
        while (rowids != next_page_rowid) {
            DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
            rowid_t curr = *rowids;
            _current_ordinal = implicit_cast<ordinal_t>(curr);
            RETURN_IF_ERROR(_page->seek(curr - _page->first_ordinal()));
            const rowid_t* p = rowids + 1;
            while ((next_page_rowid != p) && (*p == curr + 1)) {
                curr = *p++;
            }
            size_t nread = p - rowids;
            RETURN_IF_ERROR(page_parse(values, &nread));
            _current_ordinal += nread;
            rowids = p;
        }
        DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
    } while (rowids != end);
    values->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    _opts.stats->bytes_read += static_cast<int64_t>(values->byte_size() - prev_bytes);
    DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
    return Status::OK();
}

Status ScalarColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto page_parse = [&](Column* column, size_t* count) { return _page->read(column, count); };
    return _fetch_by_rowid(rowids, size, values, page_parse);
}

Status ScalarColumnIterator::fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto page_parse = [&](Column* column, size_t* count) { return _page->read_dict_codes(column, count); };
    return _fetch_by_rowid(rowids, size, values, page_parse);
}

int ScalarColumnIterator::dict_size() {
    if (_reader->column_type() == TYPE_CHAR) {
        auto dict = down_cast<BinaryPlainPageDecoder<TYPE_CHAR>*>(_dict_decoder.get());
        return static_cast<int>(dict->dict_size());
    } else if (_reader->column_type() == TYPE_JSON) {
        auto dict = down_cast<BinaryPlainPageDecoder<TYPE_JSON>*>(_dict_decoder.get());
        return static_cast<int>(dict->dict_size());
    } else if (_reader->column_type() == TYPE_VARCHAR) {
        auto dict = down_cast<BinaryPlainPageDecoder<TYPE_VARCHAR>*>(_dict_decoder.get());
        return static_cast<int>(dict->dict_size());
    }
    __builtin_unreachable();
    return 0;
}

bool ScalarColumnIterator::_contains_deleted_row(uint32_t page_index) const {
    if (_reader->has_zone_map() && _delete_partial_satisfied_pages.has_value()) {
        return _delete_partial_satisfied_pages->contains(page_index);
    }
    // if there is no zone map should be treated as DEL_PARTIAL_SATISFIED
    return true;
}

StatusOr<std::vector<std::pair<int64_t, int64_t>>> ScalarColumnIterator::get_io_range_vec(const SparseRange<>& range,
                                                                                          Column* dst) {
    (void)dst;
    std::vector<std::pair<int64_t, int64_t>> res;
    auto reader = get_column_reader();
    if (reader == nullptr) {
        // should't happen
        return Status::InvalidArgument(fmt::format("column reader for {} is nullptr", _opts.read_file->filename()));
    }

    std::vector<std::pair<int, int>> page_index;
    int prev_page_index = -1;
    for (auto index = 0; index < range.size(); index++) {
        auto row_start = range[index].begin();
        auto row_end = range[index].end() - 1;
        OrdinalPageIndexIterator iter_start;
        OrdinalPageIndexIterator iter_end;
        RETURN_IF_ERROR(reader->seek_at_or_before(row_start, &iter_start));
        RETURN_IF_ERROR(reader->seek_at_or_before(row_end, &iter_end));

        if (prev_page_index == iter_start.page_index()) {
            // merge page index
            page_index.back().second = iter_end.page_index();
        } else {
            page_index.emplace_back(std::make_pair(iter_start.page_index(), iter_end.page_index()));
        }

        prev_page_index = iter_end.page_index();
    }

    for (auto pair : page_index) {
        OrdinalPageIndexIterator iter_start;
        OrdinalPageIndexIterator iter_end;
        RETURN_IF_ERROR(reader->seek_by_page_index(pair.first, &iter_start));
        RETURN_IF_ERROR(reader->seek_by_page_index(pair.second, &iter_end));
        auto offset = iter_start.page().offset;
        auto size = iter_end.page().offset - offset + iter_end.page().size;
        res.emplace_back(offset, size);
    }

    return res;
}

} // namespace starrocks
