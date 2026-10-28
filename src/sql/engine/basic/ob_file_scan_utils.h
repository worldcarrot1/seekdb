/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

// column types the file scanner can infer; everything maps to an ObItemType later
enum ObFileTypeClass
{
  FILE_TYPE_INVALID = 0,
  FILE_TYPE_BIGINT,
  FILE_TYPE_DOUBLE,
  FILE_TYPE_BOOLEAN,
  FILE_TYPE_VARCHAR,
  FILE_TYPE_DATE,
  FILE_TYPE_DATETIME,
  FILE_TYPE_ALL_NULL,
};

struct ObFileColumnDef
{
  ObFileColumnDef() : name_(), type_class_(FILE_TYPE_INVALID), nullable_(false) {}
  void reset() { name_.reset(); type_class_ = FILE_TYPE_INVALID; nullable_ = false; }
  TO_STRING_KV(K_(name), K_(type_class), K_(nullable));
  common::ObString name_; // owned by the owning table def arena
  ObFileTypeClass type_class_;
  bool nullable_;
};

struct ObFileScanOptions
{
  ObFileScanOptions()
    : has_header_(true), delimiter_(','), quote_('\"'), escape_('\0'),
      sample_size_(-1), has_sample_size_(false), null_str_(), is_glob_(false) {}
  bool is_valid_sample_size() const { return !has_sample_size_ || -1 == sample_size_; }
  TO_STRING_KV(K_(has_header), K_(delimiter), K_(quote), K_(escape), K_(sample_size),
               K_(has_sample_size), K_(null_str), K_(is_glob));
  bool has_header_;      // csv: first line is header
  char delimiter_;
  char quote_;
  char escape_;          // '\0' means quote doubling is the escape rule
  int64_t sample_size_;  // only -1 is supported
  bool has_sample_size_;
  common::ObString null_str_; // reserved; empty means unquoted empty field is NULL
  bool is_glob_;         // pattern may contain '*'
};

struct ObFileStat
{
  ObFileStat() : size_(-1), mtime_(-1) {}
  int fill(const char *path);
  bool is_same(const ObFileStat &other) const { return size_ == other.size_ && mtime_ == other.mtime_; }
  TO_STRING_KV(K_(size), K_(mtime));
  int64_t size_;
  int64_t mtime_;
};

// One materialized cell: normalized text value, or SQL NULL when is_null_.
struct ObFileCell
{
  ObFileCell() : val_(), is_null_(false) {}
  TO_STRING_KV(K_(val), K_(is_null));
  common::ObString val_;
  bool is_null_;
};

// A resolved file table: one or more physical files plus their shared column schema.
// Rows are always streamed from disk; nothing here caches file contents.
class ObFileTableDef
{
public:
  ObFileTableDef();
  ~ObFileTableDef();
  void reset();

  const common::ObIArray<ObFileColumnDef> &get_columns() const { return columns_; }
  int64_t get_column_count() const { return columns_.count(); }
  const ObFileColumnDef &get_column(int64_t idx) const { return columns_.at(idx); }
  const common::ObIArray<common::ObString> &get_files() const { return files_; }
  ObFileScanOptions &get_options() { return options_; }
  const ObFileScanOptions &get_options() const { return options_; }
  const ObFileStat &get_stat(int64_t idx) const { return stats_.at(idx); }
  int64_t get_estimated_rows() const { return est_rows_; }
  int64_t get_format() const { return format_; }  // ObFileFormat enum below
  bool changed() const;                            // any file size/mtime drifted since resolve

  // resolve entry: expands glob or single path, sniffs the format from the magic bytes
  // of the first file and infers the whole-file schema (csv/jsonl) or footer schema (parquet).
  int init(common::ObIAllocator &alloc, const common::ObString &raw_path,
           const ObFileScanOptions &opts);

  // glob('...'): matched file paths become rows under a single varchar column `file`
  int init_files_only(common::ObIAllocator &alloc, const common::ObString &raw_path);
  // materialize every row of the resolved table into normalized string cells
  int read_all_cells(common::ObIAllocator &cell_alloc,
                     common::ObIArray<common::ObSEArray<ObFileCell, 8> > &cells);

  TO_STRING_KV(K_(format), K_(files), K_(columns), K_(est_rows));

private:
  int expand_files(common::ObIAllocator &alloc, const common::ObString &raw_path);
  int detect_format();
  int infer_schema(common::ObIAllocator &alloc);
  int infer_csv(common::ObIAllocator &alloc, const char *path);
  int infer_jsonl(common::ObIAllocator &alloc, const char *path);
  int copy_defs(common::ObIAllocator &alloc, const ObFileTableDef &def);
  int read_csv_cells(common::ObIAllocator &alloc, common::ObIArray<common::ObSEArray<ObFileCell, 8> > &cells);
  int read_jsonl_cells(common::ObIAllocator &alloc, common::ObIArray<common::ObSEArray<ObFileCell, 8> > &cells);
  int read_parquet_cells(common::ObIAllocator &alloc, common::ObIArray<common::ObSEArray<ObFileCell, 8> > &cells);

public:
  enum ObFileFormat
  {
    FILE_FORMAT_UNKNOWN = 0,
    FILE_FORMAT_CSV = 1,
    FILE_FORMAT_JSONL = 2,
    FILE_FORMAT_PARQUET = 3,
    FILE_FORMAT_GLOB = 4,
  };

private:
  common::ObArenaAllocator file_alloc_; // owns strings for schema that outlives parsing buffers
  int64_t format_;
  common::ObSEArray<common::ObString, 4> files_;
  common::ObSEArray<ObFileColumnDef, 8> columns_;
  common::ObSEArray<ObFileStat, 4> stats_;
  ObFileScanOptions options_;
  int64_t est_rows_;
  DISALLOW_COPY_AND_ASSIGN(ObFileTableDef);
};

// one decoded row: raw field strings, cast by the caller into datums
struct ObFileRawRow
{
  ObFileRawRow() : fields_(), line_no_(0) {}
  void reset() { for (int64_t i = 0; i < fields_.count(); ++i) fields_.at(i).reset(); }
  TO_STRING_KV(K_(line_no), K_(fields));
  common::ObSEArray<common::ObString, 8> fields_;
  int64_t line_no_;
};

// Streaming CSV reader: quoted fields with embedded delimiters/newlines, "" escaping
// (or explicit escape char), LF/CRLF line endings, bad-row diagnostics with line numbers.
class ObCsvFileScanner
{
public:
  ObCsvFileScanner();
  ~ObCsvFileScanner();
  int open(const char *path, const ObFileScanOptions &opts, common::ObIAllocator &row_alloc);
  int get_next_row(ObFileRawRow &row, bool &is_done);
  const ObFileColumnDef *get_header() const { return header_; }  // NULL until parsed
  int64_t get_header_cols() const { return header_cols_; }
  int64_t get_cur_line() const { return cur_line_; }
  void close();
private:
  int fill_buf();
  int parse_field(common::ObIAllocator &alloc, ObString &field, bool &raw_eof, bool &saw_quote);
  int header_cols_;
  int fd_;
  char *buf_;
  int64_t buf_len_;
  int64_t buf_pos_;
  int64_t cur_line_;
  ObFileColumnDef *header_;
  ObFileScanOptions opts_;
  common::ObIAllocator *row_alloc_;
  bool eof_;
  bool opened_;
  DISALLOW_COPY_AND_ASSIGN(ObCsvFileScanner);
};

// Type inference state machine, fed with raw field strings per column in file order.
class ObFileColumnInfer
{
public:
  ObFileColumnInfer() : type_class_(FILE_TYPE_ALL_NULL), saw_value_(false), has_null_(false) {}
  int feed(const common::ObString &val, bool is_null);
  void finalize(ObFileColumnDef &def, const common::ObString &name) const;
  bool has_null() const { return has_null_; }
private:
  static bool is_bool(const common::ObString &s);
  static bool is_int(const common::ObString &s);
  static bool is_double(const common::ObString &s);
  static bool is_date(const common::ObString &s, bool fill_def);
  static bool is_datetime(const common::ObString &s);
  int widen(ObFileTypeClass t);
  ObFileTypeClass type_class_;
  bool saw_value_;
  bool has_null_;
};

// JSONL: flat objects only; union of keys in first-seen order; nested values are errors.
class ObJsonlSchemaHelper
{
public:
  static int parse_line(common::ObIAllocator &alloc, const common::ObString &line,
                        common::ObIArray<common::ObString> &keys,
                        common::ObIArray<common::ObString> &vals,
                        common::ObIArray<bool> &is_null,
                        int64_t &line_no);
};

// Minimal parquet reader: thrift-compact footer + row groups, PLAIN/RLE_DICTIONARY
// bit widths, UNCOMPRESSED and SNAPPY codecs, def levels for optional columns.
// Decoding is per row group: advance_cursor() moves to the next group and
// materializes only that group's projected leaf columns, never the whole file.
class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader();
  int open(const char *path);
  // column name/physical mapping from footer schema (leaf messages in order)
  int64_t get_leaf_count() const;
  int get_leaf_name(int64_t idx, common::ObString &name) const;
  int get_leaf_physical_type(int64_t idx, int32_t &type) const;   // parquet physical enum
  int get_leaf_converted_type(int64_t idx, int32_t &ct) const;    // parquet converted_type enum
  int get_leaf_repetition(int64_t idx, int32_t &rep) const;    // 0 required 1 optional
  int64_t get_num_rows() const;
  // row-group cursor: returns OB_ITER_END when exhausted; decoded values of every leaf
  // are refreshed for the new group in get_group_col()/get_group_num_rows().
  int advance_cursor();
  bool cursor_valid() const;
  int64_t get_group_num_rows() const;
  // per-leaf decoded cell of the current row group; present=false means SQL NULL.
  int get_group_int64(int64_t leaf_idx, int64_t row_idx, int64_t &v, bool &present) const;
  int get_group_double(int64_t leaf_idx, int64_t row_idx, double &v, bool &present) const;
  int get_group_bool(int64_t leaf_idx, int64_t row_idx, bool &v, bool &present) const;
  int get_group_string(int64_t leaf_idx, int64_t row_idx, common::ObString &v, bool &present) const;
  void close();
private:
  int parse_footer();
  class Impl;
  Impl *impl_;
  DISALLOW_COPY_AND_ASSIGN(ObParquetReader);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_
