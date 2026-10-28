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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <fnmatch.h>
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObFileStat::fill(const char *path)
{
  int ret = OB_SUCCESS;
  struct stat st;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != ::stat(path, &st)) {
    ret = OB_FILE_NOT_EXIST;
  } else {
    size_ = st.st_size;
    mtime_ = static_cast<int64_t>(st.st_mtime);
  }
  return ret;
}

ObFileTableDef::ObFileTableDef()
  : file_alloc_(common::ObMemAttr("FileTableDef")),
    format_(FILE_FORMAT_UNKNOWN), files_(), columns_(), stats_(), options_(), est_rows_(0)
{
}

ObFileTableDef::~ObFileTableDef()
{
}

void ObFileTableDef::reset()
{
  format_ = FILE_FORMAT_UNKNOWN;
  files_.reset();
  columns_.reset();
  stats_.reset();
  options_ = ObFileScanOptions();
  est_rows_ = 0;
  file_alloc_.reset();
}

int ObFileTableDef::init(ObIAllocator &alloc, const ObString &raw_path,
                         const ObFileScanOptions &opts)
{
  int ret = OB_SUCCESS;
  reset();
  ObFileScanOptions local_opts = opts;
  if (OB_FAIL(ob_write_string(file_alloc_, opts.null_str_, local_opts.null_str_, true))) {
    LOG_WARN("failed to copy null str", K(ret));
  } else if (FALSE_IT(options_ = local_opts)) {
  } else if (OB_FAIL(expand_files(alloc, raw_path))) {
    LOG_WARN("failed to expand files", K(ret), K(raw_path));
  } else if (OB_UNLIKELY(files_.empty())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
    LOG_WARN("no file matched", K(ret), K(raw_path));
  } else if (OB_FAIL(detect_format())) {
    LOG_WARN("failed to detect format", K(ret));
  } else if (OB_FAIL(infer_schema(alloc))) {
    LOG_WARN("failed to infer schema", K(ret), K(raw_path));
  }
  return ret;
}

int ObFileTableDef::expand_files(ObIAllocator &alloc, const ObString &raw_path)
{
  int ret = OB_SUCCESS;
  if (raw_path.empty() || raw_path.length() >= OB_MALLOC_BIG_BLOCK_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid path length", K(ret), K(raw_path));
  } else {
    ObString path_str;
    char *tmp = static_cast<char *>(file_alloc_.alloc(raw_path.length() + 1));
    if (OB_ISNULL(tmp)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(tmp, raw_path.ptr(), raw_path.length());
      tmp[raw_path.length()] = '\0';
      path_str.assign_ptr(tmp, raw_path.length());
      if (options_.is_glob_ || nullptr != memchr(path_str.ptr(), '*', path_str.length())) {
        // split into dir + pattern, list with fnmatch
        const char *slash = strrchr(path_str.ptr(), '/');
        if (OB_ISNULL(slash)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "glob path");
        } else {
          ObString dir;
          char *dir_buf = static_cast<char *>(file_alloc_.alloc(slash - path_str.ptr() + 2));
          if (OB_ISNULL(dir_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(dir_buf, path_str.ptr(), slash - path_str.ptr() + 1);
            dir_buf[slash - path_str.ptr() + 1] = '\0';
            dir.assign_ptr(dir_buf, slash - path_str.ptr() + 1);
            const char *pattern = slash + 1;
            DIR *d = ::opendir(dir_buf);
            if (OB_ISNULL(d)) {
              ret = OB_FILE_NOT_EXIST;
              LOG_USER_ERROR(OB_FILE_NOT_EXIST);
            } else {
              struct dirent *ent = nullptr;
              common::ObSEArray<ObString, 8> matched;
              while (OB_NOT_NULL(ent = ::readdir(d))) {
                if ('.' == ent->d_name[0]) {
                  // shell glob semantics: '*' does not match dot entries (., .., hidden)
                } else if (0 == ::fnmatch(pattern, ent->d_name, 0)) {
                  int64_t nlen = strlen(ent->d_name);
                  char *nb = static_cast<char *>(file_alloc_.alloc(nlen + 1));
                  if (OB_ISNULL(nb)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    break;
                  }
                  MEMCPY(nb, ent->d_name, nlen);
                  nb[nlen] = '\0';
                  ObString name;
                  name.assign_ptr(nb, nlen);
                  if (OB_FAIL(matched.push_back(name))) {
                  }
                }
              }
              ::closedir(d);
              if (OB_FAIL(ret)) {
              } else {
                // sort by full path to make the file column order deterministic
                for (int64_t i = 0; OB_SUCC(ret) && i < matched.count(); ++i) {
                  int64_t best = i;
                  for (int64_t j = i + 1; j < matched.count(); ++j) {
                    if (matched.at(j).compare(matched.at(best)) < 0) {
                      best = j;
                    }
                  }
                  if (best != i) {
                    ObString t = matched.at(i);
                    matched.at(i) = matched.at(best);
                    matched.at(best) = t;
                  }
                  int64_t flen = dir.length() + matched.at(i).length();
                  char *fb = static_cast<char *>(file_alloc_.alloc(flen + 1));
                  if (OB_ISNULL(fb)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                  } else {
                    MEMCPY(fb, dir.ptr(), dir.length());
                    MEMCPY(fb + dir.length(), matched.at(i).ptr(), matched.at(i).length());
                    fb[flen] = '\0';
                    ObString full;
                    full.assign_ptr(fb, flen);
                    if (OB_FAIL(files_.push_back(full))) {
                    }
                  }
                }
              }
            }
          }
        }
      } else {
        if (OB_FAIL(files_.push_back(path_str))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < files_.count(); ++i) {
        ObFileStat st;
        char cbuf[OB_MAX_ERROR_MSG_LEN];
        int64_t cpy = std::min<int64_t>(files_.at(i).length(), (int64_t)sizeof(cbuf) - 1);
        MEMCPY(cbuf, files_.at(i).ptr(), cpy);
        cbuf[cpy] = '\0';
        if (OB_FAIL(st.fill(cbuf))) {
          LOG_WARN("failed to stat file", K(ret), K(files_.at(i)));
        } else if (OB_FAIL(stats_.push_back(st))) {
        }
      }
    }
  }
  return ret;
}

int ObFileTableDef::detect_format()
{
  int ret = OB_SUCCESS;
  char cbuf[256];
  int64_t cpy = std::min<int64_t>(files_.at(0).length(), (int64_t)sizeof(cbuf) - 1);
  MEMCPY(cbuf, files_.at(0).ptr(), cpy);
  cbuf[cpy] = '\0';
  int fd = ::open(cbuf, O_RDONLY);
  if (fd < 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
  } else {
    char magic[4] = {0};
    ssize_t n = ::read(fd, magic, 4);
    ::close(fd);
    if (n >= 4 && 0 == MEMCMP(magic, "PAR1", 4)) {
      format_ = FILE_FORMAT_PARQUET;
    } else {
      // sniff first non-space byte: '{' suggests jsonl, otherwise csv
      FILE *fp = ::fopen(cbuf, "r");
      if (OB_ISNULL(fp)) {
        ret = OB_FILE_NOT_EXIST;
      } else {
        int ch = EOF;
        while ((ch = ::fgetc(fp)) != EOF && isspace(ch)) {}
        ::fclose(fp);
        format_ = ('{' == ch) ? FILE_FORMAT_JSONL : FILE_FORMAT_CSV;
      }
    }
  }
  return ret;
}

bool ObFileColumnInfer::is_bool(const ObString &s)
{
  return (s.length() == 4 && 0 == MEMCMP(s.ptr(), "true", 4))
      || (s.length() == 5 && 0 == MEMCMP(s.ptr(), "false", 5))
      || (s.length() == 5 && 0 == MEMCMP(s.ptr(), "TRUE", 5))
      || (s.length() == 4 && 0 == MEMCMP(s.ptr(), "True", 4))
      || (s.length() == 5 && 0 == MEMCMP(s.ptr(), "False", 5));
}

bool ObFileColumnInfer::is_int(const ObString &s)
{
  bool valid = false;
  if (!s.empty()) {
    const char *p = s.ptr();
    int64_t n = s.length();
    int64_t i = ('-' == p[0] || '+' == p[0]) ? 1 : 0;
    if (i < n) {
      valid = true;
      int64_t v = 0;
      bool neg = ('-' == p[0]);
      for (; valid && i < n; ++i) {
        if (!isdigit(p[i])) {
          valid = false;
        } else if (v > (int64_t)922337203685477580LL) {
          valid = false; // overflow guard
        } else {
          v = v * 10 + (p[i] - '0');
        }
      }
      if (valid && !neg && v > INT64_MAX / 10 + 1) {
        valid = false;
      }
    }
  }
  return valid;
}

bool ObFileColumnInfer::is_double(const ObString &s)
{
  bool valid = false;
  if (!s.empty()) {
    char buf[64];
    int64_t cpy = std::min<int64_t>(s.length(), (int64_t)sizeof(buf) - 1);
    MEMCPY(buf, s.ptr(), cpy);
    buf[cpy] = '\0';
    char *end = nullptr;
    double v = strtod(buf, &end);
    valid = (end != buf && '\0' == *end && !std::isnan(v));
  }
  return valid;
}

bool ObFileColumnInfer::is_date(const ObString &s, bool fill_def)
{
  bool valid = false;
  UNUSED(fill_def);
  if (10 == s.length()) {
    const char *p = s.ptr();
    int y = 0, m = 0, d = 0;
    if (isdigit(p[0]) && isdigit(p[1]) && isdigit(p[2]) && isdigit(p[3])
        && '-' == p[4] && isdigit(p[5]) && isdigit(p[6]) && '-' == p[7]
        && isdigit(p[8]) && isdigit(p[9])) {
      y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
      m = (p[5]-'0')*10 + (p[6]-'0');
      d = (p[8]-'0')*10 + (p[9]-'0');
      static const int DMON[] = {31,28,31,30,31,30,31,31,30,31,30,31};
      if (m >= 1 && m <= 12 && d >= 1) {
        int maxd = DMON[m-1];
        if (2 == m && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) {
          maxd = 29;
        }
        valid = (d <= maxd);
      }
    }
  }
  return valid;
}

bool ObFileColumnInfer::is_datetime(const ObString &s)
{
  bool valid = false;
  if (s.length() >= 19) {
    const ObString date_part(10, s.ptr());
    const char sep = s.ptr()[10];
    if (is_date(date_part, false) && (' ' == sep || 'T' == sep)) {
      const char *p = s.ptr() + 11;
      int64_t len = s.length() - 11;
      valid = (len >= 8 && p[2] == ':' && p[5] == ':'
               && isdigit(p[0]) && isdigit(p[1]) && isdigit(p[3]) && isdigit(p[4])
               && isdigit(p[6]) && isdigit(p[7]) && (len == 8 || '.' == p[8]));
    }
  }
  return valid;
}

int ObFileColumnInfer::widen(ObFileTypeClass t)
{
  int ret = OB_SUCCESS;
  if (FILE_TYPE_ALL_NULL == type_class_) {
    type_class_ = t;
  } else if (type_class_ == t) {
    // stay
  } else if (FILE_TYPE_BIGINT == type_class_ && FILE_TYPE_DOUBLE == t) {
    type_class_ = FILE_TYPE_DOUBLE;
  } else if (FILE_TYPE_DOUBLE == type_class_ && FILE_TYPE_BIGINT == t) {
    // integral value in a double column keeps the column double
  } else if (FILE_TYPE_DATE == type_class_ && FILE_TYPE_DATETIME == t) {
    type_class_ = FILE_TYPE_DATETIME;
  } else if (FILE_TYPE_DATETIME == type_class_ && FILE_TYPE_DATE == t) {
    // keep datetime
  } else if ((FILE_TYPE_BIGINT == type_class_ && FILE_TYPE_BOOLEAN == t)
             || (FILE_TYPE_BOOLEAN == type_class_ && FILE_TYPE_BIGINT == t)) {
    type_class_ = FILE_TYPE_VARCHAR;
  } else {
    type_class_ = FILE_TYPE_VARCHAR;
  }
  return ret;
}

int ObFileColumnInfer::feed(const ObString &val, bool is_null)
{
  int ret = OB_SUCCESS;
  if (is_null || val.empty()) {
    if (saw_value_) {
      has_null_ = true;
    }
  } else {
    saw_value_ = true;
    if (is_bool(val)) {
      if (OB_FAIL(widen(FILE_TYPE_BOOLEAN))) {
      }
    } else if (is_int(val)) {
      if (OB_FAIL(widen(FILE_TYPE_BIGINT))) {
      }
    } else if (is_double(val)) {
      if (OB_FAIL(widen(FILE_TYPE_DOUBLE))) {
      }
    } else if (is_date(val, false)) {
      if (OB_FAIL(widen(FILE_TYPE_DATE))) {
      }
    } else if (is_datetime(val)) {
      if (OB_FAIL(widen(FILE_TYPE_DATETIME))) {
      }
    } else {
      if (OB_FAIL(widen(FILE_TYPE_VARCHAR))) {
      }
    }
  }
  return ret;
}

void ObFileColumnInfer::finalize(ObFileColumnDef &def, const ObString &name) const
{
  def.name_ = name;
  def.type_class_ = saw_value_ ? type_class_ : FILE_TYPE_VARCHAR; // all-null column becomes varchar
  def.nullable_ = !saw_value_ || has_null_;
}

ObCsvFileScanner::ObCsvFileScanner()
  : header_cols_(0), fd_(-1), buf_(nullptr), buf_len_(0), buf_pos_(0),
    cur_line_(0), header_(nullptr), opts_(), row_alloc_(nullptr), eof_(false), opened_(false)
{
}

ObCsvFileScanner::~ObCsvFileScanner()
{
  close();
}

void ObCsvFileScanner::close()
{
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (nullptr != buf_) {
    ob_free(buf_);
    buf_ = nullptr;
  }
  buf_len_ = 0;
  buf_pos_ = 0;
  eof_ = false;
  opened_ = false;
  header_cols_ = 0;
}

int ObCsvFileScanner::open(const char *path, const ObFileScanOptions &opts, ObIAllocator &row_alloc)
{
  int ret = OB_SUCCESS;
  if (opened_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((fd_ = ::open(path, O_RDONLY)) < 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
  } else if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(64 * 1024, ObMemAttr("CsvScan"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    opts_ = opts;
    row_alloc_ = &row_alloc;
    opened_ = true;
    if (opts_.has_header_) {
      // parse first record as header
      ObFileRawRow hrow;
      bool done = false;
      if (OB_FAIL(get_next_row(hrow, done))) {
      } else if (done) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "empty csv file");
      } else {
        header_cols_ = hrow.fields_.count();
        void *hb = row_alloc_->alloc(sizeof(ObFileColumnDef) * header_cols_);
        if (OB_ISNULL(hb)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          header_ = new (hb) ObFileColumnDef[header_cols_];
          for (int64_t i = 0; i < header_cols_; ++i) {
            header_[i].name_ = hrow.fields_.at(i);
            header_[i].type_class_ = FILE_TYPE_ALL_NULL;
            header_[i].nullable_ = false;
          }
        }
      }
      hrow.fields_.reset();
    }
  }
  return ret;
}

int ObCsvFileScanner::fill_buf()
{
  int ret = OB_SUCCESS;
  if (buf_pos_ > 0 && buf_pos_ < buf_len_) {
    MEMCPY(buf_, buf_ + buf_pos_, buf_len_ - buf_pos_);
    buf_len_ -= buf_pos_;
    buf_pos_ = 0;
  } else if (buf_pos_ >= buf_len_) {
    buf_len_ = 0;
    buf_pos_ = 0;
  }
  ssize_t n = ::read(fd_, buf_ + buf_len_, 64 * 1024 - buf_len_);
  if (n < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("failed to read csv", K(ret), K(errno));
  } else if (0 == n) {
    eof_ = true;
  } else {
    buf_len_ += n;
  }
  return ret;
}

// field grammar: [space]* (quoted|bare) [space]*, quoted = '"' ( '""' escape or doubled )* '"'
// caller resets alloc per row.
int ObCsvFileScanner::parse_field(ObIAllocator &alloc, ObString &field, bool &raw_eof, bool &saw_quote)
{
  int ret = OB_SUCCESS;
  saw_quote = false;
  raw_eof = false;
  // skip leading spaces (but keep other chars)
  while (true) {
    if (buf_pos_ >= buf_len_ && OB_FAIL(fill_buf())) {
      return ret;
    }
    if (buf_pos_ >= buf_len_ || ' ' != buf_[buf_pos_]) {
      break;
    }
    ++buf_pos_;
  }
  if (buf_pos_ >= buf_len_ && eof_) {
    raw_eof = true;
    field.reset();
  } else if (buf_pos_ < buf_len_ && opts_.quote_ == buf_[buf_pos_]) {
    saw_quote = true;
    ++buf_pos_;
    char *out = nullptr;
    int64_t cap = 0;
    int64_t outlen = 0;
    bool closed = false;
    while (!closed && OB_SUCC(ret)) {
      if (buf_pos_ >= buf_len_ && OB_FAIL(fill_buf())) {
        return ret;
      }
      if (1 == buf_len_ - buf_pos_ && !eof_ && OB_FAIL(fill_buf())) {
        return ret;
      }
      int64_t avail = buf_len_ - buf_pos_;
      if (avail <= 0 && eof_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unterminated quoted csv field");
        break;
      }
      if (nullptr == out) {
        cap = std::max<int64_t>(avail * 2, 64);
        if (OB_ISNULL(out = static_cast<char *>(alloc.alloc(cap)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
      } else if (outlen + avail >= cap) {
        int64_t ncap = (outlen + avail) * 2;
        char *nb = nullptr;
        if (OB_ISNULL(nb = static_cast<char *>(alloc.alloc(ncap)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        MEMCPY(nb, out, outlen);
        out = nb;
        cap = ncap;
      }
      if (OB_SUCC(ret) && avail > 0) {
        if (opts_.escape_ != '\0') {
          // explicit escape char precedes special chars
        }
        int64_t i = buf_pos_;
        for (; i < buf_len_; ) {
          char c = buf_[i];
          if (opts_.quote_ == c) {
            if (i + 1 < buf_len_) {
              if (opts_.quote_ == buf_[i+1]) {
                out[outlen++] = opts_.quote_;
                i += 2;
                continue;
              } else {
                ++i;
                closed = true;
                break;
              }
            } else if (eof_) {
              ++i;
              closed = true;
              break;
            } else {
              // quote at buffer edge: refill to decide doubled vs closing
              break;
            }
          } else if (opts_.escape_ != '\0' && opts_.escape_ == c && i + 1 < buf_len_) {
            out[outlen++] = buf_[i+1];
            i += 2;
          } else {
            out[outlen++] = c;
            ++i;
          }
          if (outlen + 2 >= cap) {
            break; // grow on next chunk
          }
        }
        buf_pos_ = i;
        if (buf_pos_ >= buf_len_ && !closed && OB_FAIL(fill_buf())) {
          return ret;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!closed) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unterminated quoted csv field");
      } else {
        // consume trailing spaces up to delimiter/newline
        while (true) {
          if (buf_pos_ >= buf_len_ && !eof_ && OB_FAIL(fill_buf())) {
            return ret;
          }
          if (buf_pos_ < buf_len_ && ' ' == buf_[buf_pos_]) {
            ++buf_pos_;
          } else {
            break;
          }
        }
        field.assign_ptr(out, outlen);
      }
    }
  } else {
    // bare field until delimiter/newline/eof
    char *out = nullptr;
    int64_t outlen = 0;
    bool fin = false;
    while (!fin && OB_SUCC(ret)) {
      if (buf_pos_ >= buf_len_ && !eof_ && OB_FAIL(fill_buf())) {
        return ret;
      }
      if (buf_pos_ >= buf_len_) {
        raw_eof = eof_;
        fin = true;
      } else {
        int64_t avail = buf_len_ - buf_pos_;
        if (nullptr == out) {
          if (OB_ISNULL(out = static_cast<char *>(alloc.alloc(avail)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
        } else if (outlen + avail > 0) {
          char *nb = nullptr;
          if (OB_ISNULL(nb = static_cast<char *>(alloc.alloc(outlen + avail)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          MEMCPY(nb, out, outlen);
          out = nb;
        }
        int64_t i = buf_pos_;
        for (; i < buf_len_; ++i) {
          char c = buf_[i];
          if (c == opts_.delimiter_ || '\n' == c || '\r' == c) {
            break;
          }
          out[outlen++] = c;
        }
        buf_pos_ = i;
        if (buf_pos_ < buf_len_) {
          fin = true;
        } else if (eof_) {
          fin = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr == out) {
        field.reset();
      } else {
        // trim trailing spaces of bare fields
        while (outlen > 0 && ' ' == out[outlen-1]) { --outlen; }
        field.assign_ptr(out, outlen);
      }
    }
  }
  return ret;
}

int ObCsvFileScanner::get_next_row(ObFileRawRow &row, bool &is_done)
{
  int ret = OB_SUCCESS;
  is_done = false;
  row.reset();
  bool started = false;
  bool row_finished = false;
  while (OB_SUCC(ret) && !is_done && !row_finished) {
    if (buf_pos_ >= buf_len_ && !eof_ && OB_FAIL(fill_buf())) {
      return ret;
    }
    if (!started) {
      if (buf_pos_ >= buf_len_) {
        if (eof_) {
          is_done = true;
          row_finished = true;
        } else {
          continue;
        }
      } else if ('\n' == buf_[buf_pos_]) {
        ++buf_pos_; // skip blank line
        continue;
      } else {
        cur_line_++;
      }
    }
    started = true;
    ObString field;
    bool raw_eof = false;
    bool saw_quote = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_field(*row_alloc_, field, raw_eof, saw_quote))) {
    } else if (OB_FAIL(row.fields_.push_back(field))) {
    } else if (raw_eof) {
      row_finished = true;
    } else {
      // advance past field terminator
      if (buf_pos_ >= buf_len_ && !eof_ && OB_FAIL(fill_buf())) {
        return ret;
      }
      if (buf_pos_ >= buf_len_) {
        row_finished = true; // file ended right after the last field
      } else {
        char c = buf_[buf_pos_];
        if (c == opts_.delimiter_) {
          ++buf_pos_;
        } else if ('\n' == c || '\r' == c) {
          ++buf_pos_;
          if ('\r' == c) {
            if (buf_pos_ >= buf_len_ && !eof_ && OB_FAIL(fill_buf())) {
              return ret;
            }
            if (buf_pos_ < buf_len_ && '\n' == buf_[buf_pos_]) {
              ++buf_pos_;
            }
          }
          row_finished = true;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unexpected character after csv field");
          LOG_WARN("unexpected char after field", K(ret), K(c), K(cur_line_));
        }
      }
    }
  }
  return ret;
}

int ObJsonlSchemaHelper::parse_line(ObIAllocator &alloc, const ObString &line,
                                    ObIArray<ObString> &keys, ObIArray<ObString> &vals,
                                    ObIArray<bool> &is_null, int64_t &line_no)
{
  int ret = OB_SUCCESS;
  keys.reset();
  vals.reset();
  is_null.reset();
  int64_t i = 0;
  const char *p = line.ptr();
  const int64_t n = line.length();
  auto skip_space = [&]() { while (i < n && isspace(p[i])) { ++i; } };
  auto parse_string = [&](ObString &out) -> int {
    int r = OB_SUCCESS;
    out.reset();
    if (i >= n || '"' != p[i]) {
      r = OB_INVALID_ARGUMENT;
    } else {
      ++i;
      char *buf = static_cast<char *>(alloc.alloc(n - i));
      if (OB_ISNULL(buf)) {
        r = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t len = 0;
        bool closed = false;
        while (OB_SUCC(r) && i < n) {
          char c = p[i];
          if ('"' == c) {
            ++i;
            closed = true;
            break;
          } else if ('\\' == c) {
            if (i + 1 >= n) {
              r = OB_INVALID_ARGUMENT;
            } else {
              char e = p[++i];
              char cv = e;
              switch (e) {
                case 'n': cv = '\n'; break;
                case 't': cv = '\t'; break;
                case 'r': cv = '\r'; break;
                case 'b': cv = '\b'; break;
                case 'f': cv = '\f'; break;
                case '"': cv = '"'; break;
                case '\\': cv = '\\'; break;
                case '/': cv = '/'; break;
                default:
                  buf[len++] = '\\';
                  cv = e;
              }
              buf[len++] = cv;
              ++i;
            }
          } else {
            buf[len++] = c;
            ++i;
          }
        }
        if (OB_FAIL(r) || !closed) {
          r = OB_INVALID_ARGUMENT == r ? r : OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl string");
        } else {
          out.assign_ptr(buf, len);
        }
      }
    }
    return r;
  };
  skip_space();
  if (i >= n || '{' != p[i]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl object");
    LOG_WARN("jsonl line is not a flat object", K(ret), K(line), K(line_no));
  } else {
    ++i;
    skip_space();
    if (i < n && '}' == p[i]) {
      ++i; // empty object fine
    }
    while (OB_SUCC(ret) && i < n) {
      ObString key;
      ObString value;
      bool null_value = false;
      skip_space();
      if (OB_FAIL(parse_string(key))) {
        LOG_WARN("failed to parse jsonl key", K(ret), K(line), K(line_no));
      } else if (FALSE_IT(skip_space())) {
      } else if (i >= n || ':' != p[i]) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ++i;
        skip_space();
        if (i >= n) {
          ret = OB_INVALID_ARGUMENT;
        } else if ('{' == p[i] || '[' == p[i]) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nested jsonl value");
          LOG_WARN("nested value in jsonl rejected", K(ret), K(key), K(line_no));
        } else if ('"' == p[i]) {
          if (OB_FAIL(parse_string(value))) {
          }
        } else {
          int64_t start = i;
          while (i < n && ',' != p[i] && '}' != p[i] && !isspace(p[i])) {
            ++i;
          }
          char *sbuf = static_cast<char *>(alloc.alloc(i - start));
          if (OB_ISNULL(sbuf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(sbuf, p + start, i - start);
            value.assign_ptr(sbuf, i - start);
          }
          if (0 == value.case_compare("null")) {
            null_value = true;
            value.reset();
          } else if (0 == value.case_compare("true") || 0 == value.case_compare("false")
                     || ((value.length() > 0) && (isdigit(value.ptr()[0]) || '-' == value.ptr()[0]))) {
            // scalar number/bool kept raw
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl value");
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(keys.push_back(key))) {
        } else if (OB_FAIL(vals.push_back(value))) {
        } else if (OB_FAIL(is_null.push_back(null_value))) {
        } else {
          skip_space();
          if (i < n && ',' == p[i]) {
            ++i;
          } else if (i < n && '}' == p[i]) {
            ++i;
            break;
          } else {
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      skip_space();
      if (i != n) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl trailing chars");
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl line at 1-based position");
    ret = (ret == OB_INVALID_ARGUMENT) ? ret : ret;
  }
  UNUSED(line_no);
  return ret;
}

int ObFileTableDef::infer_csv(ObIAllocator &alloc, const char *path)
{
  int ret = OB_SUCCESS;
  ObCsvFileScanner scanner;
  if (OB_FAIL(scanner.open(path, options_, alloc))) {
    LOG_WARN("failed to open csv for inference", K(ret), K(path));
  } else {
    int64_t ncols = 0;
    ObFileRawRow row0_;
    if (options_.has_header_) {
      if (OB_ISNULL(scanner.get_header())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ncols = scanner.get_header_cols();
      }
    } else {
      // determine width from first row and seed inference with it
      bool done0 = false;
      if (OB_FAIL(scanner.get_next_row(row0_, done0))) {
      } else {
        ncols = row0_.fields_.count();
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObFileColumnInfer, 16> infers;
      for (int64_t i = 0; i < ncols && OB_SUCC(ret); ++i) {
        ObFileColumnInfer inf;
        if (OB_FAIL(infers.push_back(inf))) {
        }
      }
      bool done = false;
      int64_t rows = 0;
      if (OB_SUCC(ret) && !options_.has_header_ && row0_.fields_.count() == ncols) {
        ++rows;
        for (int64_t i = 0; OB_SUCC(ret) && i < ncols; ++i) {
          if (OB_FAIL(infers.at(i).feed(row0_.fields_.at(i), row0_.fields_.at(i).empty()))) {
          }
        }
      }
      while (OB_SUCC(ret) && !done) {
        ObFileRawRow row;
        if (OB_FAIL(scanner.get_next_row(row, done))) {
        } else if (!done) {
          ++rows;
          if (row.fields_.count() != ncols) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "csv row has inconsistent column count");
            LOG_WARN("bad csv row", K(ret), K(row.fields_.count()), K(ncols), K(scanner.get_cur_line()));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < ncols; ++i) {
            bool is_null = row.fields_.at(i).empty();
            if (OB_FAIL(infers.at(i).feed(row.fields_.at(i), is_null))) {
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < ncols; ++i) {
          ObFileColumnDef def;
          ObString nm;
          if (options_.has_header_) {
            const ObFileColumnDef &hd = scanner.get_header()[i];
            nm.assign_ptr(const_cast<char *>(hd.name_.ptr()), hd.name_.length());
          } else {
            char cb[32];
            int64_t len = snprintf(cb, sizeof(cb), "column%d", i + 1);
            nm.assign_ptr(cb, len);
          }
          infers.at(i).finalize(def, nm);
          // dedupe header names: id, id -> id, id__2
          bool dup = false;
          int64_t suffix = 2;
          ObString base_name(def.name_.length(), const_cast<char *>(def.name_.ptr()));
          while (true) {
            dup = false;
            for (int64_t j = 0; j < columns_.count(); ++j) {
              if (columns_.at(j).name_ == def.name_) {
                dup = true;
                break;
              }
            }
            if (!dup) {
              break;
            }
            char cb[64];
            int64_t len = snprintf(cb, sizeof(cb), "%.*s__%ld", (int)base_name.length(), base_name.ptr(), suffix++);
            char *dup_buf = static_cast<char *>(file_alloc_.alloc(len));
            if (OB_ISNULL(dup_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }
            MEMCPY(dup_buf, cb, len);
            def.name_ = ObString(len, dup_buf);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(columns_.push_back(def))) {
          }
        }
        if (OB_SUCC(ret) && est_rows_ < rows) {
          est_rows_ = rows;
        }
      }
    }
  }
  return ret;
}

int ObFileTableDef::infer_jsonl(ObIAllocator &alloc, const char *path)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 16> key_union;
  ObSEArray<ObFileColumnInfer, 16> infers;
  ObSqlString line_buf;
  char buf[8192];
  bool pending_nl = false;
  int64_t rows = 0;
  int64_t line_no = 0;
  for (int64_t f = 0; OB_SUCC(ret) && f < files_.count(); ++f) {
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(f).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(f).ptr(), cpy);
    cbuf[cpy] = '\0';
    int fd = ::open(cbuf, O_RDONLY);
    if (fd < 0) {
      ret = OB_FILE_NOT_EXIST;
    } else {
      ssize_t n = 0;
      line_buf.reset();
      while (OB_SUCC(ret) && (n = ::read(fd, buf, sizeof(buf))) > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if ('\r' == buf[i]) {
          } else if ('\n' == buf[i]) {
            ++line_no;
            if (line_buf.empty()) {
              continue;
            }
            ObSEArray<ObString, 16> keys, vals;
            ObSEArray<bool, 16> nulls;
            if (OB_FAIL(ObJsonlSchemaHelper::parse_line(alloc, line_buf.string(), keys, vals, nulls, line_no))) {
              LOG_WARN("failed to parse jsonl line", K(ret), K(line_no));
            } else {
              ++rows;
              for (int64_t k = 0; k < keys.count(); ++k) {
                int64_t pos = -1;
                for (int64_t s = 0; -1 == pos && s < key_union.count(); ++s) {
                  if (key_union.at(s) == keys.at(k)) {
                    pos = s;
                  }
                }
                if (-1 == pos) {
                  char *kb = static_cast<char *>(file_alloc_.alloc(keys.at(k).length()));
                  if (OB_ISNULL(kb)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    break;
                  }
                  MEMCPY(kb, keys.at(k).ptr(), keys.at(k).length());
                  ObString knm(keys.at(k).length(), kb);
                  if (OB_FAIL(key_union.push_back(knm))) {
                    break;
                  }
                  ObFileColumnInfer inf;
                  if (OB_FAIL(infers.push_back(inf))) {
                    break;
                  }
                  pos = key_union.count() - 1;
                }
                if (OB_FAIL(infers.at(pos).feed(vals.at(k), nulls.at(k)))) {
                  break;
                }
              }
              // columns missing from this row feed NULL after the row parsed OK
              for (int64_t s = 0; OB_SUCC(ret) && s < key_union.count(); ++s) {
                bool found = false;
                for (int64_t k = 0; !found && k < keys.count(); ++k) {
                  if (key_union.at(s) == keys.at(k)) {
                    found = true;
                  }
                }
                if (!found && OB_FAIL(infers.at(s).feed(ObString(), true))) {
                }
              }
              line_buf.reset();
            }
          } else if (OB_FAIL(line_buf.append(buf + i, 1))) {
          }
        }
      }
      ::close(fd);
      if (OB_SUCC(ret) && !line_buf.empty()) {
        ++line_no;
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl trailing line without newline");
      }
    }
  }
  UNUSED(pending_nl);
  if (OB_SUCC(ret)) {
    for (int64_t s = 0; s < key_union.count(); ++s) {
      ObFileColumnDef def;
      infers.at(s).finalize(def, key_union.at(s));
      // union keys keep first-seen order; no dup possible
      if (OB_FAIL(columns_.push_back(def))) {
      }
    }
    if (OB_SUCC(ret) && est_rows_ < rows) {
      est_rows_ = rows;
    }
  }
  return ret;
}

int ObFileTableDef::infer_schema(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (FILE_FORMAT_PARQUET == format_) {
    ObParquetReader reader;
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(0).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(0).ptr(), cpy);
    cbuf[cpy] = '\0';
    if (OB_FAIL(reader.open(cbuf))) {
      LOG_WARN("failed to open parquet for inference", K(ret));
    } else {
      const int64_t leaves = reader.get_leaf_count();
      for (int64_t i = 0; i < leaves && OB_SUCC(ret); ++i) {
        ObString nm;
        int32_t ptype = 0;
        int32_t rep = 0;
        int32_t ct = 0;
        ObFileColumnDef def;
        if (OB_FAIL(reader.get_leaf_name(i, nm))) {
        } else if (OB_FAIL(reader.get_leaf_physical_type(i, ptype))) {
        } else if (OB_FAIL(reader.get_leaf_converted_type(i, ct))) {
        } else if (OB_FAIL(reader.get_leaf_repetition(i, rep))) {
        } else {
          char *nb = static_cast<char *>(file_alloc_.alloc(nm.length()));
          if (OB_ISNULL(nb)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(nb, nm.ptr(), nm.length());
            def.name_ = ObString(nm.length(), nb);
            // parquet physical type: 0 BOOLEAN, 1 INT32, 2 INT64, 3 INT96,
            // 4 FLOAT, 5 DOUBLE, 6 BYTE_ARRAY; converted: 6 DATE, 9/10 TIMESTAMP
            switch (ptype) {
              case 0: def.type_class_ = FILE_TYPE_BOOLEAN; break;
              case 1: def.type_class_ = (6 == ct) ? FILE_TYPE_DATE : FILE_TYPE_BIGINT; break;
              case 2: def.type_class_ = (9 == ct || 10 == ct) ? FILE_TYPE_DATETIME : FILE_TYPE_BIGINT; break;
              case 3: def.type_class_ = FILE_TYPE_DATETIME; break;
              case 4:
              case 5: def.type_class_ = FILE_TYPE_DOUBLE; break;
              case 6: def.type_class_ = FILE_TYPE_VARCHAR; break;
              default:
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("unsupported parquet physical type", K(ret), K(ptype));
                break;
            }
            def.nullable_ = (1 == rep);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(columns_.push_back(def))) {
            }
          }
        }
      }
      est_rows_ = reader.get_num_rows();
    }
  } else if (FILE_FORMAT_CSV == format_) {
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(0).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(0).ptr(), cpy);
    cbuf[cpy] = '\0';
    if (OB_FAIL(infer_csv(alloc, cbuf))) {
      LOG_WARN("failed to infer csv schema", K(ret));
    }
  } else {
    if (OB_FAIL(infer_jsonl(alloc, nullptr))) {
      LOG_WARN("failed to infer jsonl schema", K(ret));
    }
  }
  if (OB_SUCC(ret) && columns_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "empty file schema");
  }
  return ret;
}

bool ObFileTableDef::changed() const
{
  bool is_changed = false;
  for (int64_t i = 0; !is_changed && i < files_.count(); ++i) {
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(i).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(i).ptr(), cpy);
    cbuf[cpy] = '\0';
    ObFileStat st;
    if (OB_SUCCESS == st.fill(cbuf)) {
      is_changed = !st.is_same(stats_.at(i));
    } else {
      is_changed = true;
    }
  }
  return is_changed;
}

int ObFileTableDef::init_files_only(ObIAllocator &alloc, const ObString &raw_path)
{
  int ret = OB_SUCCESS;
  reset();
  options_.is_glob_ = true;
  if (OB_FAIL(expand_files(alloc, raw_path))) {
    LOG_WARN("failed to expand glob path", K(ret), K(raw_path));
  } else if (OB_UNLIKELY(files_.empty())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
  } else {
    format_ = FILE_FORMAT_GLOB;
    est_rows_ = files_.count();
    ObFileColumnDef def;
    def.name_ = ObString("file");
    def.type_class_ = FILE_TYPE_VARCHAR;
    def.nullable_ = false;
    if (OB_FAIL(columns_.push_back(def))) {
      LOG_WARN("failed to push file column", K(ret));
    }
  }
  return ret;
}

// normalize cell text per inferred column class: SQL-ize bools, datetime separator
static int normalize_file_cell(ObIAllocator &alloc, ObFileTypeClass tc, ObFileCell &cell)
{
  int ret = OB_SUCCESS;
  if (cell.is_null_ || cell.val_.empty()) {
  } else if (FILE_TYPE_BOOLEAN == tc) {
    bool is_true = (0 == cell.val_.case_compare("true"));
    bool is_false = (0 == cell.val_.case_compare("false"));
    if (is_true || is_false) {
      char *buf = static_cast<char *>(alloc.alloc(1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        buf[0] = is_true ? '1' : '0';
        cell.val_.assign_ptr(buf, 1);
      }
    }
  } else if ((FILE_TYPE_DATETIME == tc || FILE_TYPE_DATE == tc) && cell.val_.length() >= 11
             && 'T' == cell.val_.ptr()[10]) {
    char *buf = static_cast<char *>(alloc.alloc(cell.val_.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, cell.val_.ptr(), cell.val_.length());
      buf[10] = ' ';
      cell.val_.assign_ptr(buf, cell.val_.length());
    }
  }
  return ret;
}

int ObFileTableDef::read_csv_cells(ObIAllocator &alloc,
                                   ObIArray<ObSEArray<ObFileCell, 8> > &cells)
{
  int ret = OB_SUCCESS;
  const int64_t ncols = columns_.count();
  for (int64_t f = 0; OB_SUCC(ret) && f < files_.count(); ++f) {
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(f).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(f).ptr(), cpy);
    cbuf[cpy] = '\0';
    ObCsvFileScanner scanner;
    if (OB_FAIL(scanner.open(cbuf, options_, alloc))) {
      LOG_WARN("failed to open csv for read", K(ret));
    } else {
      bool done = false;
      while (OB_SUCC(ret) && !done) {
        ObFileRawRow row;
        if (OB_FAIL(scanner.get_next_row(row, done))) {
          LOG_WARN("failed to scan csv row", K(ret), K(scanner.get_cur_line()));
        } else if (!done) {
          ObSEArray<ObFileCell, 8> cells_row;
          for (int64_t i = 0; OB_SUCC(ret) && i < ncols; ++i) {
            ObFileCell cell;
            if (i < row.fields_.count()) {
              cell.val_ = row.fields_.at(i);
              cell.is_null_ = cell.val_.empty();
            } else {
              cell.is_null_ = true;
            }
            if (OB_FAIL(normalize_file_cell(alloc, columns_.at(i).type_class_, cell))) {
            } else if (OB_FAIL(cells_row.push_back(cell))) {
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(cells.push_back(cells_row))) {
          }
        }
      }
      scanner.close();
    }
  }
  return ret;
}

int ObFileTableDef::read_jsonl_cells(ObIAllocator &alloc,
                                     ObIArray<ObSEArray<ObFileCell, 8> > &cells)
{
  int ret = OB_SUCCESS;
  const int64_t ncols = columns_.count();
  ObSqlString line_buf;
  char buf[8192];
  for (int64_t f = 0; OB_SUCC(ret) && f < files_.count(); ++f) {
    char cbuf[OB_MAX_ERROR_MSG_LEN];
    int64_t cpy = std::min<int64_t>(files_.at(f).length(), (int64_t)sizeof(cbuf) - 1);
    MEMCPY(cbuf, files_.at(f).ptr(), cpy);
    cbuf[cpy] = '\0';
    int fd = ::open(cbuf, O_RDONLY);
    if (fd < 0) {
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST);
    } else {
      ssize_t n = 0;
      line_buf.reset();
      int64_t line_no = 0;
      while (OB_SUCC(ret) && (n = ::read(fd, buf, sizeof(buf))) > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if ('\r' == buf[i]) {
          } else if ('\n' == buf[i]) {
            ++line_no;
            if (line_buf.empty()) {
            } else {
              ObSEArray<ObString, 16> keys, vals;
              ObSEArray<bool, 16> nulls;
              if (OB_FAIL(ObJsonlSchemaHelper::parse_line(alloc, line_buf.string(), keys, vals, nulls, line_no))) {
                LOG_WARN("failed to parse jsonl line", K(ret), K(line_no));
              } else {
                ObSEArray<ObFileCell, 8> cells_row;
                for (int64_t c = 0; OB_SUCC(ret) && c < ncols; ++c) {
                  ObFileCell cell;
                  cell.is_null_ = true;
                  for (int64_t k = 0; k < keys.count(); ++k) {
                    if (keys.at(k) == columns_.at(c).name_) {
                      cell.is_null_ = nulls.at(k);
                      cell.val_ = vals.at(k);
                      break;
                    }
                  }
                  if (OB_FAIL(normalize_file_cell(alloc, columns_.at(c).type_class_, cell))) {
                  } else if (OB_FAIL(cells_row.push_back(cell))) {
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(cells.push_back(cells_row))) {
                }
                line_buf.reset();
              }
            }
          } else if (OB_FAIL(line_buf.append(buf + i, 1))) {
          }
        }
      }
      if (OB_SUCC(ret) && !line_buf.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "jsonl trailing line without newline");
      }
      ::close(fd);
    }
  }
  return ret;
}

int ObFileTableDef::read_parquet_cells(ObIAllocator &alloc,
                                       ObIArray<ObSEArray<ObFileCell, 8> > &cells)
{
  int ret = OB_SUCCESS;
  const int64_t ncols = columns_.count();
  char cbuf[OB_MAX_ERROR_MSG_LEN];
  int64_t cpy = std::min<int64_t>(files_.at(0).length(), (int64_t)sizeof(cbuf) - 1);
  MEMCPY(cbuf, files_.at(0).ptr(), cpy);
  cbuf[cpy] = '\0';
  ObParquetReader reader;
  if (OB_FAIL(reader.open(cbuf))) {
    LOG_WARN("failed to open parquet for read", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      int tmp_ret = reader.advance_cursor();
      if (OB_ITER_END == tmp_ret) {
        break;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to advance parquet cursor", K(ret));
        break;
      }
      const int64_t nrows = reader.get_group_num_rows();
      for (int64_t r = 0; OB_SUCC(ret) && r < nrows; ++r) {
        ObSEArray<ObFileCell, 8> cells_row;
        for (int64_t c = 0; OB_SUCC(ret) && c < ncols; ++c) {
          ObFileCell cell;
          int32_t phys = 0;
          if (OB_FAIL(reader.get_leaf_physical_type(c, phys))) {
            LOG_WARN("failed to get leaf physical type", K(ret), K(c));
          } else if (3 == phys) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "parquet INT96 leaf");
          } else {
            bool present = false;
            char numbuf[64];
            int64_t numlen = 0;
            if (0 == phys) {
              bool bv = false;
              if (OB_FAIL(reader.get_group_bool(c, r, bv, present))) {
                LOG_WARN("failed to read parquet bool cell", K(ret), K(c), K(r));
              } else if (present) {
                numbuf[0] = bv ? '1' : '0';
                numlen = 1;
              }
            } else if (1 == phys || 2 == phys) {
              int64_t iv = 0;
              if (OB_FAIL(reader.get_group_int64(c, r, iv, present))) {
                LOG_WARN("failed to read parquet int cell", K(ret), K(c), K(r));
              } else if (present) {
                numlen = snprintf(numbuf, sizeof(numbuf), "%ld", iv);
              }
            } else if (4 == phys || 5 == phys) {
              double dv = 0;
              if (OB_FAIL(reader.get_group_double(c, r, dv, present))) {
                LOG_WARN("failed to read parquet double cell", K(ret), K(c), K(r));
              } else if (present) {
                numlen = snprintf(numbuf, sizeof(numbuf), "%.*g", 15, dv);
              }
            } else {
              ObString sv;
              if (OB_FAIL(reader.get_group_string(c, r, sv, present))) {
                LOG_WARN("failed to read parquet string cell", K(ret), K(c), K(r));
              } else if (present) {
                char *sb = static_cast<char *>(alloc.alloc(sv.length()));
                if (OB_ISNULL(sb)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                } else {
                  MEMCPY(sb, sv.ptr(), sv.length());
                  cell.val_.assign_ptr(sb, sv.length());
                }
              }
            }
            if (OB_FAIL(ret)) {
            } else {
              cell.is_null_ = !present;
              if (present && numlen > 0) {
                if (numlen >= (int64_t)sizeof(numbuf)) {
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  char *cb = static_cast<char *>(alloc.alloc(numlen));
                  if (OB_ISNULL(cb)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                  } else {
                    MEMCPY(cb, numbuf, numlen);
                    cell.val_.assign_ptr(cb, numlen);
                  }
                }
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(cells_row.push_back(cell))) {
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(cells.push_back(cells_row))) {
        }
      }
    }
    reader.close();
  }
  return ret;
}

int ObFileTableDef::read_all_cells(ObIAllocator &cell_alloc,
                                   ObIArray<ObSEArray<ObFileCell, 8> > &cells)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(columns_.empty())) {
    ret = OB_NOT_INIT;
  } else if (FILE_FORMAT_GLOB == format_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < files_.count(); ++i) {
      ObSEArray<ObFileCell, 8> row;
      ObFileCell cell;
      cell.val_ = files_.at(i);
      if (OB_FAIL(row.push_back(cell))) {
      } else if (OB_FAIL(cells.push_back(row))) {
      }
    }
  } else if (FILE_FORMAT_CSV == format_) {
    if (OB_FAIL(read_csv_cells(cell_alloc, cells))) {
      LOG_WARN("failed to read csv cells", K(ret));
    }
  } else if (FILE_FORMAT_JSONL == format_) {
    if (OB_FAIL(read_jsonl_cells(cell_alloc, cells))) {
      LOG_WARN("failed to read jsonl cells", K(ret));
    }
  } else if (FILE_FORMAT_PARQUET == format_) {
    if (OB_FAIL(read_parquet_cells(cell_alloc, cells))) {
      LOG_WARN("failed to read parquet cells", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(cells.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "empty file result set");
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
