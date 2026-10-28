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
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace oceanbase
{
using namespace common;
namespace sql
{

// ---------------------------------------------------------------------------
// minimal thrift compact protocol reader (enough for parquet metadata)
// field types: 1/2 bool, 3 byte, 4 i16, 5 i32, 6 i64, 7 double,
//              8 binary, 9 list, 10 map, 11 struct
// ---------------------------------------------------------------------------
class ObThriftCompactReader
{
public:
  ObThriftCompactReader(const char *buf, int64_t len)
    : buf_(buf), pos_(0), len_(len), last_id_(0) {}
  bool eof() const { return pos_ >= len_; }
  int64_t pos() const { return pos_; }
  int read_u8(uint8_t &b);
  int read_varint(uint64_t &v);
  int read_i32(int32_t &v);
  int read_i64(int64_t &v);
  int read_binary(char *&ptr, int64_t &len);
  // fid == -1 means STOP (0x00) field header
  int next_field(int32_t &fid, int32_t &type);
  int skip_field(int32_t type);
  int list_header(int32_t &elem_type, int64_t &size);
  int enter_struct();
  int exit_struct();
private:
  const char *buf_;
  int64_t pos_;
  int64_t len_;
  int64_t last_id_;
  ObSEArray<int64_t, 8> id_stack_;
};

int ObThriftCompactReader::read_u8(uint8_t &b)
{
  int ret = OB_SUCCESS;
  if (pos_ >= len_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    b = static_cast<uint8_t>(buf_[pos_++]);
  }
  return ret;
}

int ObThriftCompactReader::read_varint(uint64_t &v)
{
  int ret = OB_SUCCESS;
  v = 0;
  int shift = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    uint8_t b = 0;
    if (OB_FAIL(read_u8(b))) {
    } else {
      v |= (static_cast<uint64_t>(b & 0x7F)) << shift;
      if (0 == (b & 0x80)) {
        done = true;
      } else if (shift += 7, shift > 63) {
        ret = OB_INVALID_DATA;
      }
    }
  }
  return ret;
}

int ObThriftCompactReader::read_i32(int32_t &v)
{
  int ret = OB_SUCCESS;
  uint64_t z = 0;
  if (OB_FAIL(read_varint(z))) {
  } else if (z <= 0xFFFFFFFFULL) {
    int64_t s = static_cast<int64_t>(z);
    v = static_cast<int32_t>((s >> 1) ^ -(s & 1));
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int ObThriftCompactReader::read_i64(int64_t &v)
{
  int ret = OB_SUCCESS;
  uint64_t z = 0;
  if (OB_FAIL(read_varint(z))) {
  } else {
    v = static_cast<int64_t>(z >> 1) ^ -static_cast<int64_t>(z & 1);
  }
  return ret;
}

int ObThriftCompactReader::read_binary(char *&ptr, int64_t &len)
{
  int ret = OB_SUCCESS;
  uint64_t n = 0;
  if (OB_FAIL(read_varint(n))) {
  } else if (n > static_cast<uint64_t>(len_ - pos_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ptr = const_cast<char *>(buf_ + pos_);
    len = static_cast<int64_t>(n);
    pos_ += len;
  }
  return ret;
}

int ObThriftCompactReader::next_field(int32_t &fid, int32_t &type)
{
  int ret = OB_SUCCESS;
  uint8_t b = 0;
  if (OB_FAIL(read_u8(b))) {
  } else {
    type = b & 0x0F;
    int delta = b >> 4;
    if (0 == type && 0 == delta) {
      fid = -1; // STOP
    } else if (0 == delta) {
      int32_t id = 0;
      if (OB_FAIL(read_i32(id))) {
      } else {
        fid = id;
        last_id_ = id;
      }
    } else {
      fid = static_cast<int32_t>(last_id_ + delta);
      last_id_ = fid;
    }
  }
  return ret;
}

int ObThriftCompactReader::list_header(int32_t &elem_type, int64_t &size)
{
  int ret = OB_SUCCESS;
  uint8_t b = 0;
  if (OB_FAIL(read_u8(b))) {
  } else {
    elem_type = b & 0x0F;
    size = b >> 4;
    if (0x0F == size) {
      uint64_t n = 0;
      if (OB_FAIL(read_varint(n))) {
      } else {
        size = static_cast<int64_t>(n);
      }
    }
  }
  return ret;
}

int ObThriftCompactReader::enter_struct()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(id_stack_.push_back(last_id_))) {
  } else {
    last_id_ = 0;
  }
  return ret;
}

int ObThriftCompactReader::exit_struct()
{
  int ret = OB_SUCCESS;
  if (id_stack_.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    last_id_ = id_stack_.at(id_stack_.count() - 1);
    id_stack_.pop_back();
  }
  return ret;
}

int ObThriftCompactReader::skip_field(int32_t type)
{
  int ret = OB_SUCCESS;
  switch (type) {
    case 1: // BOOLEAN_TRUE
    case 2: // BOOLEAN_FALSE
      break;
    case 3: { // BYTE
      uint8_t b = 0;
      ret = read_u8(b);
      break;
    }
    case 4: // I16
    case 5: { // I32
      int32_t v = 0;
      ret = read_i32(v);
      break;
    }
    case 6: { // I64
      int64_t v = 0;
      ret = read_i64(v);
      break;
    }
    case 7: { // DOUBLE
      char tmp[8];
      ret = (pos_ + 8 > len_) ? OB_BUF_NOT_ENOUGH : (MEMCPY(tmp, buf_ + pos_, 8), pos_ += 8, OB_SUCCESS);
      break;
    }
    case 8: { // BINARY
      char *p = nullptr;
      int64_t l = 0;
      ret = read_binary(p, l);
      break;
    }
    case 9: { // LIST
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(list_header(et, n))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(skip_field(et))) {
          }
        }
      }
      break;
    }
    case 10: { // MAP
      uint64_t n = 0;
      if (OB_FAIL(read_varint(n))) {
      } else if (n > 0) {
        uint8_t kv = 0;
        if (OB_FAIL(read_u8(kv))) {
        } else {
        int32_t kt = kv >> 4;
        int32_t vt = kv & 0x0F;
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(skip_field(kt))) {
          } else if (OB_FAIL(skip_field(vt))) {
          }
        }
        }
      }
      break;
    }
    case 11: { // SET
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(list_header(et, n))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(skip_field(et))) {
          }
        }
      }
      break;
    }
    case 12: { // STRUCT
      if (OB_FAIL(enter_struct())) {
      } else {
        int32_t fid = 0;
        int32_t ft = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(next_field(fid, ft))) {
          } else if (fid < 0) {
            break;
          } else if (OB_FAIL(skip_field(ft))) {
          }
        }
        int tmp_ret = exit_struct();
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      break;
    }
    default:
      ret = OB_INVALID_DATA;
      LOG_WARN("unknown thrift compact type", K(ret), K(type));
      break;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// snappy raw format decoder (parquet SNAPPY codec uses raw snappy, no framing)
// ---------------------------------------------------------------------------
static int snappy_decode(const char *in, int64_t in_len, char *out, int64_t out_cap,
                         int64_t &out_len)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t ulen = 0;
  {
    int shift = 0;
    bool done = false;
    uint64_t v = 0;
    while (OB_SUCC(ret) && !done) {
      if (ip >= in_len) {
        ret = OB_INVALID_DATA;
      } else {
        uint8_t b = static_cast<uint8_t>(in[ip++]);
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (0 == (b & 0x80)) {
          done = true;
        } else if (shift += 7, shift > 63) {
          ret = OB_INVALID_DATA;
        }
      }
    }
    ulen = v;
  }
  if (OB_FAIL(ret)) {
  } else if (ulen > out_cap) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("snappy output exceeds page size", K(ret), K(ulen), K(out_cap));
  }
  int64_t op = 0;
  while (OB_SUCC(ret) && ip < in_len && op < ulen) {
    uint8_t tag = static_cast<uint8_t>(in[ip++]);
    int kind = tag & 0x03;
    if (0 == kind) {
      // literal
      int64_t len = (tag >> 2) + 1;
      if (len > 60) {
        int64_t extra = len - 60; // 1..4 little-endian length bytes follow
        if (ip + extra > in_len) {
          ret = OB_INVALID_DATA;
        } else {
          int64_t l = 0;
          for (int64_t i = 0; i < extra; ++i) {
            l |= static_cast<int64_t>(static_cast<uint8_t>(in[ip + i])) << (8 * i);
          }
          ip += extra;
          len = l + 1;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ip + len > in_len || op + len > out_cap) {
        ret = OB_INVALID_DATA;
      } else {
        MEMCPY(out + op, in + ip, len);
        ip += len;
        op += len;
      }
    } else {
      int64_t len = 0;
      int64_t off = 0;
      if (1 == kind) { // copy, 1-byte offset, length 4..11
        if (ip >= in_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = 4 + ((tag >> 2) & 0x07);
          off = ((tag >> 5) << 8) | static_cast<int64_t>(static_cast<uint8_t>(in[ip++]));
        }
      } else if (2 == kind) { // copy, 2-byte little-endian offset
        if (ip + 2 > in_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          off = static_cast<int64_t>(static_cast<uint8_t>(in[ip]))
              | (static_cast<int64_t>(static_cast<uint8_t>(in[ip + 1])) << 8);
          ip += 2;
        }
      } else { // copy, 4-byte little-endian offset
        if (ip + 4 > in_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          off = static_cast<int64_t>(static_cast<uint8_t>(in[ip]))
              | (static_cast<int64_t>(static_cast<uint8_t>(in[ip + 1])) << 8)
              | (static_cast<int64_t>(static_cast<uint8_t>(in[ip + 2])) << 16)
              | (static_cast<int64_t>(static_cast<uint8_t>(in[ip + 3])) << 24);
          ip += 4;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (off <= 0 || off > op || op + len > ulen) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bad snappy backref", K(ret), K(off), K(op), K(len));
      } else {
        for (int64_t i = 0; i < len; ++i) {
          out[op + i] = out[op + i - off]; // overlapping copy is legal
        }
        op += len;
      }
    }
  }
  if (OB_SUCC(ret)) {
    out_len = op;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// RLE / bit-packed hybrid decoder
// ---------------------------------------------------------------------------
static int hybrid_decode(const char *buf, int64_t len, int32_t bit_width, int64_t want,
                         ObIArray<int64_t> &out)
{
  // want < 0: consume until bytes exhausted
  int ret = OB_SUCCESS;
  if (bit_width <= 0 || bit_width > 64) {
    ret = OB_INVALID_DATA;
    LOG_WARN("bad hybrid bit width", K(ret), K(bit_width));
  }
  int64_t ip = 0;
  int64_t consumed_bytes = 0; // bytes of value data after the last header
  while (OB_SUCC(ret) && (want < 0 || out.count() < want)) {
    if (ip >= len) {
      if (want >= 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("hybrid stream ended early", K(ret), K(want), K(out.count()));
      }
      break;
    }
    uint64_t header = 0;
    {
      int shift = 0;
      while (OB_SUCC(ret)) {
        if (ip >= len) {
          ret = OB_INVALID_DATA;
          break;
        }
        uint8_t b = static_cast<uint8_t>(buf[ip++]);
        header |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (0 == (b & 0x80)) {
          break;
        } else if (shift += 7, shift > 63) {
          ret = OB_INVALID_DATA;
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == (header & 1)) {
      // rle: (header >> 1) repetitions of one bit_width value at byte offset ip
      int64_t repeat = header >> 1;
      int64_t need_bytes = (bit_width + 7) / 8;
      if (ip + need_bytes > len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        uint64_t v = 0;
        for (int64_t i = 0; i < need_bytes; ++i) {
          v |= static_cast<uint64_t>(static_cast<uint8_t>(buf[ip + i])) << (8 * i);
        }
        if ((bit_width % 8) != 0 && (v >> bit_width) != 0) {
          // value may span partial bits; re-extract LSB-aligned by reading bits directly
          v = 0;
          for (int64_t bit = 0; bit < bit_width; ++bit) {
            int64_t byte = ip + bit / 8;
            int64_t bpos = bit % 8;
            v |= static_cast<uint64_t>((static_cast<uint8_t>(buf[byte]) >> bpos) & 1) << bit;
          }
        } else {
          v &= (bit_width == 64 ? ~0ULL : (1ULL << bit_width) - 1);
        }
        ip += need_bytes;
        for (int64_t i = 0; OB_SUCC(ret) && i < repeat; ++i) {
          if (OB_FAIL(out.push_back(static_cast<int64_t>(v)))) {
          }
        }
      }
    } else {
      // bit-packed: (header >> 1) groups of 8 values, LSB-first across bytes
      int64_t groups = header >> 1;
      int64_t nvals = groups * 8;
      int64_t run_bytes = groups * bit_width;
      if (ip + run_bytes > len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        int64_t base = ip;
        for (int64_t i = 0; OB_SUCC(ret) && i < nvals; ++i) {
          uint64_t v = 0;
          for (int64_t bit = 0; bit < bit_width; ++bit) {
            int64_t idx = i * bit_width + bit;
            int64_t byte = base + idx / 8;
            int64_t bpos = idx % 8;
            v |= static_cast<uint64_t>((static_cast<uint8_t>(buf[byte]) >> bpos) & 1) << bit;
          }
          if (OB_FAIL(out.push_back(static_cast<int64_t>(v)))) {
          }
        }
        ip = base + run_bytes;
      }
    }
    UNUSED(consumed_bytes);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObParquetReader::Impl
// ---------------------------------------------------------------------------
class ObParquetReader::Impl
{
public:
  struct LeafCol
  {
    LeafCol() : name_(), physical_(0), repetition_(0), converted_(0) {}
    ObString name_;
    int32_t physical_;
    int32_t repetition_;
    int32_t converted_;
    TO_STRING_KV(K_(name), K_(physical), K_(repetition), K_(converted));
  };

  struct ChunkLoc
  {
    ChunkLoc() : start_off_(-1), comp_size_(-1), num_values_(0), leaf_idx_(-1),
                 physical_(0), repetition_(0), codec_(0), dict_off_(-1), data_off_(-1) {}
    int64_t start_off_;
    int64_t comp_size_;
    int64_t num_values_;
    int64_t leaf_idx_;
    int32_t physical_;
    int32_t repetition_;
    int32_t codec_;
    int64_t dict_off_;
    int64_t data_off_;
    TO_STRING_KV(K_(start_off), K_(comp_size), K_(num_values), K_(leaf_idx),
                 K_(physical), K_(repetition), K_(codec), K_(dict_off), K_(data_off));
  };

  struct RowGroupLoc
  {
    RowGroupLoc() : num_rows_(0), chunks_() {}
    int64_t num_rows_;
    ObSEArray<ChunkLoc, 8> chunks_;
    TO_STRING_KV(K_(num_rows), K_(chunks));
  };

  // per-leaf row-aligned decoded storage for the current row group
  struct DecodedCol
  {
    DecodedCol() : present_(), ints_(), dbls_(), bools_(), strs_() {}
    void reset() { present_.reset(); ints_.reset(); dbls_.reset(); bools_.reset(); strs_.reset(); }
    ObSEArray<bool, 64> present_;
    ObSEArray<int64_t, 64> ints_;   // physical 1/2
    ObSEArray<double, 64> dbls_;    // physical 4/5
    ObSEArray<bool, 64> bools_;     // physical 0
    ObSEArray<ObString, 64> strs_;  // physical 6
  };

  // scratch page values, one entry per (present) row
  struct TmpVals
  {
    TmpVals() : ints_(), dbls_(), bools_(), strs_() {}
    void reset() { ints_.reset(); dbls_.reset(); bools_.reset(); strs_.reset(); }
    ObSEArray<int64_t, 64> ints_;
    ObSEArray<double, 64> dbls_;
    ObSEArray<bool, 64> bools_;
    ObSEArray<ObString, 64> strs_;
  };

  Impl()
    : arena_(ObMemAttr("ParquetRd")),
      fd_(-1), file_size_(0), num_rows_(0), cur_group_(-1), opened_(false),
      leaves_(), groups_(), decoded_() {}
  ~Impl() { close_raw(); }
  int open_raw(const char *path);
  void close_raw();
  int advance();
  int decode_chunk(const ChunkLoc &chunk, const char *raw, int64_t raw_len);
  int read_range(int64_t off, int64_t len, char *&buf);
  int64_t group_rows() const
  {
    return (cur_group_ >= 0 && cur_group_ < groups_.count())
         ? groups_.at(cur_group_).num_rows_ : 0;
  }

private:
  int parse_footer(const char *buf, int64_t len);
  int parse_schema_element(ObThriftCompactReader &tc, bool &is_leaf);
  int parse_row_group(ObThriftCompactReader &tc, RowGroupLoc &rg);
  int parse_column_chunk(ObThriftCompactReader &tc, ChunkLoc &cc);
  int parse_column_meta(ObThriftCompactReader &tc, ChunkLoc &cc);
  int decode_plain(const char *p, int64_t len, int32_t physical, int64_t count, TmpVals &tv);
  int read_le32(const char *p, int32_t &v);

public:
  ObArenaAllocator arena_;
  int fd_;
  int64_t file_size_;
  int64_t num_rows_;
  int64_t cur_group_; // -1 before first advance
  bool opened_;
  ObSEArray<LeafCol, 8> leaves_;
  ObSEArray<RowGroupLoc, 4> groups_;
  ObSEArray<DecodedCol, 8> decoded_;
};

void ObParquetReader::Impl::close_raw()
{
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  opened_ = false;
}

int ObParquetReader::Impl::read_le32(const char *p, int32_t &v)
{
  int ret = OB_SUCCESS;
  uint32_t u = static_cast<uint32_t>(static_cast<uint8_t>(p[0]))
             | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8)
             | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16)
             | (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
  if (u > 0x7FFFFFFFUL) {
    ret = OB_INVALID_DATA;
  } else {
    v = static_cast<int32_t>(u);
  }
  return ret;
}

int ObParquetReader::Impl::read_range(int64_t off, int64_t len, char *&buf)
{
  int ret = OB_SUCCESS;
  char *b = nullptr;
  if (off < 0 || len < 0 || off + len > file_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parquet range out of file", K(ret), K(off), K(len), K(file_size_));
  } else if (len > 256 * 1024 * 1024) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parquet read range too large", K(ret), K(len));
  } else if (OB_ISNULL(b = static_cast<char *>(arena_.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ssize_t n = ::pread(fd_, b, len, off);
    if (n != len) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to pread parquet range", K(ret), K(off), K(len), K(n), K(errno));
    } else {
      buf = b;
    }
  }
  return ret;
}

int ObParquetReader::Impl::open_raw(const char *path)
{
  int ret = OB_SUCCESS;
  struct stat st;
  if (opened_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != ::stat(path, &st)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
  } else if (st.st_size < 12) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parquet file too small");
  } else if ((fd_ = ::open(path, O_RDONLY)) < 0) {
    ret = OB_FILE_NOT_EXIST;
  } else {
    file_size_ = st.st_size;
    char *tail = nullptr;
    if (OB_FAIL(read_range(file_size_ - 8, 8, tail))) {
    } else if (0 != MEMCMP(tail + 4, "PAR1", 4)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "not a parquet file");
    } else {
      int32_t footer_len = 0;
      if (OB_FAIL(read_le32(tail, footer_len))) {
      } else if (footer_len <= 0 || file_size_ - 8 - footer_len < 4) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parquet footer length");
      } else {
        char *footer = nullptr;
        if (OB_FAIL(read_range(file_size_ - 8 - footer_len, footer_len, footer))) {
        } else if (OB_FAIL(parse_footer(footer, footer_len))) {
          LOG_WARN("failed to parse parquet footer", K(ret));
        } else if (leaves_.empty() || groups_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parquet schema");
        } else {
          opened_ = true;
        }
      }
    }
    if (OB_FAIL(ret)) {
      close_raw();
    }
  }
  return ret;
}

int ObParquetReader::Impl::parse_footer(const char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  ObThriftCompactReader tc(buf, len);
  int32_t fid = 0;
  int32_t ftype = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tc.next_field(fid, ftype))) {
    } else if (fid < 0) {
      break;
    } else if (1 == fid && 5 == ftype) {
      int32_t version = 0;
      if (OB_FAIL(tc.read_i32(version))) {
      } else if (version < 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("parquet version unsupported", K(ret), K(version));
      }
    } else if (2 == fid && 9 == ftype) {
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(tc.list_header(et, n))) {
      } else if (12 != et) {
        ret = OB_INVALID_DATA;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(tc.enter_struct())) {
          } else {
            bool is_leaf = false;
            if (OB_FAIL(parse_schema_element(tc, is_leaf))) {
            }
            if (OB_SUCC(ret)) {
              ret = tc.exit_struct();
            }
          }
        }
      }
    } else if (3 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(num_rows_))) {
      }
    } else if (4 == fid && 9 == ftype) {
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(tc.list_header(et, n))) {
      } else if (12 != et) {
        ret = OB_INVALID_DATA;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(tc.enter_struct())) {
          } else {
            RowGroupLoc rg;
            if (OB_FAIL(parse_row_group(tc, rg))) {
            } else if (OB_FAIL(groups_.push_back(rg))) {
            }
            if (OB_SUCC(ret)) {
              ret = tc.exit_struct();
            }
          }
        }
      }
    } else if (OB_FAIL(tc.skip_field(ftype))) {
    }
  }
  // chunks must line up with leaves
  for (int64_t g = 0; OB_SUCC(ret) && g < groups_.count(); ++g) {
    if (groups_.at(g).chunks_.count() != leaves_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("chunk/leaf count mismatch", K(ret), K(g),
               K(groups_.at(g).chunks_.count()), K(leaves_.count()));
    }
  }
  return ret;
}

int ObParquetReader::Impl::parse_schema_element(ObThriftCompactReader &tc, bool &is_leaf)
{
  int ret = OB_SUCCESS;
  is_leaf = false;
  LeafCol col;
  int32_t num_children = 0;
  const char *name_p = nullptr;
  int64_t name_len = 0;
  int32_t fid = 0;
  int32_t ftype = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tc.next_field(fid, ftype))) {
    } else if (fid < 0) {
      break;
    } else if (1 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(col.physical_))) {
      }
    } else if (3 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(col.repetition_))) {
      }
    } else if (4 == fid && 8 == ftype) {
      char *p = nullptr;
      int64_t l = 0;
      if (OB_FAIL(tc.read_binary(p, l))) {
      } else {
        name_p = p;
        name_len = l;
      }
    } else if (5 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(num_children))) {
      }
    } else if (6 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(col.converted_))) {
      }
    } else if (OB_FAIL(tc.skip_field(ftype))) {
    }
  }
  if (OB_SUCC(ret) && 0 == num_children) {
    if (OB_ISNULL(name_p)) {
      ret = OB_INVALID_DATA;
    } else {
      char *nb = static_cast<char *>(arena_.alloc(name_len));
      if (OB_ISNULL(nb)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(nb, name_p, name_len);
        col.name_.assign_ptr(nb, name_len);
        is_leaf = true;
        if (OB_FAIL(leaves_.push_back(col))) {
        }
      }
    }
  }
  return ret;
}

int ObParquetReader::Impl::parse_row_group(ObThriftCompactReader &tc, RowGroupLoc &rg)
{
  int ret = OB_SUCCESS;
  int32_t fid = 0;
  int32_t ftype = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tc.next_field(fid, ftype))) {
    } else if (fid < 0) {
      break;
    } else if (1 == fid && 9 == ftype) {
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(tc.list_header(et, n))) {
      } else if (12 != et) {
        ret = OB_INVALID_DATA;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(tc.enter_struct())) {
          } else {
            ChunkLoc cc;
            if (OB_FAIL(parse_column_chunk(tc, cc))) {
            } else if (OB_FAIL(rg.chunks_.push_back(cc))) {
            }
            if (OB_SUCC(ret)) {
              ret = tc.exit_struct();
            }
          }
        }
      }
    } else if (2 == fid && 6 == ftype) {
      int64_t tbs = 0;
      if (OB_FAIL(tc.read_i64(tbs))) {
      }
    } else if (3 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(rg.num_rows_))) {
      }
    } else if (OB_FAIL(tc.skip_field(ftype))) {
    }
  }
  return ret;
}

int ObParquetReader::Impl::parse_column_chunk(ObThriftCompactReader &tc, ChunkLoc &cc)
{
  int ret = OB_SUCCESS;
  int32_t fid = 0;
  int32_t ftype = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tc.next_field(fid, ftype))) {
    } else if (fid < 0) {
      break;
    } else if (1 == fid && 8 == ftype) {
      char *p = nullptr;
      int64_t l = 0;
      if (OB_FAIL(tc.read_binary(p, l))) {
      } else if (l > 0) {
        ret = OB_NOT_SUPPORTED; // partitioned/hive-style external paths not in scope
        LOG_WARN("parquet chunk file_path not supported", K(ret));
      }
    } else if (2 == fid && 6 == ftype) {
      int64_t off = 0;
      if (OB_FAIL(tc.read_i64(off))) {
      } else if (off >= 0) {
        cc.start_off_ = off; // first-page hint; refined by meta offsets below
      }
    } else if (3 == fid && 12 == ftype) {
      if (OB_FAIL(tc.enter_struct())) {
      } else if (OB_FAIL(parse_column_meta(tc, cc))) {
      } else {
        ret = tc.exit_struct();
      }
    } else if (OB_FAIL(tc.skip_field(ftype))) {
    }
  }
  if (OB_SUCC(ret)) {
    if (cc.dict_off_ >= 0) {
      cc.start_off_ = cc.dict_off_;
    } else if (cc.data_off_ >= 0) {
      cc.start_off_ = cc.data_off_;
    }
    if (cc.start_off_ < 0 || cc.comp_size_ <= 0 || cc.num_values_ < 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("bad parquet chunk location", K(ret), K(cc));
    }
  }
  return ret;
}

int ObParquetReader::Impl::parse_column_meta(ObThriftCompactReader &tc, ChunkLoc &cc)
{
  int ret = OB_SUCCESS;
  int32_t fid = 0;
  int32_t ftype = 0;
  const char *last_part = nullptr;
  int64_t last_len = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tc.next_field(fid, ftype))) {
    } else if (fid < 0) {
      break;
    } else if (1 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(cc.physical_))) {
      }
    } else if (2 == fid && 9 == ftype) {
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(tc.list_header(et, n))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          int32_t v = 0;
          if (OB_FAIL(tc.read_i32(v))) { // encodings list<i32>
          }
        }
      }
    } else if (3 == fid && 9 == ftype) {
      int32_t et = 0;
      int64_t n = 0;
      if (OB_FAIL(tc.list_header(et, n))) {
      } else if (8 != et) {
        ret = OB_INVALID_DATA;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          char *p = nullptr;
          int64_t l = 0;
          if (OB_FAIL(tc.read_binary(p, l))) {
          } else {
            last_part = p; // flat schema: last part is the leaf name
            last_len = l;
          }
        }
      }
    } else if (4 == fid && 5 == ftype) {
      if (OB_FAIL(tc.read_i32(cc.codec_))) {
      }
    } else if (5 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(cc.num_values_))) {
      }
    } else if (7 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(cc.comp_size_))) {
      }
    } else if (9 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(cc.data_off_))) {
      }
    } else if (10 == fid && 6 == ftype) {
      int64_t off = 0;
      if (OB_FAIL(tc.read_i64(off))) {
      }
    } else if (11 == fid && 6 == ftype) {
      if (OB_FAIL(tc.read_i64(cc.dict_off_))) {
      }
    } else if (OB_FAIL(tc.skip_field(ftype))) {
    }
  }
  if (OB_SUCC(ret)) {
    cc.leaf_idx_ = -1;
    for (int64_t i = 0; -1 == cc.leaf_idx_ && i < leaves_.count(); ++i) {
      if (leaves_.at(i).name_.length() == last_len
          && 0 == MEMCMP(leaves_.at(i).name_.ptr(), last_part, last_len)) {
        cc.leaf_idx_ = i;
        cc.repetition_ = leaves_.at(i).repetition_;
      }
    }
    if (cc.leaf_idx_ < 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("chunk path does not match any leaf", K(ret), K(last_len));
    } else if (cc.repetition_ > 1) { // REPEATED not supported
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObParquetReader::Impl::decode_plain(const char *p, int64_t len, int32_t physical,
                                        int64_t count, TmpVals &tv)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    switch (physical) {
      case 0: { // BOOLEAN bit-packed, LSB first
        int64_t byte = i / 8;
        int64_t bit = i % 8;
        if (byte >= len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else if (OB_FAIL(tv.bools_.push_back((static_cast<uint8_t>(p[byte]) >> bit) & 1))) {
        }
        break;
      }
      case 1: { // INT32
        if (pos + 4 > len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          int32_t v = 0;
          if (OB_FAIL(read_le32(p + pos, v))) {
          } else {
            pos += 4;
            if (OB_FAIL(tv.ints_.push_back(v))) {
            }
          }
        }
        break;
      }
      case 2: { // INT64
        if (pos + 8 > len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          int64_t v = 0;
          for (int k = 0; k < 8; ++k) {
            v |= static_cast<int64_t>(static_cast<uint8_t>(p[pos + k])) << (8 * k);
          }
          pos += 8;
          if (OB_FAIL(tv.ints_.push_back(v))) {
          }
        }
        break;
      }
      case 4: { // FLOAT
        if (pos + 4 > len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          float v = 0;
          for (int k = 0; k < 4; ++k) {
            reinterpret_cast<uint8_t*>(&v)[k] = static_cast<uint8_t>(p[pos + k]);
          }
          pos += 4;
          if (OB_FAIL(tv.dbls_.push_back(v))) {
          }
        }
        break;
      }
      case 5: { // DOUBLE
        if (pos + 8 > len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          double v = 0;
          for (int k = 0; k < 8; ++k) {
            reinterpret_cast<uint8_t*>(&v)[k] = static_cast<uint8_t>(p[pos + k]);
          }
          pos += 8;
          if (OB_FAIL(tv.dbls_.push_back(v))) {
          }
        }
        break;
      }
      case 6: { // BYTE_ARRAY
        int32_t slen = 0;
        if (pos + 4 > len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else if (OB_FAIL(read_le32(p + pos, slen))) {
        } else {
          pos += 4;
          if (slen < 0 || pos + slen > len) {
            ret = OB_BUF_NOT_ENOUGH;
          } else {
            ObString s;
            s.assign_ptr(p + pos, slen);
            pos += slen;
            if (OB_FAIL(tv.strs_.push_back(s))) {
            }
          }
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet physical type", K(ret), K(physical));
        break;
    }
  }
  return ret;
}

int ObParquetReader::Impl::decode_chunk(const ChunkLoc &chunk, const char *raw, int64_t raw_len)
{
  int ret = OB_SUCCESS;
  DecodedCol &col = decoded_.at(chunk.leaf_idx_);
  TmpVals dict;
  bool have_dict = false;
  int64_t off = 0;
  int64_t rows_done = 0;
  while (OB_SUCC(ret) && off < raw_len && rows_done < chunk.num_values_) {
    ObThriftCompactReader tc(raw + off, raw_len - off);
    int32_t page_type = -1;
    int32_t uncomp_size = 0;
    int32_t comp_size = 0;
    int32_t dph_rows = 0;
    int32_t dph_enc = -1;
    int32_t dict_num_values = 0;
    bool v2_page = false;
    int32_t v2_num_values = 0;
    int32_t v2_num_nulls = 0;
    int32_t v2_enc = -1;
    int32_t v2_def_len = 0;
    int32_t v2_rep_len = 0;
    bool v2_is_comp = true;
    int32_t fid = 0;
    int32_t ftype = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tc.next_field(fid, ftype))) {
      } else if (fid < 0) {
        break;
      } else if (1 == fid && 5 == ftype) {
        if (OB_FAIL(tc.read_i32(page_type))) {
        }
      } else if (2 == fid && 5 == ftype) {
        if (OB_FAIL(tc.read_i32(uncomp_size))) {
        }
      } else if (3 == fid && 5 == ftype) {
        if (OB_FAIL(tc.read_i32(comp_size))) {
        }
      } else if (5 == fid && 12 == ftype) { // DataPageHeader (v1)
        if (OB_FAIL(tc.enter_struct())) {
        } else {
          while (OB_SUCC(ret)) {
            int32_t sfid = 0;
            int32_t sftype = 0;
            if (OB_FAIL(tc.next_field(sfid, sftype))) {
            } else if (sfid < 0) {
              break;
            } else if (1 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(dph_rows))) {
              }
            } else if (2 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(dph_enc))) {
              }
            } else if (OB_FAIL(tc.skip_field(sftype))) {
            }
          }
          if (OB_SUCC(ret)) {
            ret = tc.exit_struct();
          }
        }
      } else if (7 == fid && 12 == ftype) { // DictionaryPageHeader
        if (OB_FAIL(tc.enter_struct())) {
        } else {
          while (OB_SUCC(ret)) {
            int32_t sfid = 0;
            int32_t sftype = 0;
            if (OB_FAIL(tc.next_field(sfid, sftype))) {
            } else if (sfid < 0) {
              break;
            } else if (1 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(dict_num_values))) {
              }
            } else if (OB_FAIL(tc.skip_field(sftype))) {
            }
          }
          if (OB_SUCC(ret)) {
            ret = tc.exit_struct();
          }
        }
      } else if (8 == fid && 12 == ftype) { // DataPageHeaderV2
        if (OB_FAIL(tc.enter_struct())) {
        } else {
          while (OB_SUCC(ret)) {
            int32_t sfid = 0;
            int32_t sftype = 0;
            if (OB_FAIL(tc.next_field(sfid, sftype))) {
            } else if (sfid < 0) {
              break;
            } else if (1 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(v2_num_values))) {
              }
            } else if (2 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(v2_num_nulls))) {
              }
            } else if (4 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(v2_enc))) {
              }
            } else if (5 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(v2_def_len))) {
              }
            } else if (6 == sfid && 5 == sftype) {
              if (OB_FAIL(tc.read_i32(v2_rep_len))) {
              }
            } else if (7 == sfid && (1 == sftype || 2 == sftype)) {
              v2_is_comp = (1 == sftype);
            } else if (OB_FAIL(tc.skip_field(sftype))) {
            }
          }
          if (OB_SUCC(ret)) {
            ret = tc.exit_struct();
          }
        }
      } else if (fid > 0 && OB_FAIL(tc.skip_field(ftype))) {
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to parse parquet page header", K(ret), K(off), K(raw_len));
    } else if (comp_size <= 0 || uncomp_size <= 0 || off + tc.pos() + comp_size > raw_len) {
      ret = OB_INVALID_DATA;
      LOG_WARN("bad parquet page", K(ret), K(page_type), K(comp_size), K(off), K(tc.pos()));
    } else {
      int64_t body_off = off + tc.pos();
      const char *body = raw + body_off;
      const char *dec = body;
      int64_t dec_len = comp_size;
      v2_page = (3 == page_type);
      const int64_t lv_len = v2_page ? (static_cast<int64_t>(v2_def_len) + v2_rep_len) : 0;
      const char *snap_in = body;
      int64_t snap_len = comp_size;
      int64_t snap_cap = uncomp_size;
      bool do_snappy = (1 == chunk.codec_);
      if (OB_UNLIKELY(0 != chunk.codec_ && 1 != chunk.codec_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet codec", K(ret), K(chunk.codec_));
      } else if (v2_page && (lv_len < 0 || lv_len > comp_size || lv_len > uncomp_size)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bad v2 level lengths", K(ret), K(lv_len), K(comp_size), K(uncomp_size));
      } else if (v2_page) {
        // v2 pages store def/rep levels uncompressed ahead of the values
        snap_in = body + lv_len;
        snap_len = comp_size - lv_len;
        snap_cap = uncomp_size - lv_len;
        do_snappy = do_snappy && v2_is_comp;
        if (!do_snappy) {
          dec = body + lv_len;
          dec_len = comp_size - lv_len;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (do_snappy) {
        char *dbuf = nullptr;
        if (snap_cap <= 0 || snap_len <= 0) {
          ret = OB_INVALID_DATA;
        } else if (OB_ISNULL(dbuf = static_cast<char *>(arena_.alloc(snap_cap)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          int64_t dl = 0;
          if (OB_FAIL(snappy_decode(snap_in, snap_len, dbuf, snap_cap, dl))) {
            LOG_WARN("snappy decode failed", K(ret), K(snap_len), K(snap_cap));
          } else {
            dec = dbuf;
            dec_len = dl;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (2 == page_type) { // DICTIONARY_PAGE: PLAIN values
        dict.reset();
        if (OB_FAIL(decode_plain(dec, dec_len, chunk.physical_, dict_num_values, dict))) {
          LOG_WARN("failed to decode dictionary page", K(ret), K(dict_num_values));
        } else {
          have_dict = true;
        }
      } else if (0 == page_type) { // DATA_PAGE v1
        if (v2_page) { // unreachable given page_type==0
          ret = OB_NOT_SUPPORTED;
        } else if (chunk.repetition_ > 1) {
          ret = OB_NOT_SUPPORTED;
        } else {
          const int64_t max_def = (1 == chunk.repetition_) ? 1 : 0;
          int64_t bp = 0;
          ObSEArray<int64_t, 64> defs;
          if (dph_rows <= 0) {
            ret = OB_INVALID_DATA;
          } else if (max_def > 0) {
            int32_t dlen = 0;
            if (bp + 4 > dec_len) {
              ret = OB_BUF_NOT_ENOUGH;
            } else if (OB_FAIL(read_le32(dec + bp, dlen))) {
            } else {
              bp += 4;
              if (dlen < 0 || bp + dlen > dec_len) {
                ret = OB_INVALID_DATA;
              } else if (OB_FAIL(hybrid_decode(dec + bp, dlen, 1, dph_rows, defs))) {
                LOG_WARN("failed to decode def levels", K(ret), K(dph_rows));
              } else {
                bp += dlen;
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            const char *vals = dec + bp;
            int64_t vals_len = dec_len - bp;
            int64_t n_present = dph_rows;
            if (max_def > 0) {
              n_present = 0;
              for (int64_t i = 0; i < dph_rows; ++i) {
                if (i < defs.count() && 1 == defs.at(i)) { ++n_present; }
              }
            }
            TmpVals tv;
            if (0 == dph_enc) { // PLAIN
              if (OB_FAIL(decode_plain(vals, vals_len, chunk.physical_, n_present, tv))) {
                LOG_WARN("failed to decode plain values", K(ret), K(n_present));
              }
            } else if ((2 == dph_enc || 8 == dph_enc) && have_dict) {
              int32_t bw = (vals_len >= 1) ? static_cast<int32_t>(static_cast<uint8_t>(vals[0])) : 0;
              if (bw <= 0) {
                ret = OB_INVALID_DATA;
              } else {
                ObSEArray<int64_t, 64> idxs;
                if (OB_FAIL(hybrid_decode(vals + 1, vals_len - 1, bw, n_present, idxs))) {
                  LOG_WARN("failed to decode dict indexes", K(ret), K(n_present), K(bw));
                } else {
                  for (int64_t k = 0; OB_SUCC(ret) && k < idxs.count(); ++k) {
                    int64_t d = idxs.at(k);
                    switch (chunk.physical_) {
                      case 0:
                        if (d < dict.bools_.count()) { ret = tv.bools_.push_back(dict.bools_.at(d)); }
                        else { ret = OB_INVALID_DATA; }
                        break;
                      case 1:
                      case 2:
                        if (d < dict.ints_.count()) { ret = tv.ints_.push_back(dict.ints_.at(d)); }
                        else { ret = OB_INVALID_DATA; }
                        break;
                      case 4:
                      case 5:
                        if (d < dict.dbls_.count()) { ret = tv.dbls_.push_back(dict.dbls_.at(d)); }
                        else { ret = OB_INVALID_DATA; }
                        break;
                      case 6:
                        if (d < dict.strs_.count()) { ret = tv.strs_.push_back(dict.strs_.at(d)); }
                        else { ret = OB_INVALID_DATA; }
                        break;
                      default:
                        ret = OB_NOT_SUPPORTED;
                        break;
                    }
                  }
                }
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported data page encoding", K(ret), K(dph_enc), K(have_dict));
            }
            if (OB_FAIL(ret)) {
            } else if (tv.ints_.count() + tv.dbls_.count() + tv.bools_.count() + tv.strs_.count()
                       < n_present) {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("page values fewer than present rows", K(ret), K(n_present));
            } else {
              // interleave so col arrays and present_ stay row-aligned
              int64_t vi = 0;
              for (int64_t r = 0; OB_SUCC(ret) && r < dph_rows; ++r) {
                bool present = true;
                if (max_def > 0) {
                  present = (r < defs.count() && 1 == defs.at(r));
                }
                if (OB_FAIL(col.present_.push_back(present))) {
                } else if (!present) {
                  switch (chunk.physical_) {
                    case 0: ret = col.bools_.push_back(false); break;
                    case 1: case 2: ret = col.ints_.push_back(0); break;
                    case 4: case 5: ret = col.dbls_.push_back(0.0); break;
                    case 6: ret = col.strs_.push_back(ObString()); break;
                    default: ret = OB_NOT_SUPPORTED; break;
                  }
                } else {
                  switch (chunk.physical_) {
                    case 0: ret = col.bools_.push_back(tv.bools_.at(vi)); break;
                    case 1: case 2: ret = col.ints_.push_back(tv.ints_.at(vi)); break;
                    case 4: case 5: ret = col.dbls_.push_back(tv.dbls_.at(vi)); break;
                    case 6: ret = col.strs_.push_back(tv.strs_.at(vi)); break;
                    default: ret = OB_NOT_SUPPORTED; break;
                  }
                  if (OB_SUCC(ret)) { ++vi; }
                }
              }
              if (OB_SUCC(ret)) {
                rows_done += dph_rows;
              }
            }
          }
        }
      } else if (3 == page_type) { // DATA_PAGE_V2
        const int64_t nvals = v2_num_values;
        const int64_t max_def = (1 == chunk.repetition_) ? 1 : 0;
        ObSEArray<int64_t, 64> defs;
        int64_t n_present = nvals;
        if (nvals <= 0 || 0 != v2_rep_len || chunk.repetition_ > 1 || v2_num_nulls < 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("bad v2 data page", K(ret), K(nvals), K(v2_num_nulls));
        } else if (max_def > 0
                   && OB_FAIL(hybrid_decode(body, v2_def_len, 1, nvals, defs))) {
          LOG_WARN("failed to decode v2 def levels", K(ret));
        } else {
          if (max_def > 0) {
            n_present = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < nvals; ++i) {
              if (i < defs.count() && 1 == defs.at(i)) { ++n_present; }
            }
          }
          TmpVals tv;
          if (0 != v2_enc) { // only PLAIN values on v2 pages here
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported v2 page encoding", K(ret), K(v2_enc));
          } else if (OB_FAIL(decode_plain(dec, dec_len, chunk.physical_, n_present, tv))) {
            LOG_WARN("failed to decode v2 plain values", K(ret), K(n_present));
          } else if (tv.ints_.count() + tv.dbls_.count() + tv.bools_.count() + tv.strs_.count()
                     < n_present) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("v2 page values fewer than present rows", K(ret), K(n_present));
          } else {
            int64_t vi = 0;
            for (int64_t r = 0; OB_SUCC(ret) && r < nvals; ++r) {
              bool present = true;
              if (max_def > 0) {
                present = (r < defs.count() && 1 == defs.at(r));
              }
              if (OB_FAIL(col.present_.push_back(present))) {
              } else if (!present) {
                switch (chunk.physical_) {
                  case 0: ret = col.bools_.push_back(false); break;
                  case 1: case 2: ret = col.ints_.push_back(0); break;
                  case 4: case 5: ret = col.dbls_.push_back(0.0); break;
                  case 6: ret = col.strs_.push_back(ObString()); break;
                  default: ret = OB_NOT_SUPPORTED; break;
                }
              } else {
                switch (chunk.physical_) {
                  case 0: ret = col.bools_.push_back(tv.bools_.at(vi)); break;
                  case 1: case 2: ret = col.ints_.push_back(tv.ints_.at(vi)); break;
                  case 4: case 5: ret = col.dbls_.push_back(tv.dbls_.at(vi)); break;
                  case 6: ret = col.strs_.push_back(tv.strs_.at(vi)); break;
                  default: ret = OB_NOT_SUPPORTED; break;
                }
                if (OB_SUCC(ret)) { ++vi; }
              }
            }
            if (OB_SUCC(ret)) {
              rows_done += nvals;
            }
          }
        }
      } else { // INDEX_PAGE / others
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet page type", K(ret), K(page_type));
      }
      off = body_off + comp_size;
    }
  }
  if (OB_SUCC(ret) && rows_done != chunk.num_values_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("parquet chunk rows mismatch", K(ret), K(rows_done), K(chunk.num_values_), K(chunk));
  }
  return ret;
}

int ObParquetReader::Impl::advance()
{
  int ret = OB_SUCCESS;
  int64_t next = cur_group_ + 1;
  if (next >= groups_.count()) {
    ret = OB_ITER_END;
  } else {
    arena_.reuse();
    for (int64_t i = 0; i < decoded_.count(); ++i) {
      decoded_.at(i).reset();
    }
    cur_group_ = next;
    const RowGroupLoc &rg = groups_.at(next);
    if (rg.num_rows_ <= 0) {
      ret = OB_INVALID_DATA;
    }
    for (int64_t c = 0; OB_SUCC(ret) && c < rg.chunks_.count(); ++c) {
      const ChunkLoc &cc = rg.chunks_.at(c);
      char *raw = nullptr;
      int64_t span = rg.num_rows_; // chunk range is independent; use stored comp size
      UNUSED(span);
      if (OB_FAIL(read_range(cc.start_off_, cc.comp_size_, raw))) {
        LOG_WARN("failed to read chunk bytes", K(ret), K(cc));
      } else if (OB_FAIL(decode_chunk(cc, raw, cc.comp_size_))) {
        LOG_WARN("failed to decode chunk", K(ret), K(cc));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < decoded_.count(); ++i) {
        if (decoded_.at(i).present_.count() != rg.num_rows_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("decoded column not aligned with row group", K(ret), K(i),
                   K(decoded_.at(i).present_.count()), K(rg.num_rows_));
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObParquetReader public wrappers
// ---------------------------------------------------------------------------

ObParquetReader::ObParquetReader() : impl_(nullptr) {}

ObParquetReader::~ObParquetReader()
{
  close();
}

int ObParquetReader::open(const char *path)
{
  int ret = OB_SUCCESS;
  if (nullptr != impl_) {
    ret = OB_INIT_TWICE;
  } else {
    void *buf = ob_malloc(sizeof(Impl), ObMemAttr("ParquetImpl"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      Impl *impl = new (buf) Impl();
      if (OB_FAIL(impl->open_raw(path))) {
        LOG_WARN("failed to open parquet", K(ret), K(path));
        impl->~Impl();
        ob_free(impl);
      } else if (OB_FAIL(impl->decoded_.prepare_allocate(impl->leaves_.count()))) {
        LOG_WARN("failed to prepare decoded columns", K(ret));
        impl->~Impl();
        ob_free(impl);
      } else {
        impl_ = impl;
      }
    }
  }
  return ret;
}

void ObParquetReader::close()
{
  if (nullptr != impl_) {
    Impl *impl = impl_;
    impl_ = nullptr;
    impl->~Impl();
    ob_free(impl);
  }
}

int64_t ObParquetReader::get_leaf_count() const
{
  return nullptr == impl_ ? 0 : impl_->leaves_.count();
}

int ObParquetReader::get_leaf_name(int64_t idx, ObString &name) const
{
  int ret = OB_SUCCESS;
  if (nullptr == impl_ || idx < 0 || idx >= impl_->leaves_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    name = impl_->leaves_.at(idx).name_;
  }
  return ret;
}

int ObParquetReader::get_leaf_physical_type(int64_t idx, int32_t &type) const
{
  int ret = OB_SUCCESS;
  if (nullptr == impl_ || idx < 0 || idx >= impl_->leaves_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    type = impl_->leaves_.at(idx).physical_;
  }
  return ret;
}

int ObParquetReader::get_leaf_converted_type(int64_t idx, int32_t &ct) const
{
  int ret = OB_SUCCESS;
  if (nullptr == impl_ || idx < 0 || idx >= impl_->leaves_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ct = impl_->leaves_.at(idx).converted_;
  }
  return ret;
}

int ObParquetReader::get_leaf_repetition(int64_t idx, int32_t &rep) const
{
  int ret = OB_SUCCESS;
  if (nullptr == impl_ || idx < 0 || idx >= impl_->leaves_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    rep = impl_->leaves_.at(idx).repetition_;
  }
  return ret;
}

int64_t ObParquetReader::get_num_rows() const
{
  return nullptr == impl_ ? 0 : impl_->num_rows_;
}

bool ObParquetReader::cursor_valid() const
{
  return nullptr != impl_ && impl_->cur_group_ >= 0
      && impl_->cur_group_ < impl_->groups_.count();
}

int64_t ObParquetReader::get_group_num_rows() const
{
  return nullptr == impl_ ? 0 : impl_->group_rows();
}

int ObParquetReader::advance_cursor()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(impl_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(impl_->advance())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to advance parquet cursor", K(ret));
    }
  }
  return ret;
}

int ObParquetReader::get_group_int64(int64_t leaf_idx, int64_t row_idx, int64_t &v,
                                     bool &present) const
{
  int ret = OB_SUCCESS;
  present = false;
  v = 0;
  if (OB_ISNULL(impl_) || !cursor_valid()
      || leaf_idx < 0 || leaf_idx >= impl_->leaves_.count()
      || row_idx < 0 || row_idx >= impl_->group_rows()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const Impl::DecodedCol &col = impl_->decoded_.at(leaf_idx);
    const int32_t phys = impl_->leaves_.at(leaf_idx).physical_;
    present = col.present_.at(row_idx);
    if (present) {
      if ((1 == phys || 2 == phys) && row_idx < col.ints_.count()) {
        v = col.ints_.at(row_idx);
      } else if (0 == phys && row_idx < col.bools_.count()) {
        v = col.bools_.at(row_idx) ? 1 : 0;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get_group_int64 used on non-int leaf", K(ret), K(phys));
      }
    }
  }
  return ret;
}

int ObParquetReader::get_group_double(int64_t leaf_idx, int64_t row_idx, double &v,
                                      bool &present) const
{
  int ret = OB_SUCCESS;
  present = false;
  v = 0;
  if (OB_ISNULL(impl_) || !cursor_valid()
      || leaf_idx < 0 || leaf_idx >= impl_->leaves_.count()
      || row_idx < 0 || row_idx >= impl_->group_rows()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const Impl::DecodedCol &col = impl_->decoded_.at(leaf_idx);
    const int32_t phys = impl_->leaves_.at(leaf_idx).physical_;
    present = col.present_.at(row_idx);
    if (present) {
      if ((4 == phys || 5 == phys) && row_idx < col.dbls_.count()) {
        v = col.dbls_.at(row_idx);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get_group_double used on non-float leaf", K(ret), K(phys));
      }
    }
  }
  return ret;
}

int ObParquetReader::get_group_bool(int64_t leaf_idx, int64_t row_idx, bool &v,
                                    bool &present) const
{
  int ret = OB_SUCCESS;
  present = false;
  v = false;
  if (OB_ISNULL(impl_) || !cursor_valid()
      || leaf_idx < 0 || leaf_idx >= impl_->leaves_.count()
      || row_idx < 0 || row_idx >= impl_->group_rows()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const Impl::DecodedCol &col = impl_->decoded_.at(leaf_idx);
    const int32_t phys = impl_->leaves_.at(leaf_idx).physical_;
    present = col.present_.at(row_idx);
    if (present) {
      if (0 == phys && row_idx < col.bools_.count()) {
        v = col.bools_.at(row_idx);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get_group_bool used on non-bool leaf", K(ret), K(phys));
      }
    }
  }
  return ret;
}

int ObParquetReader::get_group_string(int64_t leaf_idx, int64_t row_idx, ObString &v,
                                      bool &present) const
{
  int ret = OB_SUCCESS;
  present = false;
  v.reset();
  if (OB_ISNULL(impl_) || !cursor_valid()
      || leaf_idx < 0 || leaf_idx >= impl_->leaves_.count()
      || row_idx < 0 || row_idx >= impl_->group_rows()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const Impl::DecodedCol &col = impl_->decoded_.at(leaf_idx);
    const int32_t phys = impl_->leaves_.at(leaf_idx).physical_;
    present = col.present_.at(row_idx);
    if (present) {
      if (6 == phys && row_idx < col.strs_.count()) {
        v = col.strs_.at(row_idx);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get_group_string used on non-binary leaf", K(ret), K(phys));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
