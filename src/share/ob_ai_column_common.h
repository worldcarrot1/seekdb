// Helpers for AI COLUMN (competition 800493). Header-only to avoid CMake churn.
#ifndef OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
#define OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "lib/utility/alloc_assist.h"
#include <cctype>
#include "share/schema/ob_column_schema.h"
namespace oceanbase
{
namespace share
{
// async task lifecycle state, persisted in each row's hidden <col>_state column.
enum ObAIColumnState : int64_t
{
  AI_COL_PENDING = 0,   // task created, not yet claimed
  AI_COL_RUNNING = 1,   // claimed by worker (lease in eligible_ts)
  AI_COL_DONE    = 2,   // result published
  AI_COL_RETRY   = 3,   // failed, awaiting retry/backoff
  AI_COL_FAILED  = 4,   // terminal failure, result stays NULL
};

// hidden physical column name suffixes for one AI result column named base.
const char *const AI_COLUMN_STATE_SUFFIX   = "_ai_state";
const char *const AI_COLUMN_REQUEST_SUFFIX = "_ai_request";
const char *const AI_COLUMN_SPEC_SUFFIX    = "_ai_spec";

// per-AI-column spec carried from resolver to RS via the hidden result_spec
// column's comment field (which is deep-copied + serialized on ObColumnSchemaV2).
// wire format: __AICOL1__|e=<sync>|... see encode/decode below.
struct ObAIColumnSpec
{
  ObAIColumnSpec()
    : expr_(), sync_mode_(), result_type_(), max_retries_(0), result_dim_(0) {}
  void reset()
  {
    expr_.reset(); sync_mode_.reset(); result_type_.reset();
    max_retries_ = 0; result_dim_ = 0;
  }
  bool is_valid() const { return !expr_.empty() && !result_type_.empty(); }
  TO_STRING_KV(K_(expr), K_(sync_mode), K_(max_retries), K_(result_type), K_(result_dim));
  common::ObString expr_;         // normalized expression text (JSON suffix folded in)
  common::ObString sync_mode_;    // "async"
  common::ObString result_type_;  // "json"/"vector"/"text"
  int64_t max_retries_;
  int64_t result_dim_;            // 0 when not an embedding
};

// ---------- hidden column vocabulary ----------
const int64_t AI_COLUMN_HIDDEN_COL_CNT = 3;
inline const char *get_ai_hidden_suffix(const int64_t idx)
{
  return 0 == idx ? AI_COLUMN_STATE_SUFFIX
       : 1 == idx ? AI_COLUMN_REQUEST_SUFFIX
       : AI_COLUMN_SPEC_SUFFIX;
}
inline int64_t get_ai_hidden_flag(const int64_t idx)
{
  return 0 == idx ? AI_COLUMN_STATE_FLAG
       : 1 == idx ? AI_COLUMN_REQUEST_FLAG
       : AI_COLUMN_SPEC_FLAG;
}

// ---------- spec wire format ----------
const char *const AI_COLUMN_SPEC_PREFIX = "__AICOL1__";
const char *const AI_COLUMN_JSON_PROMPT = " Return exactly one valid JSON value";

inline bool is_ai_column_spec_comment(const common::ObString &comment)
{
  const int64_t prefix_len = STRLEN(AI_COLUMN_SPEC_PREFIX);
  return comment.length() > prefix_len
         && 0 == MEMCMP(comment.ptr(), AI_COLUMN_SPEC_PREFIX, prefix_len);
}

// percent-escape the delimiters only: % | = CR LF (everything else stays raw)
inline int ai_column_encode_text(const common::ObString &in, common::ObSqlString &out)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < in.length(); ++i) {
    const char c = in.ptr()[i];
    const char *esc = NULL;
    switch (c) {
      case '%': esc = "%25"; break;
      case '|': esc = "%7C"; break;
      case '=': esc = "%3D"; break;
      case '\n': esc = "%0A"; break;
      case '\r': esc = "%0D"; break;
      default: break;
    }
    if (NULL != esc) {
      ret = out.append(esc);
    } else {
      ret = out.append(&c, 1);
    }
  }
  return ret;
}

inline int8_t ai_hex_val(const char c)
{
  int8_t v = -1;
  if (c >= '0' && c <= '9') { v = c - '0'; }
  else if (c >= 'a' && c <= 'f') { v = c - 'a' + 10; }
  else if (c >= 'A' && c <= 'F') { v = c - 'A' + 10; }
  return v;
}

// decode a percent-escaped region into buf (which must hold len + 1 bytes)
inline int ai_column_decode_text(const char *in, const int64_t len,
                                 char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ) {
    if ('%' == in[i] && i + 2 < len) {
      const int8_t hi = ai_hex_val(in[i + 1]);
      const int8_t lo = ai_hex_val(in[i + 2]);
      if (hi >= 0 && lo >= 0) {
        const char decoded = static_cast<char>((hi << 4) | lo);
        if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, "%c", decoded))) {
        } else { i += 3; }
        continue;
      }
    }
    if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, "%c", in[i]))) {
    } else { ++i; }
  }
  return ret;
}

inline int64_t ai_column_find(const char *buf, const int64_t len,
                              const int64_t start, const char *marker)
{
  const int64_t mlen = STRLEN(marker);
  int64_t found = -1;
  for (int64_t i = start; -1 == found && i + mlen <= len; ++i) {
    if (0 == MEMCMP(buf + i, marker, mlen)) { found = i; }
  }
  return found;
}

inline int64_t ai_strtoll(const char *p, const int64_t s, const int64_t e, bool &valid)
{
  int64_t v = 0;
  valid = e > s;
  for (int64_t i = s; valid && i < e; ++i) {
    if (p[i] >= '0' && p[i] <= '9') { v = v * 10 + (p[i] - '0'); }
    else { valid = false; }
  }
  return v;
}

inline int encode_ai_column_spec(const ObAIColumnSpec &spec, common::ObSqlString &out)
{
  int ret = common::OB_SUCCESS;
  common::ObSqlString enc_expr;
  if (!spec.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ai_column_encode_text(spec.expr_, enc_expr))) {
  } else if (OB_FAIL(out.assign_fmt("%s|e=%s|s=%.*s|r=%ld|t=%.*s|d=%ld",
                                    AI_COLUMN_SPEC_PREFIX,
                                    enc_expr.ptr(),
                                    static_cast<int>(spec.sync_mode_.length()),
                                    spec.sync_mode_.ptr(),
                                    spec.max_retries_,
                                    static_cast<int>(spec.result_type_.length()),
                                    spec.result_type_.ptr(),
                                    spec.result_dim_))) {
  }
  return ret;
}

// decode must be the exact inverse of encode_ai_column_spec
inline int decode_ai_column_spec(const common::ObString &comment,
                                 common::ObIAllocator &alloc,
                                 ObAIColumnSpec &spec)
{
  int ret = common::OB_SUCCESS;
  spec.reset();
  const char *p = comment.ptr();
  const int64_t n = comment.length();
  const int64_t prefix_len = STRLEN(AI_COLUMN_SPEC_PREFIX);
  const int64_t e_start = prefix_len + 3;  // after "|e="
  int64_t s_at = -1, r_at = -1, t_at = -1, d_at = -1;
  char *sbuf = NULL, *tbuf = NULL;
  int64_t spos = 0, tpos = 0;
  bool valid = false;
  if (!is_ai_column_spec_comment(comment) || n < e_start + 3
      || 0 != MEMCMP(p + prefix_len, "|e=", 3)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    s_at = ai_column_find(p, n, e_start, "|s=");
    if (s_at < 0) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret)) {
    r_at = ai_column_find(p, n, s_at + 3, "|r=");
    if (r_at < 0) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret)) {
    t_at = ai_column_find(p, n, r_at + 3, "|t=");
    if (t_at < 0) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret)) {
    d_at = ai_column_find(p, n, t_at + 3, "|d=");
    if (d_at < 0) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret)) {
    const int64_t e_len = s_at - e_start;
    char *ebuf = NULL;
    int64_t epos = 0;
    if (NULL == (ebuf = static_cast<char *>(alloc.alloc(e_len + 1)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ai_column_decode_text(p + e_start, e_len, ebuf, e_len + 1, epos))) {
    } else {
      ebuf[epos] = '\0';
      spec.expr_.assign_ptr(ebuf, static_cast<int32_t>(epos));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t s_len = r_at - (s_at + 3);
    if (NULL == (sbuf = static_cast<char *>(alloc.alloc(s_len + 1)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ai_column_decode_text(p + s_at + 3, s_len, sbuf, s_len + 1, spos))) {
    } else {
      sbuf[spos] = '\0';
      spec.sync_mode_.assign_ptr(sbuf, static_cast<int32_t>(spos));
    }
  }
  if (OB_SUCC(ret)) {
    spec.max_retries_ = ai_strtoll(p, r_at + 3, t_at, valid);
    if (!valid || spec.max_retries_ < 0) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret)) {
    const int64_t t_len = d_at - (t_at + 3);
    if (NULL == (tbuf = static_cast<char *>(alloc.alloc(t_len + 1)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ai_column_decode_text(p + t_at + 3, t_len, tbuf, t_len + 1, tpos))) {
    } else {
      tbuf[tpos] = '\0';
      spec.result_type_.assign_ptr(tbuf, static_cast<int32_t>(tpos));
    }
  }
  if (OB_SUCC(ret)) {
    spec.result_dim_ = ai_strtoll(p, d_at + 3, n, valid);
    if (!valid) { ret = common::OB_INVALID_ARGUMENT; }
  }
  if (OB_SUCC(ret) && !spec.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  return ret;
}

// ---------- identifier scan (quote aware) ----------
inline bool ai_ident_char_start(const char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || '_' == c;
}
inline bool ai_ident_char_cont(const char c)
{
  return ai_ident_char_start(c) || (c >= '0' && c <= '9') || '$' == c;
}

inline int ai_column_expr_refers_identifier(const common::ObString &expr,
                                            const common::ObString &ident,
                                            bool &found)
{
  int ret = common::OB_SUCCESS;
  found = false;
  const char *p = expr.ptr();
  const int64_t n = expr.length();
  int64_t i = 0;
  while (i < n && !found) {
    const char c = p[i];
    if ('\'' == c) {  // skip string literal, '' is an embedded quote
      ++i;
      while (i < n) {
        if ('\'' == p[i]) {
          if (i + 1 < n && '\'' == p[i + 1]) { i += 2; }
          else { ++i; break; }
        } else { ++i; }
      }
    } else if ('"' == c) {  // skip double-quoted (ANSI mode)
      ++i;
      while (i < n && '"' != p[i]) { ++i; }
      if (i < n) { ++i; }
    } else if ('`' == c) {  // backticked identifier: content must match
      const int64_t s = ++i;
      while (i < n && '`' != p[i]) { ++i; }
      const common::ObString word(static_cast<int32_t>(i - s), p + s);
      if (i < n) { ++i; }
      if (word.case_compare_equal(ident)) { found = true; }
    } else if (ai_ident_char_start(c)) {
      const int64_t s = i;
      while (i < n && ai_ident_char_cont(p[i])) { ++i; }
      const common::ObString word(static_cast<int32_t>(i - s), p + s);
      if (word.case_compare_equal(ident)) { found = true; }
    } else {
      ++i;
    }
  }
  return ret;
}

// ---------- call argument split ----------
inline common::ObString ai_trim(const common::ObString &s)
{
  const char *p = s.ptr();
  int64_t n = s.length();
  while (n > 0 && isspace(static_cast<unsigned char>(p[0]))) { ++p; --n; }
  while (n > 0 && isspace(static_cast<unsigned char>(p[n - 1]))) { --n; }
  return common::ObString(static_cast<int32_t>(n), const_cast<char *>(p));
}

// text is e.g. "AI_COMPLETE('m', concat('x: ', c))": split the top-level
// comma-separated arguments of the first call encountered.
inline int ai_column_split_call_args(const common::ObString &text,
                                     common::ObIArray<common::ObString> &args)
{
  int ret = common::OB_SUCCESS;
  args.reset();
  const char *p = text.ptr();
  const int64_t n = text.length();
  int64_t open = -1;
  for (int64_t i = 0; i < n && open < 0; ++i) {
    if ('(' == p[i]) { open = i; }
  }
  if (open < 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    int64_t depth = 1;
    int64_t arg_start = open + 1;
    int64_t i = open + 1;
    bool closed = false;
    while (OB_SUCC(ret) && !closed && i < n) {
      const char c = p[i];
      if ('\'' == c || '"' == c || '`' == c) {
        const int64_t q = ++i;
        bool term = false;
        while (i < n) {
          if (c == p[i]) {
            if ('\'' == c && i + 1 < n && '\'' == p[i + 1]) { i += 2; }
            else { term = true; break; }
          } else { ++i; }
        }
        if (!term) {
          ret = common::OB_INVALID_ARGUMENT;
        } else {
          ++i; continue; // step past the closing quote; the outer ++ must not swallow the next char
        }
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c) {
        --depth;
        if (0 == depth) {
          if (OB_FAIL(args.push_back(ai_trim(common::ObString(
                  static_cast<int32_t>(i - arg_start), const_cast<char *>(p + arg_start)))))) {
          } else { closed = true; }
        }
      } else if (',' == c && 1 == depth) {
        if (OB_FAIL(args.push_back(ai_trim(common::ObString(
                static_cast<int32_t>(i - arg_start), const_cast<char *>(p + arg_start)))))) {
        } else { arg_start = i + 1; }
      }
      if (OB_SUCC(ret) && !closed) { ++i; }
    }
    if (OB_FAIL(ret)) {
    } else if (!closed) {
      ret = common::OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

// ---------- hidden column schema builder ----------
inline int fill_ai_hidden_column_schema(const common::ObString &base_name,
                                        const int64_t idx,
                                        schema::ObColumnSchemaV2 &column)
{
  int ret = common::OB_SUCCESS;
  const int64_t flag = get_ai_hidden_flag(idx);
  char name_buf[OB_MAX_COLUMN_NAME_LENGTH + 1];
  int64_t pos = 0;
  MEMSET(name_buf, 0, sizeof(name_buf));
  if (idx < 0 || idx >= AI_COLUMN_HIDDEN_COL_CNT) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (base_name.length() + static_cast<int64_t>(STRLEN(AI_COLUMN_SPEC_SUFFIX))
             > OB_MAX_COLUMN_NAME_LENGTH) {
    ret = common::OB_NOT_SUPPORTED;
  } else if (OB_FAIL(common::databuff_printf(name_buf, sizeof(name_buf), pos, "%.*s%s",
                                             static_cast<int>(base_name.length()),
                                             base_name.ptr(), get_ai_hidden_suffix(idx)))) {
  } else if (OB_FAIL(column.set_column_name(name_buf))) {
  } else {
    column.set_is_hidden(true);
    column.add_column_flag(flag);
    column.set_rowkey_position(0);
    column.set_index_position(0);
    column.set_tbl_part_key_pos(0);
    column.set_prev_column_id(UINT64_MAX);
    column.set_next_column_id(UINT64_MAX);
    if (AI_COLUMN_SPEC_FLAG == flag) {
      column.set_data_type(ObVarcharType);
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_data_length(4096);
      column.set_nullable(true);
    } else {
      common::ObObj default_value;
      default_value.set_int(0);
      column.set_data_type(ObIntType);
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_data_length(0);
      column.set_nullable(false);
      if (OB_FAIL(column.set_orig_default_value(default_value))) {
      } else if (OB_FAIL(column.set_cur_default_value(default_value, false))) {
      }
    }
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
