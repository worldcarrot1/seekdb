// helpers for AI COLUMN (competition 800493). Header-only to avoid CMake churn.
#ifndef OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
#define OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
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
// wire format: __AICOL1__|e=<expr>|s=<sync_mode>|r=<max_retries>|t=<result_type>|d=<dim>
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
  common::ObString result_type_;  // "json"/"vector"/"bigint"/...
  int64_t max_retries_;
  int64_t result_dim_;
};
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_AI_COLUMN_COMMON_H_
