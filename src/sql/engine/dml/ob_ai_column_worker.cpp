// Async worker driving AI COLUMN tasks (competition 800493).
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ob_ai_column_worker.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_server_runtime.h"
#include "share/ob_ai_column_common.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_mod_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
using common::sqlclient::ObMySQLResult;
namespace
{
const int64_t WORKER_HTTP_TIMEOUT_SEC = 30;
const int64_t WORKER_CLAIM_LEASE_SEC = 300;
const int64_t WORKER_RETRY_BACKOFF_US = 800 * 1000L;

// deep copy into the cycle arena; structs below only keep ObString views so
// they stay assignable inside ObSEArray
int arena_dup(ObIAllocator &alloc, const ObString &in, ObString &out)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (in.length() <= 0) {
    out.reset();
  } else if (NULL == (buf = static_cast<char *>(alloc.alloc(in.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, in.ptr(), in.length());
    buf[in.length()] = '\0'; // sql text views are printed with %s
    out.assign_ptr(buf, in.length());
  }
  return ret;
}

// one queued __all_ai_column_task row
struct AiTaskRow
{
  AiTaskRow() : table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID),
      request_id_(0), state_(0), retry_count_(0) {}
  TO_STRING_KV(K_(table_id), K_(column_id), K_(request_id), K_(pred), K_(state), K_(retry_count));
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t request_id_;
  ObString pred_;         // rowkey predicate text, e.g. `id` = 1
  int64_t state_;
  int64_t retry_count_;
};

// one __all_ai_column meta row
struct AiColMeta
{
  AiColMeta() : column_id_(OB_INVALID_ID), max_retries_(0) {}
  TO_STRING_KV(K_(column_id), K_(column_name), K_(result_type), K_(max_retries));
  uint64_t column_id_;
  ObString column_name_;      // result column name
  ObString normalized_expr_;  // full AI_COMPLETE(...) text
  ObString result_type_;      // json / text / int / vector
  int64_t max_retries_;
};

// inner SQL has no real_escape here; doubling single quotes is the rule the
// DML helper uses as well (shared by predicates and result payloads)
int escape_into(ObSqlString &out, const ObString &raw)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length(); ++i) {
    if ('\'' == raw[i] && OB_FAIL(out.append("'"))) {
      LOG_WARN("failed to append escape quote", K(ret));
    } else if (OB_FAIL(out.append(raw.ptr() + i, 1))) {
      LOG_WARN("failed to append escaped char", K(ret));
    }
  }
  return ret;
}

// drop the outer single quotes of a model name literal, un-doubling '' pairs
int unquote_literal(ObIAllocator &alloc, const ObString &in, ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  const int64_t n = in.length();
  if (n >= 2 && '\'' == in.ptr()[0] && '\'' == in.ptr()[n - 1]) {
    char *buf = NULL;
    int64_t pos = 0;
    if (NULL == (buf = static_cast<char *>(alloc.alloc(n - 2 + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < n - 1; ++i) {
        if ('\'' == in.ptr()[i]) {
          if (i + 1 < n - 1 && '\'' == in.ptr()[i + 1]) {
            ++i;
          } else {
            break; // malformed literal, keep what was read so far
          }
        }
        buf[pos++] = in.ptr()[i];
      }
      if (OB_SUCC(ret)) {
        buf[pos] = '\0';
        out.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }
  } else if (OB_FAIL(arena_dup(alloc, in, out))) {
    LOG_WARN("failed to copy literal", K(ret));
  }
  return ret;
}

const char *get_system_prompt(const ObString &result_type)
{
  const char *prompt = "You are a helpful assistant.";
  if (0 == result_type.case_compare("json")) {
    prompt = "Return exactly one valid JSON value";
  } else if (0 == result_type.case_compare("int")) {
    prompt = "Return exactly one base-10 integer value";
  }
  return prompt;
}

int fetch_pending_tasks(ObIAllocator &alloc, ObIArray<AiTaskRow> &rows)
{
  int ret = OB_SUCCESS;
  const char *sql = "select table_id, column_id, request_id, rowkey_predicate, state, retry_count "
      "from oceanbase.__all_ai_column_task where state = 0 "
      "or (state in (1, 3) and eligible_ts <= now(6)) "
      "order by request_id, table_id, column_id limit 64";
  ObMySQLProxy::MySQLResult res;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql))) {
    LOG_WARN("failed to read ai column tasks", K(ret));
  } else {
    ObMySQLResult *result = res.get_result();
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
          LOG_WARN("failed to move task cursor", K(ret));
        } else {
          AiTaskRow row;
          ObString pred;
          int64_t table_id = 0;
          int64_t column_id = 0;
          if (OB_FAIL(result->get_int("table_id", table_id))) {
            LOG_WARN("failed to get table_id", K(ret));
          } else if (OB_FAIL(result->get_int("column_id", column_id))) {
            LOG_WARN("failed to get column_id", K(ret));
          } else if (OB_FAIL(result->get_int("request_id", row.request_id_))) {
            LOG_WARN("failed to get request_id", K(ret));
          } else if (OB_FAIL(result->get_varchar("rowkey_predicate", pred))) {
            LOG_WARN("failed to get rowkey_predicate", K(ret));
          } else if (OB_FAIL(result->get_int("state", row.state_))) {
            LOG_WARN("failed to get state", K(ret));
          } else if (OB_FAIL(result->get_int("retry_count", row.retry_count_))) {
            LOG_WARN("failed to get retry_count", K(ret));
          } else if (OB_FAIL(arena_dup(alloc, pred, row.pred_))) {
            LOG_WARN("failed to copy predicate", K(ret));
          } else {
            row.table_id_ = static_cast<uint64_t>(table_id);
            row.column_id_ = static_cast<uint64_t>(column_id);
            if (OB_FAIL(rows.push_back(row))) {
              LOG_WARN("failed to push task row", K(ret));
            }
          }
        }
      }
    }
    res.close();
  }
  return ret;
}

int exec_task_sql(const char *sql, int64_t &affected)
{
  int ret = OB_SUCCESS;
  affected = 0;
  if (OB_FAIL(GCTX.sql_proxy_->write(sql, affected))) {
    LOG_WARN("failed to exec task sql", K(ret));
  }
  return ret;
}

// returns OB_SUCCESS and claimed=true only when this worker owns the row
int claim_task(const AiTaskRow &row, bool &claimed)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  claimed = false;
  if (OB_FAIL(sql.append_fmt("update oceanbase.__all_ai_column_task set state = 1, "
      "eligible_ts = now(6) + interval %ld second "
      "where table_id = %lu and column_id = %lu and request_id = %ld "
      "and (state <> 1 or eligible_ts <= now(6))",
      WORKER_CLAIM_LEASE_SEC, row.table_id_, row.column_id_, row.request_id_))) {
    LOG_WARN("failed to build claim sql", K(ret), K(row));
  } else if (OB_FAIL(exec_task_sql(sql.ptr(), affected))) {
    LOG_WARN("failed to claim task", K(ret), K(row));
  } else {
    claimed = (affected > 0);
  }
  return ret;
}

int defer_task(const AiTaskRow &row, const int64_t retry_count, const int64_t backoff_us)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.append_fmt("update oceanbase.__all_ai_column_task set state = 3, "
      "retry_count = %ld, eligible_ts = now(6) + interval %ld microsecond "
      "where table_id = %lu and column_id = %lu and request_id = %ld",
      retry_count, backoff_us, row.table_id_, row.column_id_, row.request_id_))) {
    LOG_WARN("failed to build defer sql", K(ret), K(row));
  } else if (OB_FAIL(exec_task_sql(sql.ptr(), affected))) {
    LOG_WARN("failed to defer task", K(ret), K(row));
  }
  return ret;
}

int delete_task(const AiTaskRow &row)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.append_fmt("delete from oceanbase.__all_ai_column_task "
      "where table_id = %lu and column_id = %lu and request_id = %ld",
      row.table_id_, row.column_id_, row.request_id_))) {
    LOG_WARN("failed to build delete sql", K(ret), K(row));
  } else if (OB_FAIL(exec_task_sql(sql.ptr(), affected))) {
    LOG_WARN("failed to delete task", K(ret), K(row));
  }
  return ret;
}

int load_col_metas(ObIAllocator &alloc, const uint64_t table_id, ObIArray<AiColMeta> &metas)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt("select column_id, column_name, normalized_expr, max_retries, "
      "result_type from oceanbase.__all_ai_column where table_id = %lu", table_id))) {
    LOG_WARN("failed to build meta sql", K(ret), K(table_id));
  } else {
    ObMySQLProxy::MySQLResult res;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("failed to read ai column meta", K(ret), K(table_id));
    } else {
      ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
            LOG_WARN("failed to move meta cursor", K(ret));
          } else {
            AiColMeta meta;
            int64_t column_id = 0;
            ObString name, expr, type;
            if (OB_FAIL(result->get_int("column_id", column_id))) {
              LOG_WARN("failed to get column_id", K(ret));
            } else if (OB_FAIL(result->get_varchar("column_name", name))) {
              LOG_WARN("failed to get column_name", K(ret));
            } else if (OB_FAIL(result->get_varchar("normalized_expr", expr))) {
              LOG_WARN("failed to get normalized_expr", K(ret));
            } else if (OB_FAIL(result->get_int("max_retries", meta.max_retries_))) {
              LOG_WARN("failed to get max_retries", K(ret));
            } else if (OB_FAIL(result->get_varchar("result_type", type))) {
              LOG_WARN("failed to get result_type", K(ret));
            } else if (OB_FAIL(arena_dup(alloc, name, meta.column_name_))) {
              LOG_WARN("failed to copy column name", K(ret));
            } else if (OB_FAIL(arena_dup(alloc, expr, meta.normalized_expr_))) {
              LOG_WARN("failed to copy expression", K(ret));
            } else if (OB_FAIL(arena_dup(alloc, type, meta.result_type_))) {
              LOG_WARN("failed to copy result type", K(ret));
            } else {
              meta.column_id_ = static_cast<uint64_t>(column_id);
              if (OB_FAIL(metas.push_back(meta))) {
                LOG_WARN("failed to push meta", K(ret));
              }
            }
          }
        }
      }
      res.close();
    }
  }
  return ret;
}

int resolve_table_names(ObIAllocator &alloc, const uint64_t table_id,
                        ObString &db_name, ObString &tbl_name)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  const share::schema::ObDatabaseSchema *db_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
    LOG_WARN("failed to get runtime schema guard", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table for ai task vanished", K(ret), K(table_id));
  } else if (OB_FAIL(guard.get_database_schema(table_schema->get_database_id(), db_schema))) {
    LOG_WARN("failed to get database schema", K(ret));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(arena_dup(alloc, db_schema->get_database_name_str(), db_name))) {
    LOG_WARN("failed to copy db name", K(ret));
  } else if (OB_FAIL(arena_dup(alloc, table_schema->get_table_name_str(), tbl_name))) {
    LOG_WARN("failed to copy table name", K(ret));
  }
  return ret;
}

int fetch_user_text(ObIAllocator &alloc, const ObString &db_name, const ObString &tbl_name,
                    const ObString &user_expr, const ObString &pred,
                    ObString &user_text, bool &row_gone)
{
  int ret = OB_SUCCESS;
  row_gone = false;
  user_text.reset();
  ObSqlString sql;
  char *buf = NULL;
  const int64_t len = user_expr.length();
  if (len <= 0 || len > 4096) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bad ai expression length", K(ret), K(len));
  } else if (NULL == (buf = static_cast<char *>(alloc.alloc(len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, user_expr.ptr(), len);
    buf[len] = '\0';
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt("select cast(%s as char) as ai_v from `%.*s`.`%.*s` where %.*s",
             buf, db_name.length(), db_name.ptr(), tbl_name.length(), tbl_name.ptr(),
             pred.length(), pred.ptr()))) {
    LOG_WARN("failed to build user text sql", K(ret));
  } else {
    ObMySQLProxy::MySQLResult res;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("failed to eval ai user expression", K(ret));
    } else {
      ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          row_gone = true;
        } else {
          LOG_WARN("failed to move user text cursor", K(ret));
        }
      } else {
        ObString value;
        if (OB_FAIL(result->get_varchar("ai_v", value))) {
          if (OB_ERR_NULL_VALUE == ret || OB_OBJ_TYPE_ERROR == ret) {
            ret = OB_SUCCESS; // NULL dependency renders as empty user text
          } else {
            LOG_WARN("failed to read ai_v", K(ret));
          }
        } else if (OB_FAIL(arena_dup(alloc, value, user_text))) {
          LOG_WARN("failed to copy user text", K(ret));
        }
      }
      res.close();
    }
  }
  return ret;
}

int call_ai(ObIAllocator &alloc, const ObString &model_name, const ObString &result_type,
            const ObString &user_text, ObString &result_value)
{
  int ret = OB_SUCCESS;
  result_value.reset();
  query::ObIAiEndpointResolver *resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  ObAiModelEndpointInfo endpoint;
  if (OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai endpoint resolver unavailable", K(ret));
  } else if (OB_FAIL(resolver->resolve_by_model_name(model_name, alloc, endpoint))) {
    LOG_WARN("failed to resolve ai model endpoint", K(ret), K(model_name));
  } else if (0 == result_type.case_compare("vector")) {
    // embeddings ride the native dense embedding client; the result text is a
    // JSON array like [0,0,0,0], which the vector column accepts as a literal
    ObAIFuncExprInfo *info = NULL;
    if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(alloc, model_name, info))) {
      LOG_WARN("failed to get ai func info", K(ret), K(model_name));
    } else if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ai func info", K(ret), K(model_name));
    } else {
      ObAIFuncModel model(alloc, *info, endpoint);
      ObString content_copy = user_text;
      ObString embed_str;
      if (OB_FAIL(model.call_dense_embedding(content_copy, NULL, embed_str))) {
        LOG_WARN("failed to call dense embedding", K(ret), K(model_name));
      } else if (OB_FAIL(arena_dup(alloc, embed_str, result_value))) {
        LOG_WARN("failed to copy embedding result", K(ret));
      }
    }
  } else {
    ObString access_key;
    ObArray<ObString> headers;
    ObJsonObject *body = NULL;
    ObJsonObject *response = NULL;
    ObIJsonBase *base = NULL;
    ObAIFuncIComplete *provider = NULL;
    ObString out_str;
    ObAIFuncClient client;
    ObString request_model = endpoint.get_request_model_name();
    ObString sys_prompt(get_system_prompt(result_type));
    ObString user_copy = user_text;
    if (OB_FAIL(endpoint.get_unencrypted_access_key(alloc, access_key))) {
      LOG_WARN("failed to decrypt access key", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(alloc, endpoint.get_provider(), provider))) {
      LOG_WARN("failed to get ai provider", K(ret));
    } else if (OB_ISNULL(provider)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(provider->get_header(alloc, access_key, headers))) {
      LOG_WARN("failed to build ai headers", K(ret));
    } else if (OB_FAIL(provider->get_body(alloc, request_model, sys_prompt, user_copy, NULL, body))) {
      LOG_WARN("failed to build ai body", K(ret));
    } else {
      client.set_timeout_sec(WORKER_HTTP_TIMEOUT_SEC);
      if (OB_FAIL(client.send_post(alloc, endpoint.get_url(), headers, body, response))) {
        LOG_WARN("ai http call failed", K(ret));
      } else if (OB_FAIL(provider->parse_output(alloc, response, base))) {
        LOG_WARN("failed to parse ai response", K(ret));
      } else if (OB_ISNULL(base)) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(alloc, base, out_str))) {
        LOG_WARN("failed to print ai result", K(ret));
      } else if (OB_FAIL(arena_dup(alloc, out_str, result_value))) {
        LOG_WARN("failed to copy ai result", K(ret));
      }
    }
  }
  return ret;
}

int publish_group(ObIArray<AiTaskRow> &rows, const ObIArray<int64_t> &claimed_idx,
                  const ObIArray<ObString> &values,
                  const ObIArray<AiColMeta *> &metas,
                  const ObString &db_name, const ObString &tbl_name)
{
  int ret = OB_SUCCESS;
  const AiTaskRow &head = rows.at(claimed_idx.at(0));
  bool stale = false;
  // stale guard: a queued task with a larger request id for the same row
  // means a newer invalidation already superseded this group's values
  {
    ObSqlString sql;
    ObSqlString esc_pred;
    if (OB_FAIL(escape_into(esc_pred, head.pred_))) {
      LOG_WARN("failed to escape predicate", K(ret));
    } else if (OB_FAIL(sql.append_fmt("select count(*) as c from oceanbase.__all_ai_column_task "
        "where table_id = %lu and request_id > %ld and rowkey_predicate = '%s'",
        head.table_id_, head.request_id_, esc_pred.ptr()))) {
      LOG_WARN("failed to build stale sql", K(ret));
    } else {
      ObMySQLProxy::MySQLResult res;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("failed to check newer tasks", K(ret));
      } else {
        ObMySQLResult *result = res.get_result();
        int64_t cnt = 0;
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
        } else if (OB_FAIL(result->get_int("c", cnt))) {
          LOG_WARN("failed to read stale count", K(ret));
        } else if (cnt > 0) {
          stale = true;
        }
        res.close();
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (stale) {
    for (int64_t i = 0; i < claimed_idx.count(); ++i) {
      int tmp_ret = delete_task(rows.at(claimed_idx.at(i)));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to drop stale task", K(tmp_ret));
      }
    }
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      LOG_WARN("failed to start publish trans", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected = 0;
      bool first = true;
      if (OB_FAIL(sql.append_fmt("update `%.*s`.`%.*s` set ",
                 db_name.length(), db_name.ptr(), tbl_name.length(), tbl_name.ptr()))) {
        LOG_WARN("failed to build publish sql", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < claimed_idx.count(); ++i) {
        const AiColMeta *meta = metas.at(i);
        ObSqlString esc_value;
        if (NULL == meta) {
          // slot without a publishable value (orphan/dropped), skip
        } else if (OB_FAIL(escape_into(esc_value, values.at(i)))) {
          LOG_WARN("failed to escape result value", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s`%.*s` = '%s'", first ? "" : ", ",
                   meta->column_name_.length(), meta->column_name_.ptr(), esc_value.ptr()))) {
          LOG_WARN("failed to append publish set", K(ret));
        } else {
          first = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (first) {
        // nothing to publish; still clear the claimed tasks below
      } else if (OB_FAIL(sql.append_fmt(" where %.*s", head.pred_.length(), head.pred_.ptr()))) {
        LOG_WARN("failed to append publish predicate", K(ret));
      } else if (OB_FAIL(trans.write(sql.ptr(), affected))) {
        LOG_WARN("failed to publish ai results", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < claimed_idx.count(); ++i) {
        const AiTaskRow &row = rows.at(claimed_idx.at(i));
        ObSqlString del;
        int64_t aff2 = 0;
        if (OB_FAIL(del.append_fmt("delete from oceanbase.__all_ai_column_task "
            "where table_id = %lu and column_id = %lu and request_id = %ld",
            row.table_id_, row.column_id_, row.request_id_))) {
          LOG_WARN("failed to build task delete", K(ret));
        } else if (OB_FAIL(trans.write(del.ptr(), aff2))) {
          LOG_WARN("failed to delete published task", K(ret));
        }
      }
      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to end publish trans", K(tmp_ret), K(ret));
        if (OB_SUCC(ret)) { ret = tmp_ret; }
      }
    }
  }
  return ret;
}

void process_group(ObIAllocator &alloc, ObIArray<AiTaskRow> &rows,
                   const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = rows.at(begin).table_id_;
  ObSEArray<int64_t, 4> claimed_idx;
  ObSEArray<ObString, 4> values;
  ObSEArray<AiColMeta *, 4> metas;
  // claim
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    bool claimed = false;
    if (OB_FAIL(claim_task(rows.at(i), claimed))) {
      LOG_WARN("failed to claim task", K(ret), K(i));
    } else if (claimed && OB_FAIL(claimed_idx.push_back(i))) {
      LOG_WARN("failed to record claimed task", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // lease expiry will retry these rows later
  } else if (claimed_idx.empty()) {
    // nothing claimed; a rollback or delete raced us
  } else {
    ObSEArray<AiColMeta, 4> col_metas;
    ObString db_name, tbl_name;
    if (OB_FAIL(load_col_metas(alloc, table_id, col_metas))) {
      LOG_WARN("failed to load ai column metas", K(ret), K(table_id));
    } else if (OB_FAIL(resolve_table_names(alloc, table_id, db_name, tbl_name))) {
      LOG_WARN("failed to resolve table for ai task", K(ret), K(table_id));
    } else {
      bool aborted = false;
      bool row_gone = false;
      for (int64_t ci = 0; !aborted && !row_gone && ci < claimed_idx.count(); ++ci) {
        AiTaskRow &row = rows.at(claimed_idx.at(ci));
        AiColMeta *meta = NULL;
        for (int64_t m = 0; NULL == meta && m < col_metas.count(); ++m) {
          if (col_metas.at(m).column_id_ == row.column_id_) { meta = &col_metas.at(m); }
        }
        if (NULL == meta) {
          if (OB_FAIL(delete_task(row))) {
            LOG_WARN("failed to drop orphan task", K(ret), K(row));
          } else if (OB_FAIL(values.push_back(ObString()))
                     || OB_FAIL(metas.push_back(NULL))) {
            LOG_WARN("failed to extend arrays", K(ret));
          }
        } else {
          ObSEArray<ObString, 4> args;
          ObString model;
          if (OB_FAIL(share::ai_column_split_call_args(meta->normalized_expr_, args))) {
            LOG_WARN("failed to split ai call args", K(ret), KPC(meta));
          } else if (args.count() < 2) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("ai expression has no value argument", K(ret), K(args.count()));
          } else if (OB_FAIL(unquote_literal(alloc, args.at(0), model))) {
            LOG_WARN("failed to unquote model name", K(ret));
          } else {
            ObString user_text;
            ObString value;
            bool gone = false;
            int call_ret = fetch_user_text(alloc, db_name, tbl_name, args.at(1),
                                           row.pred_, user_text, gone);
            if (OB_SUCCESS == call_ret && gone) {
              row_gone = true;
              break;
            }
            if (OB_SUCCESS == call_ret) {
              call_ret = call_ai(alloc, model, meta->result_type_, user_text, value);
            }
            if (OB_SUCCESS != call_ret) {
              // transient or terminal failure for this column
              const int64_t next_retry = row.retry_count_ + 1;
              if (next_retry > meta->max_retries_) {
                if (OB_FAIL(delete_task(row))) {
                  LOG_WARN("failed to drop terminal task", K(ret), K(row));
                } else if (OB_FAIL(values.push_back(ObString()))
                           || OB_FAIL(metas.push_back(NULL))) {
                  LOG_WARN("failed to extend arrays", K(ret));
                }
              } else if (OB_FAIL(defer_task(row, next_retry, WORKER_RETRY_BACKOFF_US))) {
                LOG_WARN("failed to defer failed task", K(ret), K(row));
              } else {
                // the whole group waits: hold already finished columns too,
                // with their retry count untouched
                for (int64_t done = 0; !aborted && done < metas.count(); ++done) {
                  if (NULL != metas.at(done)) {
                    AiTaskRow &prev = rows.at(claimed_idx.at(done));
                    if (OB_FAIL(defer_task(prev, prev.retry_count_, WORKER_RETRY_BACKOFF_US))) {
                      LOG_WARN("failed to hold sibling task", K(ret), K(prev));
                    }
                  }
                }
                aborted = true;
              }
            } else if (OB_FAIL(values.push_back(value)) || OB_FAIL(metas.push_back(meta))) {
              LOG_WARN("failed to record ai result", K(ret));
            }
          }
        }
      }
      if (row_gone) {
        // the target row disappeared (pk moved or delete raced us): drop all
        for (int64_t ci2 = 0; ci2 < claimed_idx.count(); ++ci2) {
          int tmp_ret = delete_task(rows.at(claimed_idx.at(ci2)));
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("failed to drop task of missing row", K(tmp_ret));
          }
        }
      } else if (!aborted && OB_SUCC(ret)) {
        bool any_publish = false;
        for (int64_t m = 0; m < metas.count(); ++m) {
          if (NULL != metas.at(m)) { any_publish = true; }
        }
        if (any_publish && OB_FAIL(publish_group(rows, claimed_idx, values, metas,
                                                 db_name, tbl_name))) {
          LOG_WARN("failed to publish ai group", K(ret), K(table_id));
          // lease expiry / state machine retries; do not poison the loop
        }
      }
    }
  }
}

} // end of anonymous namespace

// ---------- ObAiColumnWorker ----------
ObAiColumnWorker &ObAiColumnWorker::get_instance()
{
  static ObAiColumnWorker worker;
  return worker;
}

ObAiColumnWorker::ObAiColumnWorker()
  : timer_(), scan_task_(), inited_(false), is_running_(false), working_(false)
{
}

ObAiColumnWorker::~ObAiColumnWorker()
{
}

int ObAiColumnWorker::init_and_start()
{
  int ret = OB_SUCCESS;
  // ObTimer::init starts the thread; schedule arms the repeating scan
  if (OB_FAIL(timer_.init("AiColWorker", lib::ObMemAttr("AiColWrk")))) {
    LOG_WARN("failed to init ai column timer", K(ret));
  } else if (OB_FAIL(timer_.schedule(scan_task_, SCAN_INTERVAL_US, true /*repeat*/, false))) {
    LOG_WARN("failed to schedule ai column scan", K(ret));
  } else {
    is_running_.store(true, std::memory_order_release);
    LOG_INFO("ai column worker started");
  }
  return ret;
}

int ObAiColumnWorker::ensure_started()
{
  int ret = OB_SUCCESS;
  if (!is_running_.load(std::memory_order_acquire)) {
    static ObSpinLock lock;
    ObSpinLockGuard guard(lock);
    if (!is_running_.load(std::memory_order_relaxed)) {
      if (OB_FAIL(init_and_start())) {
        LOG_WARN("failed to start ai column worker", K(ret));
      }
    }
  }
  return ret;
}

void ObAiColumnWorker::ObAiColumnScanTask::runTimerTask()
{
  ObAiColumnWorker::get_instance().do_work();
}

void ObAiColumnWorker::do_work()
{
  int ret = OB_SUCCESS;
  bool expected = false;
  if (!is_running_.load(std::memory_order_acquire) || OB_ISNULL(GCTX.sql_proxy_)) {
    // not up yet
  } else if (!working_.compare_exchange_strong(expected, true)) {
    // timer threads must not overlap scans: a concurrent drain of the same
    // pending set would re-process tasks and double-send HTTP requests
  } else {
    ObArenaAllocator alloc(lib::ObMemAttr("AiColTask"));
    ObSEArray<AiTaskRow, 16> rows;
    if (OB_FAIL(fetch_pending_tasks(alloc, rows))) {
      LOG_WARN("failed to fetch ai tasks", K(ret));
    }
    int64_t begin = 0;
    while (begin < rows.count()) {
      int64_t end = begin + 1;
      while (end < rows.count()
             && rows.at(end).table_id_ == rows.at(begin).table_id_
             && rows.at(end).request_id_ == rows.at(begin).request_id_) {
        ++end;
      }
      process_group(alloc, rows, begin, end);
      begin = end;
    }
    working_.store(false, std::memory_order_release);
  }
}

} // namespace sql
} // namespace oceanbase
