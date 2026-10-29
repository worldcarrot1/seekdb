// DML integration for async AI COLUMN (competition 800493).
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ob_ai_column_dml_helper.h"
#include "sql/engine/dml/ob_ai_column_worker.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/das/ob_das_define.h"
#include "share/ob_ai_column_common.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "lib/time/ob_clock_generator.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
namespace
{
int64_t g_ai_request_seq = 0;
} // end of anonymous namespace

int64_t ObAiColumnDmlHelper::gen_request_id()
{
  // Milliseconds occupy the high bits so ids stay monotonic per server, the
  // low 20 bits break ties inside one tick. All AI columns of one row share
  // a single request id, which is what lets the worker batch them together.
  const int64_t seq = ATOMIC_AAF(&g_ai_request_seq, 1) & 0xFFFFF;
  return ((ObClockGenerator::getClock() / 1000) << 20) | seq;
}

int ObAiColumnDmlHelper::fetch_table_schema(
    ObDMLRtCtx &dml_rtctx,
    const ObDASTabletLoc *tablet_loc,
    ObSchemaGetterGuard *&schema_guard,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  schema_guard = NULL;
  table_schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_ISNULL(tablet_loc) || OB_ISNULL(tablet_loc->loc_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablet location", K(ret));
  } else if (OB_ISNULL(schema_guard = dml_rtctx.get_exec_ctx().get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema guard", K(ret));
  } else if (FALSE_IT(table_id = tablet_loc->loc_meta_->ref_table_id_)) {
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema not found", K(ret), K(table_id));
  }
  return ret;
}

int ObAiColumnDmlHelper::collect_async_ai_columns(
    const ObTableSchema &table_schema,
    ObIAllocator &alloc,
    ObIArray<ObAiColEntry> &entries)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 32> column_ids;
  const int64_t suffix_len = static_cast<int64_t>(STRLEN(AI_COLUMN_SPEC_SUFFIX));
  if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  }
  // The encoded spec lives in the hidden "<result>_ai_spec" column, while the
  // value that DML invalidates and the worker publishes is the result column.
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    const ObColumnSchemaV2 *spec_col = table_schema.get_column_schema(column_ids.at(i));
    if (OB_ISNULL(spec_col) || !spec_col->is_ai_spec_column()
        || !is_ai_column_spec_comment(spec_col->get_comment_str())) {
      // not an AI spec column
    } else {
      ObAIColumnSpec spec;
      const ObString &spec_name = spec_col->get_column_name_str();
      const ObColumnSchemaV2 *result_col = NULL;
      ObAiColEntry entry;
      if (OB_FAIL(decode_ai_column_spec(spec_col->get_comment_str(), alloc, spec))) {
        LOG_WARN("failed to decode ai column spec, ignore this column",
                 K(ret), K(spec_col->get_column_id()));
        ret = OB_SUCCESS; // a broken comment must not fail unrelated DML
      } else if (0 != spec.sync_mode_.case_compare("async")) {
        // only async columns are driven by the task queue
      } else if (spec_name.length() <= suffix_len) {
        // malformed spec column name
      } else if (FALSE_IT(result_col = table_schema.get_column_schema(
                 common::ObString(static_cast<int32_t>(spec_name.length() - suffix_len),
                                  spec_name.ptr())))) {
      } else if (OB_ISNULL(result_col) || !result_col->is_ai_result_column()) {
        // dangling spec column without its result column, nothing to refresh
      } else {
        entry.column_id_ = result_col->get_column_id();
        entry.name_ = result_col->get_column_name_str();
        entry.expr_ = spec.expr_;
        entry.result_type_ = spec.result_type_;
        if (OB_FAIL(entries.push_back(entry))) {
          LOG_WARN("failed to push back ai column entry", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAiColumnDmlHelper::build_rowkey_predicate(
    const ObTableSchema &table_schema,
    const ExprFixedArray &row,
    const ObIArray<uint64_t> &das_column_ids,
    ObEvalCtx &eval_ctx,
    const ObObjPrintParams &print_params,
    char *buf,
    const int64_t buf_len,
    ObString &pred)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> rowkey_ids;
  int64_t pos = 0;
  pred.reset();
  if (OB_ISNULL(buf) || buf_len <= 0 || row.count() != das_column_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row.count()), K(das_column_ids.count()));
  } else if (OB_FAIL(table_schema.get_rowkey_column_ids(rowkey_ids))) {
    LOG_WARN("failed to get rowkey column ids", K(ret));
  } else if (rowkey_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table without rowkey cannot describe a task target", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); ++i) {
    int64_t idx = -1;
    for (int64_t j = 0; idx < 0 && j < das_column_ids.count(); ++j) {
      if (das_column_ids.at(j) == rowkey_ids.at(i)) { idx = j; }
    }
    const ObColumnSchemaV2 *col = table_schema.get_column_schema(rowkey_ids.at(i));
    ObDatum *datum = NULL;
    ObObj obj;
    if (idx < 0 || idx >= row.count() || OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey column missing from dml row", K(ret), K(rowkey_ids.at(i)), K(idx));
    } else if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
      LOG_WARN("predicate buffer overflow", K(ret), K(pos));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%.*s` = ",
               col->get_column_name_str().length(), col->get_column_name_str().ptr()))) {
      LOG_WARN("predicate buffer overflow", K(ret), K(pos));
    } else if (OB_FAIL(row.at(idx)->eval(eval_ctx, datum))) {
      LOG_WARN("failed to eval rowkey expr", K(ret));
    } else if (FALSE_IT(obj.reset())) {
    } else if (OB_FAIL(datum->to_obj(obj, row.at(idx)->obj_meta_))) {
      LOG_WARN("failed to cast rowkey datum to obj", K(ret));
    } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos, print_params))) {
      LOG_WARN("failed to print rowkey value", K(ret), K(pos));
    }
  }
  if (OB_SUCC(ret)) {
    buf[pos] = '\0';
    pred.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

// databuff_printf has no ObHexEscapeSqlStr specialization; escape by doubling
// single quotes. print_sql_literal already emits '' for embedded quotes, so
// this is idempotent for quoted values and only matters for the identifier
// backticks around non-string rowkeys (which contain no quotes at all).
int ObAiColumnDmlHelper::append_escaped(ObSqlString &sql, const ObString &raw)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw.length(); ++i) {
    if ('\'' == raw[i] && OB_FAIL(sql.append("'"))) {
      LOG_WARN("failed to append escape quote", K(ret));
    } else if (OB_FAIL(sql.append(raw.ptr() + i, 1))) {
      LOG_WARN("failed to append escaped char", K(ret));
    }
  }
  return ret;
}

int ObAiColumnDmlHelper::gen_task_insert_sql(
    const uint64_t table_id,
    const ObAiColEntry &entry,
    const int64_t request_id,
    const ObString &rowkey_pred,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "insert into oceanbase.__all_ai_column_task (table_id, column_id, request_id, "
      "rowkey_predicate, state, retry_count, eligible_ts, result_type) values "
      "(%lu, %lu, %ld, '",
      table_id, entry.column_id_, request_id))) {
    LOG_WARN("failed to build task insert sql", K(ret));
  } else if (OB_FAIL(append_escaped(sql, rowkey_pred))) {
    LOG_WARN("failed to escape rowkey predicate", K(ret));
  } else if (OB_FAIL(sql.append_fmt("', 0, 0, now(6), '%.*s')",
      entry.result_type_.length(), entry.result_type_.ptr()))) {
    LOG_WARN("failed to build task insert sql tail", K(ret));
  } else if (sql.length() >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("task sql too long", K(ret), K(sql.length()), K(buf_len));
  } else {
    MEMCPY(buf, sql.ptr(), sql.length());
    buf[sql.length()] = '\0';
  }
  return ret;
}

int ObAiColumnDmlHelper::gen_clear_tasks_sql(
    const uint64_t table_id,
    const ObString &rowkey_pred,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "delete from oceanbase.__all_ai_column_task where table_id = %lu "
      "and rowkey_predicate = '", table_id))) {
    LOG_WARN("failed to build task clear sql", K(ret));
  } else if (OB_FAIL(append_escaped(sql, rowkey_pred))) {
    LOG_WARN("failed to escape rowkey predicate", K(ret));
  } else if (OB_FAIL(sql.append("'"))) {
    LOG_WARN("failed to close task clear sql", K(ret));
  } else if (sql.length() >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("task clear sql too long", K(ret), K(sql.length()), K(buf_len));
  } else {
    MEMCPY(buf, sql.ptr(), sql.length());
    buf[sql.length()] = '\0';
  }
  return ret;
}


int ObAiColumnDmlHelper::after_insert_row(
    const ObInsCtDef &ins_ctdef,
    const ObDASTabletLoc *tablet_loc,
    ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObTableModifyOp &dml_op = dml_rtctx.op_;
  if (!ins_ctdef.is_primary_index_ || dml_op.is_nested_session()) {
    // secondary index rows and inner sqls never enqueue AI tasks
  } else {
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    ObIAllocator &alloc = dml_rtctx.get_exec_ctx().get_allocator();
    ObSEArray<ObAiColEntry, 4> entries;
    char pred_buf[PRED_BUF_LEN];
    char sql_buf[SQL_BUF_LEN];
    ObString pred;
    bool session_started = false;
    if (OB_FAIL(fetch_table_schema(dml_rtctx, tablet_loc, schema_guard, table_schema))) {
      LOG_WARN("failed to fetch table schema", K(ret));
    } else if (OB_FAIL(collect_async_ai_columns(*table_schema, alloc, entries))) {
      LOG_WARN("failed to collect ai columns", K(ret));
    } else if (entries.empty()) {
      // fast path for tables without async AI columns
    } else if (OB_FAIL(build_rowkey_predicate(*table_schema, ins_ctdef.new_row_,
               ins_ctdef.das_ctdef_.column_ids_, dml_rtctx.get_eval_ctx(),
               dml_op.get_obj_print_params(), pred_buf, sizeof(pred_buf), pred))) {
      LOG_WARN("failed to build rowkey predicate", K(ret));
    } else if (OB_FAIL(dml_op.open_inner_conn())) {
      LOG_WARN("failed to open inner conn for ai tasks", K(ret));
    } else if (OB_FAIL(dml_op.begin_nested_session(true))) {
      LOG_WARN("failed to begin nested session", K(ret));
    } else {
      session_started = true;
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = true;
      const uint64_t table_id = tablet_loc->loc_meta_->ref_table_id_;
      const int64_t request_id = gen_request_id();
      for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
        if (OB_FAIL(gen_task_insert_sql(table_id, entries.at(i), request_id, pred,
                                        sql_buf, sizeof(sql_buf)))) {
          LOG_WARN("failed to gen task sql", K(ret), K(table_id),
                   K(entries.at(i).column_id_));
        } else if (OB_FAIL(dml_op.execute_write(sql_buf))) {
          LOG_WARN("failed to write ai task row", K(ret), K(table_id),
                   K(entries.at(i).column_id_));
        }
      }
    }
    if (session_started) {
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = false;
      int tmp_ret = dml_op.end_nested_session();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to end nested session", K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
    // the first DML that could enqueue a task arms the background worker
    if (OB_SUCC(ret) && !entries.empty()) {
      (void)ObAiColumnWorker::get_instance().ensure_started();
    }
  }
  return ret;
}

int ObAiColumnDmlHelper::after_delete_row(
    const ObDelCtDef &del_ctdef,
    const ObDASTabletLoc *tablet_loc,
    ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObTableModifyOp &dml_op = dml_rtctx.op_;
  if (!del_ctdef.is_primary_index_ || dml_op.is_nested_session()) {
    // secondary index rows and inner sqls never touch the AI task queue
  } else {
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    ObIAllocator &alloc = dml_rtctx.get_exec_ctx().get_allocator();
    ObSEArray<ObAiColEntry, 4> entries;
    char pred_buf[PRED_BUF_LEN];
    char sql_buf[SQL_BUF_LEN];
    ObString pred;
    bool session_started = false;
    if (OB_FAIL(fetch_table_schema(dml_rtctx, tablet_loc, schema_guard, table_schema))) {
      LOG_WARN("failed to fetch table schema", K(ret));
    } else if (OB_FAIL(collect_async_ai_columns(*table_schema, alloc, entries))) {
      LOG_WARN("failed to collect ai columns", K(ret));
    } else if (entries.empty()) {
      // fast path for tables without async AI columns
    } else if (OB_FAIL(build_rowkey_predicate(*table_schema, del_ctdef.old_row_,
               del_ctdef.das_ctdef_.column_ids_, dml_rtctx.get_eval_ctx(),
               dml_op.get_obj_print_params(), pred_buf, sizeof(pred_buf), pred))) {
      LOG_WARN("failed to build rowkey predicate", K(ret));
    } else if (OB_FAIL(dml_op.open_inner_conn())) {
      LOG_WARN("failed to open inner conn for ai tasks", K(ret));
    } else if (OB_FAIL(dml_op.begin_nested_session(true))) {
      LOG_WARN("failed to begin nested session", K(ret));
    } else {
      session_started = true;
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = true;
      const uint64_t table_id = tablet_loc->loc_meta_->ref_table_id_;
      if (OB_FAIL(gen_clear_tasks_sql(table_id, pred, sql_buf, sizeof(sql_buf)))) {
        LOG_WARN("failed to gen task clear sql", K(ret), K(table_id));
      } else if (OB_FAIL(dml_op.execute_write(sql_buf))) {
        LOG_WARN("failed to clear ai task rows", K(ret), K(table_id));
      }
    }
    if (session_started) {
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = false;
      int tmp_ret = dml_op.end_nested_session();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to end nested session", K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

// The main update of this transaction already wrote the row once, so the
// invalidation must not look like a second plain UPDATE on the same row (the
// memtable defensive check rejects that). Re-issuing the row as
// insert ... on duplicate key update <result cols> = NULL carries the
// insert-up write flag, which the check explicitly tolerates, and the rowkey
// values locate the row without a predicate.
int ObAiColumnDmlHelper::gen_invalidate_sql(
    const ObString &db_name,
    const ObString &tbl_name,
    const ObIArray<ObAiColEntry> &entries,
    const ObTableSchema &table_schema,
    const ExprFixedArray &row,
    const ObIArray<uint64_t> &das_column_ids,
    ObEvalCtx &eval_ctx,
    const ObObjPrintParams &print_params,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t emitted = 0;
  if (OB_ISNULL(buf) || buf_len <= 0 || row.count() != das_column_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row.count()), K(das_column_ids.count()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "insert into `%.*s`.`%.*s` (",
             db_name.length(), db_name.ptr(), tbl_name.length(), tbl_name.ptr()))) {
    LOG_WARN("failed to start invalidate sql", K(ret));
  }
  // column list: everything the DAS row carries except hidden/generated cols
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count(); ++i) {
    const ObColumnSchemaV2 *col = table_schema.get_column_schema(das_column_ids.at(i));
    if (OB_ISNULL(col) || col->is_hidden() || col->is_generated_column()) {
      // not addressable through an insert
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s`%.*s`",
               emitted > 0 ? ", " : "", col->get_column_name_str().length(),
               col->get_column_name_str().ptr()))) {
      LOG_WARN("failed to add invalidate column", K(ret));
    } else {
      ++emitted;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == emitted) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no writable columns for invalidate", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") values ("))) {
    LOG_WARN("failed to build invalidate values head", K(ret));
  }
  int64_t value_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count(); ++i) {
    const ObColumnSchemaV2 *col = table_schema.get_column_schema(das_column_ids.at(i));
    if (OB_ISNULL(col) || col->is_hidden() || col->is_generated_column()) {
      // keep aligned with the column list pass
    } else {
      ObDatum *datum = NULL;
      ObObj obj;
      if (value_cnt > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        LOG_WARN("failed to separate invalidate values", K(ret));
      } else if (OB_FAIL(row.at(i)->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval invalidate row expr", K(ret));
      } else if (FALSE_IT(obj.reset())) {
      } else if (OB_FAIL(datum->to_obj(obj, row.at(i)->obj_meta_))) {
        LOG_WARN("failed to cast invalidate datum", K(ret));
      } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos, print_params))) {
        LOG_WARN("failed to print invalidate value", K(ret));
      } else {
        ++value_cnt;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (value_cnt != emitted) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalidate column/value mismatch", K(ret), K(emitted), K(value_cnt));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") on duplicate key update "))) {
    LOG_WARN("failed to build invalidate tail head", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
    if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
      LOG_WARN("failed to separate invalidate sets", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%.*s` = NULL",
               entries.at(i).name_.length(), entries.at(i).name_.ptr()))) {
      LOG_WARN("failed to append invalidate null set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    buf[pos] = '\0';
  }
  return ret;
}

int ObAiColumnDmlHelper::before_update_row(
    const ObUpdCtDef &upd_ctdef,
    const ObDASTabletLoc *old_tablet_loc,
    ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObTableModifyOp &dml_op = dml_rtctx.op_;
  if (!upd_ctdef.is_primary_index_ || dml_op.is_nested_session()) {
    // secondary index rows and inner sqls never touch AI results
  } else {
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    ObIAllocator &alloc = dml_rtctx.get_exec_ctx().get_allocator();
    ObEvalCtx &eval_ctx = dml_rtctx.get_eval_ctx();
    ObSEArray<ObAiColEntry, 4> entries;
    if (OB_FAIL(fetch_table_schema(dml_rtctx, old_tablet_loc, schema_guard, table_schema))) {
      LOG_WARN("failed to fetch table schema", K(ret));
    } else if (OB_FAIL(collect_async_ai_columns(*table_schema, alloc, entries))) {
      LOG_WARN("failed to collect ai columns", K(ret));
    } else if (entries.empty()) {
      // fast path for tables without async AI columns
    } else {
      // When the rowkey moves, the split delete+insert serializes the new
      // row from these expressions, so nulling the stale results here is
      // the way to land NULL without racing an extra write against the row
      // move. A plain dependency update persists only the assigned columns;
      // after_update_row drops its stale results with an insert-up write.
      const bool upd_pk = upd_ctdef.das_base_ctdef_.is_update_pk_;
      for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
        bool hit = upd_pk;
        for (int64_t j = 0; OB_SUCC(ret) && !hit
             && j < upd_ctdef.assign_columns_.count(); ++j) {
          const uint64_t slot = upd_ctdef.assign_columns_.at(j).projector_index_;
          const ObColumnSchemaV2 *set_col =
              (slot < (uint64_t)upd_ctdef.column_ids_.count())
                  ? table_schema->get_column_schema(upd_ctdef.column_ids_.at(slot))
                  : NULL;
          bool found = false;
          if (OB_NOT_NULL(set_col)
              && OB_FAIL(ai_column_expr_refers_identifier(entries.at(i).expr_,
                                                          set_col->get_column_name_str(),
                                                          found))) {
            LOG_WARN("failed to scan ai expression", K(ret));
          } else if (found) {
            hit = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!hit) {
          // this result survives the update untouched
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < upd_ctdef.new_row_.count()
               && j < upd_ctdef.column_ids_.count(); ++j) {
            if (upd_ctdef.column_ids_.at(j) == entries.at(i).column_id_) {
              ObDatum *datum = NULL;
              if (OB_FAIL(upd_ctdef.new_row_.at(j)->eval(eval_ctx, datum))) {
                LOG_WARN("failed to eval stale ai result", K(ret));
              } else {
                datum->set_null();
              }
            }
          }
          // the DAS update serializes full_row_, keep it in step with new_row_
          for (int64_t j = 0; OB_SUCC(ret) && j < upd_ctdef.full_row_.count()
               && j < upd_ctdef.dupd_ctdef_.column_ids_.count(); ++j) {
            if (upd_ctdef.dupd_ctdef_.column_ids_.at(j) == entries.at(i).column_id_) {
              ObDatum *datum = NULL;
              if (OB_FAIL(upd_ctdef.full_row_.at(j)->eval(eval_ctx, datum))) {
                LOG_WARN("failed to eval stale ai result in full row", K(ret));
              } else {
                datum->set_null();
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAiColumnDmlHelper::after_update_row(
    const ObUpdCtDef &upd_ctdef,
    const ObDASTabletLoc *old_tablet_loc,
    const ObDASTabletLoc *new_tablet_loc,
    const bool is_row_changed,
    ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObTableModifyOp &dml_op = dml_rtctx.op_;
  if (!is_row_changed || !upd_ctdef.is_primary_index_ || dml_op.is_nested_session()) {
    // row locks and inner sqls never enqueue AI tasks
  } else {
    // An update always writes the full new row through the DAS update ctdef,
    // including the delete+insert split used when the rowkey changes.
    const ObDASTabletLoc *loc = old_tablet_loc;
    const bool upd_pk = upd_ctdef.das_base_ctdef_.is_update_pk_;
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    ObIAllocator &alloc = dml_rtctx.get_exec_ctx().get_allocator();
    ObSEArray<ObAiColEntry, 4> entries;
    ObSEArray<ObAiColEntry, 4> hit_entries;
    char pred_buf[PRED_BUF_LEN];
    char sql_buf[SQL_BUF_LEN];
    ObString pred;
    bool session_started = false;
    if (OB_FAIL(fetch_table_schema(dml_rtctx, loc, schema_guard, table_schema))) {
      LOG_WARN("failed to fetch table schema", K(ret));
    } else if (OB_FAIL(collect_async_ai_columns(*table_schema, alloc, entries))) {
      LOG_WARN("failed to collect ai columns", K(ret));
    } else if (entries.empty()) {
      // fast path for tables without async AI columns
    } else {
      // A result goes stale when a column referenced by its expression is
      // updated, or when the rowkey moves (the task targets the new key).
      for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
        bool hit = upd_pk;
        // assign_columns_ marks the true SET targets: column_ids_ covers
        // the whole rewritten row, so matching against it would make the
        // worker's own publish update look like a dependency change and
        // re-enqueue tasks forever
        for (int64_t j = 0;
             OB_SUCC(ret) && !hit && j < upd_ctdef.assign_columns_.count(); ++j) {
          const uint64_t slot = upd_ctdef.assign_columns_.at(j).projector_index_;
          const ObColumnSchemaV2 *set_col =
              (slot < (uint64_t)upd_ctdef.column_ids_.count())
                  ? table_schema->get_column_schema(upd_ctdef.column_ids_.at(slot))
                  : NULL;
          bool found = false;
          if (OB_NOT_NULL(set_col)
              && OB_FAIL(ai_column_expr_refers_identifier(entries.at(i).expr_,
                                                           set_col->get_column_name_str(),
                                                           found))) {
            LOG_WARN("failed to scan ai expression", K(ret));
          } else if (found) {
            hit = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (hit && OB_FAIL(hit_entries.push_back(entries.at(i)))) {
          LOG_WARN("failed to push back hit entry", K(ret));
        }
      }
    }
    if (OB_FAIL(ret) || hit_entries.empty()) {
      // no async AI result of this row is affected by the update
    } else if (OB_FAIL(build_rowkey_predicate(*table_schema, upd_ctdef.new_row_,
               upd_ctdef.dupd_ctdef_.column_ids_, dml_rtctx.get_eval_ctx(),
               dml_op.get_obj_print_params(), pred_buf, sizeof(pred_buf), pred))) {
      LOG_WARN("failed to build rowkey predicate", K(ret));
    } else if (OB_FAIL(dml_op.open_inner_conn())) {
      LOG_WARN("failed to open inner conn for ai tasks", K(ret));
    } else if (OB_FAIL(dml_op.begin_nested_session(true))) {
      LOG_WARN("failed to begin nested session", K(ret));
    } else {
      session_started = true;
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = true;
      const uint64_t table_id = loc->loc_meta_->ref_table_id_;
      const int64_t request_id = gen_request_id();
      const ObDatabaseSchema *db_schema = NULL;
      if (!upd_pk) {
        // A dependency update must also drop the stale result from storage:
        // the DAS update only persists the assigned columns, so nulling the
        // pending datums alone would never reach the row. A primary-key
        // move instead serializes the new row from new_row_ (the split
        // delete+insert), where before_update_row already nulled the
        // results; an insert-up there would materialize the new row ahead
        // of the split and trip its own duplicate key.
        if (OB_FAIL(schema_guard->get_database_schema(table_schema->get_database_id(),
                                                      db_schema))) {
          LOG_WARN("failed to get database schema", K(ret));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null database schema", K(ret));
        } else if (OB_FAIL(gen_invalidate_sql(db_schema->get_database_name_str(),
                                              table_schema->get_table_name_str(),
                                              hit_entries, *table_schema,
                                              upd_ctdef.new_row_,
                                              upd_ctdef.dupd_ctdef_.column_ids_,
                                              dml_rtctx.get_eval_ctx(),
                                              dml_op.get_obj_print_params(),
                                              sql_buf, sizeof(sql_buf)))) {
          LOG_WARN("failed to gen invalidate sql", K(ret));
        } else if (OB_FAIL(dml_op.execute_write(sql_buf))) {
          LOG_WARN("failed to invalidate ai column results", K(ret), K(table_id));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < hit_entries.count(); ++i) {
        if (OB_FAIL(gen_task_insert_sql(table_id, hit_entries.at(i), request_id, pred,
                                        sql_buf, sizeof(sql_buf)))) {
          LOG_WARN("failed to gen task sql", K(ret), K(table_id),
                   K(hit_entries.at(i).column_id_));
        } else if (OB_FAIL(dml_op.execute_write(sql_buf))) {
          LOG_WARN("failed to write ai task row", K(ret), K(table_id),
                   K(hit_entries.at(i).column_id_));
        }
      }
    }
    if (session_started) {
      dml_op.get_exec_ctx().get_das_ctx().is_ai_column_nested_ = false;
      int tmp_ret = dml_op.end_nested_session();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to end nested session", K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
