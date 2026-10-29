// Meta row operators for AI COLUMN (competition 800493).
#define USING_LOG_PREFIX SHARE
#include "share/ob_ai_column_meta_operator.h"
#include "common/mysqlclient/ob_isql_client.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_ai_column_common.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace
{
bool ai_col_name_has_suffix(const common::ObString &name,
                            const common::ObString &base,
                            const char *suffix)
{
  bool bool_ret = false;
  const int64_t slen = static_cast<int64_t>(STRLEN(suffix));
  if (name.length() == base.length() + slen
      && base.length() > 0
      && 0 == STRNCASECMP(name.ptr(), base.ptr(), base.length())) {
    bool_ret = true;
  }
  return bool_ret;
}
} // end of anonymous namespace

int ObAiColumnMetaOperator::persist_alter_add_ai_column(
    const schema::ObTableSchema &table_schema,
    const schema::ObColumnSchemaV2 &spec_col,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator alloc("AiColMeta");
  share::ObAIColumnSpec spec;
  common::ObString base_name = spec_col.get_column_name_str();
  const int64_t suffix_len = static_cast<int64_t>(STRLEN(share::AI_COLUMN_SPEC_SUFFIX));
  const share::schema::ObColumnSchemaV2 *result_col = NULL;
  const share::schema::ObColumnSchemaV2 *state_col = NULL;
  const share::schema::ObColumnSchemaV2 *request_col = NULL;
  common::ObSEArray<uint64_t, 32> column_ids;
  if (base_name.length() <= suffix_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ai spec column name", K(ret), K(base_name));
  } else {
    base_name = common::ObString(
        static_cast<int32_t>(base_name.length() - suffix_len), base_name.ptr());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(share::decode_ai_column_spec(spec_col.get_comment_str(), alloc, spec))) {
    LOG_WARN("failed to decode ai column spec", K(ret));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else {
    for (int64_t i = 0; i < column_ids.count(); ++i) {
      const share::schema::ObColumnSchemaV2 *col =
          table_schema.get_column_schema(column_ids.at(i));
      if (NULL == col) {
      } else if (col->is_ai_result_column()
                 && col->get_column_name_str().case_compare_equal(base_name)) {
        result_col = col;
      } else if (col->is_ai_state_column()
                 && ai_col_name_has_suffix(col->get_column_name_str(), base_name,
                                           share::AI_COLUMN_STATE_SUFFIX)) {
        state_col = col;
      } else if (col->is_ai_request_column()
                 && ai_col_name_has_suffix(col->get_column_name_str(), base_name,
                                           share::AI_COLUMN_REQUEST_SUFFIX)) {
        request_col = col;
      }
    }
    if (NULL == result_col || NULL == state_col || NULL == request_col) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column layout broken", K(ret), K(base_name));
    } else {
      share::ObDMLSqlSplicer splicer;
      common::ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(splicer.add_pk_column("table_id", table_schema.get_table_id()))
          || OB_FAIL(splicer.add_pk_column("column_id", result_col->get_column_id()))) {
        LOG_WARN("failed to add ai meta pk columns", K(ret));
      } else if (OB_FAIL(splicer.add_column(
                     "column_name", common::ObHexEscapeSqlStr(base_name)))) {
      } else if (OB_FAIL(splicer.add_column("state_column_id", state_col->get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column("request_column_id", request_col->get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column("result_spec_column_id", spec_col.get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column(
                     "normalized_expr", common::ObHexEscapeSqlStr(spec.expr_)))) {
      } else if (OB_FAIL(splicer.add_column(
                     "sync_mode", common::ObHexEscapeSqlStr(spec.sync_mode_)))) {
      } else if (OB_FAIL(splicer.add_column("max_retries", spec.max_retries_))) {
      } else if (OB_FAIL(splicer.add_column(
                     "result_type", common::ObHexEscapeSqlStr(spec.result_type_)))) {
      } else if (0 != spec.result_dim_
                 && OB_FAIL(splicer.add_column("result_dim", spec.result_dim_))) {
      } else if (OB_FAIL(splicer.splice_insert_sql(OB_ALL_AI_COLUMN_TNAME, sql))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ai meta affected rows", K(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObAiColumnMetaOperator::persist_table_ai_columns(
    const schema::ObTableSchema &table_schema,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<uint64_t, 32> column_ids;
  if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    const share::schema::ObColumnSchemaV2 *col =
        table_schema.get_column_schema(column_ids.at(i));
    if (NULL != col && col->is_ai_spec_column()
        && share::is_ai_column_spec_comment(col->get_comment_str())) {
      if (OB_FAIL(persist_alter_add_ai_column(table_schema, *col, trans))) {
        LOG_WARN("failed to persist ai column meta", K(ret));
      }
    }
  }
  return ret;
}

int ObAiColumnMetaOperator::delete_table_ai_meta(
    const uint64_t table_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE table_id = %lu",
                             OB_ALL_AI_COLUMN_TNAME, table_id))) {
    LOG_WARN("failed to build ai column delete sql", K(ret), K(table_id));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete ai column meta", K(ret), K(table_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE table_id = %lu",
                                    OB_ALL_AI_COLUMN_TASK_TNAME, table_id))) {
    LOG_WARN("failed to build ai column task delete sql", K(ret), K(table_id));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete ai column task meta", K(ret), K(table_id));
  }
  return ret;
}

} // end of namespace share
} // end of namespace oceanbase
