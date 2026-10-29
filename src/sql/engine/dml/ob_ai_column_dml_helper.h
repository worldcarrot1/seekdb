// DML integration for async AI COLUMN (competition 800493).
#ifndef OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HELPER_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HELPER_H_

#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
}
}
namespace sql
{
class ObDASTabletLoc;

// ObAiColumnDmlHelper keeps the __all_ai_column_task queue in sync with
// row level DML on tables that contain async AI columns:
//   insert : enqueue one pending task per async AI column of the new row;
//   update : set stale results back to NULL and enqueue tasks when a column
//            referenced by an AI expression (or the rowkey) is updated;
//   delete : drop pending tasks belonging to the deleted row.
// All queue writes go through a nested session inside the business
// transaction, so task rows share statement visibility and rollback
// semantics with the DML that produced them.
class ObAiColumnDmlHelper
{
public:
  static int after_insert_row(const ObInsCtDef &ins_ctdef,
                              const ObDASTabletLoc *tablet_loc,
                              ObDMLRtCtx &dml_rtctx);
  static int after_delete_row(const ObDelCtDef &del_ctdef,
                              const ObDASTabletLoc *tablet_loc,
                              ObDMLRtCtx &dml_rtctx);
  static int before_update_row(const ObUpdCtDef &upd_ctdef,
                               const ObDASTabletLoc *old_tablet_loc,
                               ObDMLRtCtx &dml_rtctx);
  static int after_update_row(const ObUpdCtDef &upd_ctdef,
                              const ObDASTabletLoc *old_tablet_loc,
                              const ObDASTabletLoc *new_tablet_loc,
                              const bool is_row_changed,
                              ObDMLRtCtx &dml_rtctx);
private:
  struct ObAiColEntry
  {
    ObAiColEntry() : column_id_(common::OB_INVALID_ID) {}
    TO_STRING_KV(K_(column_id), K_(name));
    uint64_t column_id_;
    common::ObString name_;        // result column name
    common::ObString expr_;        // normalized AI expression text
    common::ObString result_type_; // "json" / "vector" / "text"
  };
  static const int64_t PRED_BUF_LEN = 2048;
  static const int64_t SQL_BUF_LEN = 8192;
  static int64_t gen_request_id();
  static int fetch_table_schema(ObDMLRtCtx &dml_rtctx,
                                const ObDASTabletLoc *tablet_loc,
                                share::schema::ObSchemaGetterGuard *&schema_guard,
                                const share::schema::ObTableSchema *&table_schema);
  static int collect_async_ai_columns(const share::schema::ObTableSchema &table_schema,
                                      common::ObIAllocator &alloc,
                                      common::ObIArray<ObAiColEntry> &entries);
  static int build_rowkey_predicate(const share::schema::ObTableSchema &table_schema,
                                    const ExprFixedArray &row,
                                    const common::ObIArray<uint64_t> &das_column_ids,
                                    ObEvalCtx &eval_ctx,
                                    const common::ObObjPrintParams &print_params,
                                    char *buf,
                                    const int64_t buf_len,
                                    common::ObString &pred);
  static int gen_invalidate_sql(const common::ObString &db_name,
                                const common::ObString &tbl_name,
                                const common::ObIArray<ObAiColEntry> &entries,
                                const share::schema::ObTableSchema &table_schema,
                                const ExprFixedArray &row,
                                const common::ObIArray<uint64_t> &das_column_ids,
                                ObEvalCtx &eval_ctx,
                                const common::ObObjPrintParams &print_params,
                                char *buf,
                                const int64_t buf_len);
  static int append_escaped(common::ObSqlString &sql, const common::ObString &raw);
  static int gen_task_insert_sql(const uint64_t table_id,
                                 const ObAiColEntry &entry,
                                 const int64_t request_id,
                                 const common::ObString &rowkey_pred,
                                 char *buf,
                                 const int64_t buf_len);
  static int gen_clear_tasks_sql(const uint64_t table_id,
                                 const common::ObString &rowkey_pred,
                                 char *buf,
                                 const int64_t buf_len);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HELPER_H_
