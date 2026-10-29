// Meta row operators for AI COLUMN (competition 800493). Shared by rootserver
// DDL paths so __all_ai_column stays consistent with table schemas.
#ifndef OCEANBASE_SHARE_OB_AI_COLUMN_META_OPERATOR_H_
#define OCEANBASE_SHARE_OB_AI_COLUMN_META_OPERATOR_H_
#include <stdint.h>
#include "share/schema/ob_table_schema.h"
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
}
class ObAiColumnMetaOperator
{
public:
  static int persist_table_ai_columns(const schema::ObTableSchema &table_schema,
                                      common::ObMySQLTransaction &trans);
  static int persist_alter_add_ai_column(const schema::ObTableSchema &table_schema,
                                         const schema::ObColumnSchemaV2 &spec_col,
                                         common::ObMySQLTransaction &trans);
  static int delete_table_ai_meta(const uint64_t table_id,
                                  common::ObMySQLTransaction &trans);
};
} // end of namespace share
} // end of namespace oceanbase
#endif // OCEANBASE_SHARE_OB_AI_COLUMN_META_OPERATOR_H_
