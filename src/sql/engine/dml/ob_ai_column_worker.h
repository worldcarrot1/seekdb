// Async worker driving AI COLUMN tasks (competition 800493).
#ifndef OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_WORKER_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_WORKER_H_
#include <atomic>
#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{
// ObAiColumnWorker periodically drains __all_ai_column_task: it claims a
// request group (all AI columns of one row share one request id), evaluates
// each column expression against its row, calls the AI endpoint configured
// through DBMS_AI_SERVICE, and publishes the whole group in a single
// transaction so users never observe a partially refreshed row. A row whose
// newer request already queued a task makes the older result stale: it is
// dropped instead of overwriting fresher data. Terminal failures (retry
// budget exhausted) remove the task and leave the result NULL.
class ObAiColumnWorker final
{
public:
  static ObAiColumnWorker &get_instance();
  // First DML that enqueues a task starts the timer thread; every later
  // call is a relaxed atomic check.
  int ensure_started();
  bool is_running() const { return is_running_.load(std::memory_order_acquire); }
private:
  ObAiColumnWorker();
  ~ObAiColumnWorker();
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnWorker);

  class ObAiColumnScanTask : public common::ObTimerTask
  {
  public:
    ObAiColumnScanTask() {}
    virtual ~ObAiColumnScanTask() {}
    virtual void runTimerTask();
  };

  int init_and_start();
  void do_work();

  static const int64_t SCAN_INTERVAL_US = 200 * 1000L;
  static const int64_t HTTP_TIMEOUT_SEC = 30;
  static const int64_t CLAIM_LEASE_SEC = 300;
  static const int64_t RETRY_BACKOFF_MS = 800;

  common::ObTimer timer_;
  ObAiColumnScanTask scan_task_;
  std::atomic<bool> inited_;
  std::atomic<bool> is_running_;
  std::atomic<bool> working_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_WORKER_H_
