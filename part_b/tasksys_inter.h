#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>

template <typename T> class ThreadSafeQueue {
public:
  ThreadSafeQueue() {};
  void Push(T new_value) {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    m_data_queue.push(std::move(new_value));
    m_data_cond.notify_one();
  }
  void WaitAndPop(T &value) {
    std::unique_lock<std::mutex> ul(m_queue_mtx);
    m_data_cond.wait(ul, [this]() { return !m_data_queue.empty(); });
    value = std::move(m_data_queue.front());
    m_data_queue.pop();
  }
  bool Empty() const {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    return m_data_queue.empty();
  }

private:
  std::queue<T> m_data_queue;
  mutable std::mutex m_queue_mtx;
  std::condition_variable m_data_cond;
};

class WorkerThread {
public:
  explicit WorkerThread(int index) {}

private:
  ThreadSafeQueue<std::function<void()>> m_workqueue;
  int m_index;
  int m_local_run_num;
};

/** @brief A bulk task, can indenpently run by different executors.
 *       It does not matter each single task is executed by which executor.
 */
class BulkTask {
public:
  /** @brief construct a bulk task which contains n tasks. */
  explicit BulkTask(int n, IRunnable *runnable)
      : m_num_total(n), m_num_finish(0), m_context(runnable) {};
  ~BulkTask() = default;

  /** @brief Check if all single task has been finished. (Thread-Safe) */
  bool AllFinished();
  /** @brief Update finish num. (Thread-Safe)*/
  void UpdateFinishCount();

  /** @brief Send a task to TaskSystem. And tasksystem can decide the
   * strategy to run each task.
   */
  bool TryPopTask(ITaskSystem *tasksys);

private:
  /* Total num of bulk task */
  const int m_num_total;
  /* Number of tasks which has been executed in bulk task. */
  mutable std::atomic<int> m_num_finish;
  /* Context that bulk task need to run. */
  const IRunnable *m_context;
};

inline bool BulkTask::AllFinished() { return m_num_finish == m_num_total; }

inline void BulkTask::UpdateFinishCount() { ++m_num_finish; }

inline bool BulkTask::TryPopTask(ITaskSystem *tasksys) {
  if (AllFinished())
    return false;
}