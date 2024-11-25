#pragma once

#include "itasksys.h"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

template <typename T> class ThreadSafeQueue {
public:
  ThreadSafeQueue() {};
  void PushThreadSafe(T new_value) {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    m_data_queue.push(std::move(new_value));
    m_data_cond.notify_one();
  }

  void WaitAndPopThreadSafe(T &value) {
    std::unique_lock<std::mutex> ul(m_queue_mtx);
    m_data_cond.wait(ul, [this]() { return !m_data_queue.empty(); });
    value = std::move(m_data_queue.front());
    m_data_queue.pop();
  }

  std::shared_ptr<T> WaitAndPopThreadSafe() {
    std::unique_lock<std::mutex> ul(m_queue_mtx);
    m_data_cond.wait(ul, [this] { return !m_data_queue.empty(); });
    std::shared_ptr<T> res(
        std::make_shared<T>(std::move(m_data_queue.front())));
    m_data_queue.pop();
    return res;
  }

  bool TryPopThreadSafe(T &value) {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    if (m_data_queue.empty())
      return false;
    value = std::move(m_data_queue.front());
    m_data_queue.pop();
    return true;
  }

  bool EmptyThreadSafe() const {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    return m_data_queue.empty();
  }

  int SizeThreadSafe() const {
    std::lock_guard<std::mutex> lg(m_queue_mtx);
    return m_data_queue.size();
  }

private:
  std::queue<T> m_data_queue;
  mutable std::mutex m_queue_mtx;
  std::condition_variable m_data_cond;
};

template <typename T> class ThreadSafeAutoBalanceQueue {
public:
  explicit ThreadSafeAutoBalanceQueue(int num)
      : m_queue_num(num), m_push_cnt(0), m_threadsafe_queue_vec(num){
    if (m_indices.empty()) {
      m_indices.resize(m_queue_num);
      std::iota(m_indices.begin(), m_indices.end(), 0);
    }
  }

  void RoundRobinPush(T new_value) {
    int queue_id = m_push_cnt % m_queue_num;
    m_push_cnt++;
    m_threadsafe_queue_vec[queue_id].PushThreadSafe(new_value);
  }

  ThreadSafeQueue<T> &GetWorkQueue(int id) {
    return m_threadsafe_queue_vec[id];
  }

  bool TryAutoBalance(int id) {
    std::shuffle(m_indices.begin(), m_indices.end(),
                 std::mt19937{std::random_device{}()});
    for (auto i : m_indices) {
      if (i == id)
        continue;
      if (GetWorkQueue(i).SizeThreadSafe() >= 2) {
        std::shared_ptr<T> stolen_value =
            GetWorkQueue(i).WaitAndPopThreadSafe();
        GetWorkQueue(id).PushThreadSafe(*stolen_value);
        return true;
      }
    }
    return false;
  }

  void AutoBalancePop(int id, T &value) {
    if (GetWorkQueue(id).EmptyThreadSafe() == false) {
      GetWorkQueue(id).WaitAndPopThreadSafe(value);
      return;
    }

    if (false == TryAutoBalance(id)) {
      // if cannot steal, wait.
      GetWorkQueue(id).WaitAndPopThreadSafe(value);
    }
  }

private:
  const int m_queue_num;
  int m_push_cnt;
  std::vector<int> m_indices;
  std::vector<ThreadSafeQueue<T>> m_threadsafe_queue_vec;
};

class WorkerThread {
public:
  explicit WorkerThread(
      int index,
      std::shared_ptr<ThreadSafeAutoBalanceQueue<std::function<void()>>> queue,
      const std::atomic<bool> &shutdown_flag)
      : m_shared_work_queue(queue), m_shutdown_flag(shutdown_flag),
        m_index(index) {}

  void operator()() { WorkerLoop(); }

  void WorkerLoop() {
    while (m_shutdown_flag != true) {
      std::function<void()> task;
      m_shared_work_queue->AutoBalancePop(m_index, task);
      task();
    }
  }

private:
  std::shared_ptr<ThreadSafeAutoBalanceQueue<std::function<void()>>>
      m_shared_work_queue;
  const std::atomic<bool> &m_shutdown_flag;
  int m_index; // index for work queue
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

  std::vector<std::function<void()>> GetSubTasks();

  std::function<void()> GetSubTaskWithID(int id);

public:
  /* Total num of bulk task */
  const int m_num_total;
  /* Number of tasks which has been executed in bulk task. */
  mutable std::atomic<int> m_num_finish;
  /* Context that bulk task need to run. */
  IRunnable *m_context;
};

inline bool BulkTask::AllFinished() { return m_num_finish == m_num_total; }

inline void BulkTask::UpdateFinishCount() { ++m_num_finish; }

inline std::vector<std::function<void()>> BulkTask::GetSubTasks() {
  std::vector<std::function<void()>> vec(m_num_total);
  for (int i = 0; i < m_num_total; i++) {
    GetSubTaskWithID(i);
  }
  return vec;
}

inline std::function<void()> BulkTask::GetSubTaskWithID(int id) {
  auto subtask = [this, id]() {
    m_context->runTask(id, m_num_total);
    m_num_finish++;
  };
  return subtask;
}

class WorkerManager {
public:
  explicit WorkerManager(int thread_num) : m_thread_num(thread_num) {
    m_shared_work_queue =
        std::make_shared<ThreadSafeAutoBalanceQueue<std::function<void()>>>(
            thread_num);
    for (int i = 0; i < m_thread_num; i++) {
      m_thread_pool.emplace_back(
          WorkerThread(i, m_shared_work_queue, m_shutdown));
    }
  }

  ~WorkerManager() {
    m_shutdown = true;
    for (auto &thread : m_thread_pool) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  void SetShutdown() { m_shutdown = true; };

  void RunBulkTask(BulkTask &task) {
    // TODO: run task with WorkerManager
    auto vec = task.GetSubTasks();
    for (auto &subtask : vec) {
      m_shared_work_queue->RoundRobinPush(subtask);
    }
  }

private:
  std::atomic<bool> m_shutdown;
  int m_thread_num;
  std::shared_ptr<ThreadSafeAutoBalanceQueue<std::function<void()>>>
      m_shared_work_queue;
  std::vector<std::thread> m_thread_pool;
};
