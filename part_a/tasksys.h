#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include "thread_pool.h"
#include <algorithm>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <list>
#include <chrono>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

  int m_num_threads{0};
  std::mutex m_task_mtx;
  std::atomic<int> m_task_id{0};
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  using Task = std::function<void()>;

  void InitPool(int num_threads);
  void Shutdown();
  void AddTask(Task &task, int task_id);
  std::mutex m_task_mtx;
  std::vector<std::thread> m_thread_pool;
  std::queue<Task> m_task_queue;
  std::vector<std::queue<Task>> m_local_task_queue;
  std::atomic<bool> m_shutdown{false};
  std::condition_variable m_finish_cond;
  std::atomic<int> m_task_finish_cnt{0};
  int m_num_threads;
  int m_num_total_tasks;
  std::vector<std::mutex> m_local_mtx;

  class WorkerThread {
  public:
    explicit WorkerThread(TaskSystemParallelThreadPoolSpinning *tasksys, int index)
        : m_tasksys(tasksys), m_index(index) {
      m_worker_tasks_max_size = 2000;
    }

    ~WorkerThread() = default;

    // void GetTaskWithRace() {
    //   std::unique_lock<std::mutex> ul(m_tasksys->m_task_mtx);
    //   if (!m_tasksys->m_task_queue.empty()) {
    //     for (int pop_cnt = 0; !m_tasksys->m_task_queue.empty() &&
    //                           pop_cnt < m_worker_tasks_max_size;
    //          pop_cnt++) {
    //       Task task = m_tasksys->m_task_queue.front();
    //       m_tasksys->m_task_queue.pop();
    //       m_worker_tasks.push(std::move(task));
    //     }
    //     ul.unlock();
    //   }
    // };

    void RunLocalTask() {
      int local_run_num = 0;
      // std::cout << m_tasksys->m_local_task_queue.at(m_index).size()<< " ";
      std::unique_lock<std::mutex> ul(m_tasksys->m_local_mtx.at(m_index)); 
      if (!m_tasksys->m_local_task_queue.at(m_index).empty()) {
        Task task = m_tasksys->m_local_task_queue.at(m_index).front();
        m_tasksys->m_local_task_queue.at(m_index).pop();
        ul.unlock();
        task();
        local_run_num += 1;
      }
      m_tasksys->m_task_finish_cnt += local_run_num;
    }

    // if local task queue is empty, try to steal work from other workers.
    void StealWork() {
      std::unique_lock<std::mutex> ul_local(m_tasksys->m_local_mtx.at(m_index));
      if (m_tasksys->m_local_task_queue.at(m_index).empty()) {
        ul_local.unlock();
        for (int i = 0; i < m_tasksys->m_num_threads && i != m_index; i++) {
          std::unique_lock<std::mutex> ul(m_tasksys->m_local_mtx.at(i)); 
          if (!m_tasksys->m_local_task_queue.at(i).empty()) {
            //lock
            Task steal_task = m_tasksys->m_local_task_queue.at(i).front();

            // std::cout << "task steal!\n";
            // std::cout << m_tasksys->m_local_task_queue.at(i).size()<< " ";
            m_tasksys->m_local_task_queue.at(i).pop();
            ul.unlock();
            
            //unlock
            ul_local.lock();
            m_tasksys->m_local_task_queue.at(m_index).push(steal_task);
            ul_local.unlock();
            break;
          }
        }
      }
    }

    void operator()() {
      while (!m_tasksys->m_shutdown) {
        // GetTaskWithRace();
        RunLocalTask();
        StealWork();
      }
    }

  private:
    int m_worker_tasks_max_size;
    std::queue<Task> m_worker_tasks;
    TaskSystemParallelThreadPoolSpinning *m_tasksys;
    int m_index;
  };
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  using Task = std::function<void()>;
  void AddTask(Task &task, int task_id);
  std::mutex m_task_mtx;
  std::vector<std::thread> m_thread_pool;
  std::queue<Task> m_task_queue;
  std::atomic<bool> m_shutdown{false};
  std::atomic<int> m_task_finish_cnt{0};
  int m_num_threads;
  int m_num_total_tasks;
  std::vector<std::mutex> m_local_mtx;
  std::vector<std::condition_variable> m_local_cond;
  std::condition_variable m_finish_cond;
  std::vector<std::deque<Task>> m_local_task_queue;

  class WorkerThread {
  public:
    explicit WorkerThread(TaskSystemParallelThreadPoolSleeping *tasksys, int index)
      : m_tasksys(tasksys), m_index(index){
    }


    void RunLocalTask() {
      std::unique_lock<std::mutex> ul_local(m_tasksys->m_local_mtx.at(m_index), std::try_to_lock);
      if (ul_local.owns_lock()) {

      if(!m_tasksys->m_local_task_queue.at(m_index).empty()) 
      {
        Task task = std::move(m_tasksys->m_local_task_queue.at(m_index).front());
        m_tasksys->m_local_task_queue.at(m_index).pop_front();
        ul_local.unlock();
        task();
        m_local_run_num += 1;
      } else {
        ul_local.unlock();
        m_tasksys->m_task_finish_cnt += m_local_run_num;
        m_local_run_num = 0;
        if (StealWorkWhenEmpty()) {
        // if cannot steal, sleep, let main thread submit remaining tasks
          WaitForTask();
        }
      }
      }
    }

    void WaitForTask() {
      std::unique_lock<std::mutex> ul_local(m_tasksys->m_local_mtx[m_index]);
      m_tasksys->m_local_cond[m_index].wait(ul_local, [this]() {
        return !m_tasksys->m_local_task_queue[m_index].empty() || m_tasksys->m_shutdown;
      });
    }

    bool StealWorkWhenEmpty() {
      bool need_sleep = true;
      for (int i = 0; i < m_tasksys->m_num_threads && i != m_index; i++) {
        std::unique_lock<std::mutex> ul(m_tasksys->m_local_mtx.at(i), std::try_to_lock);
        if (ul.owns_lock() && !m_tasksys->m_local_task_queue.at(i).empty()) {
          need_sleep = false;
          Task steal_task = std::move(m_tasksys->m_local_task_queue.at(i).back());
          m_tasksys->m_local_task_queue.at(i).pop_back();
          ul.unlock();

          std::unique_lock<std::mutex> ul_local(m_tasksys->m_local_mtx.at(m_index));
          m_tasksys->m_local_task_queue.at(m_index).push_back(steal_task);
          ul_local.unlock();
          break;
        }
      }
      return need_sleep;
    }

    void StealWork() {
      std::unique_lock<std::mutex> ul_local(m_tasksys->m_local_mtx.at(m_index));
      if (m_tasksys->m_local_task_queue.at(m_index).empty()) {
        ul_local.unlock();
        for (int i = 0; i < m_tasksys->m_num_threads && i != m_index; i++) {
          std::unique_lock<std::mutex> ul(m_tasksys->m_local_mtx.at(i));
          if (!m_tasksys->m_local_task_queue.at(i).empty()) {
            Task steal_task = std::move(m_tasksys->m_local_task_queue.at(i).back());
            m_tasksys->m_local_task_queue.at(i).pop_back();
            ul.unlock();

            ul_local.lock();
            m_tasksys->m_local_task_queue.at(m_index).push_back(steal_task);
            ul_local.unlock();
            break;
          }
        }
      }
    }

    void operator()() {
      while (!m_tasksys->m_shutdown) {
        RunLocalTask();
      }
    }

  private:
    std::queue<Task> m_worker_tasks;
    TaskSystemParallelThreadPoolSleeping *m_tasksys;
    int m_index;
    int m_local_run_num {0};
  };
};

#endif
