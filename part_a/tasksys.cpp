#include "tasksys.h"
#include "itasksys.h"
#include <cmath>
#include <mutex>
#include <thread>
#include <vector>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  m_num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  //    for (int i = 0; i < num_total_tasks; i++) {
  //        runnable->runTask(i, num_total_tasks);
  //    }

  std::vector<std::thread> threads;
  // int task_per_thread = std::ceil(static_cast<float>(num_total_tasks) /
  // m_num_threads); threads.reserve(m_num_threads); for (int i = 0; i <
  // m_num_threads; i++) {
  //   threads.emplace_back([this, runnable, i, num_total_tasks,
  //   task_per_thread] {
  //     for (int j = 0; j < task_per_thread; j++) {
  //       if (i + j * m_num_threads < num_total_tasks) {
  //         // runnable->runTask(i * task_per_thread + j, num_total_tasks);
  //         runnable->runTask(i + j * m_num_threads, num_total_tasks);
  //       } else {continue;}
  //     }
  //   });
  // }

  threads.reserve(m_num_threads);
  {
    std::unique_lock<std::mutex> lock(m_task_mtx);
    m_task_id = 0;
  }
  for (int i = 0; i < m_num_threads; i++) {
    threads.emplace_back([this, runnable, num_total_tasks]() {
      auto current_task_id = 0;
      while (true) {
        std::unique_lock<std::mutex> lock(m_task_mtx);
        if (m_task_id >= num_total_tasks)
          break;
        current_task_id = m_task_id;
        m_task_id++;
        lock.unlock();
        runnable->runTask(current_task_id, num_total_tasks);
      }
    });
  }

  for (auto &thread : threads) {
    if (thread.joinable())
      thread.join();
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads), m_num_threads(num_threads),
      m_local_mtx(m_num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //

  // m_thread_pool = std::unique_ptr<ThreadPool>(new ThreadPool(num_threads));
  // InitPool(num_threads);
  {
    m_task_finish_cnt = 0;
  }
  m_local_task_queue.reserve(m_num_threads);
  for (int i = 0; i < m_num_threads; i++) {
    m_local_task_queue.emplace_back(std::queue<Task>());
  }
  InitPool(m_num_threads);

  // for (int i = 0; i < m_num_threads; i++) {
  //   m_thread_task_queue_mtx.emplace_back(std::make_unique<std::mutex>());
  // }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  // m_thread_pool->Shutdown();
  Shutdown();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // std::cout << ++count << std::endl;
  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  // for (int i = 0; i < num_total_tasks; i++) {
  //     runnable->runTask(i, num_total_tasks);
  // }

  // InitPool(m_num_threads);
  {
    m_task_finish_cnt = 0;
    m_num_total_tasks = num_total_tasks;
  }

  for (int i = 0; i < num_total_tasks; i++) {
    Task task = [runnable, i, num_total_tasks]() {
      // std::cout << "i=" << i << ", total=" << num_total_tasks << "\n";
      runnable->runTask(i, num_total_tasks);
    };
    AddTask(task, i);
  }

  {
    // m_finish_cond.wait(lock, [this]() {return m_task_queue.empty() /*&&
    // m_task_finish_cnt == m_task_start_cnt*/;});
    for (;;) {
      if (m_task_finish_cnt == m_num_total_tasks) {
        break;
      }
    }
    // std::cout << "finished!\n";
    // std::cout << ++count_finish_spinning << "\n";
  }
  // Shutdown();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

void TaskSystemParallelThreadPoolSpinning::InitPool(int num_threads) {
  // std::cout << "init pool begin\n";
  { m_shutdown = false; }
  {
    for (int i = 0; i < num_threads; i++) {
      m_thread_pool.emplace_back(WorkerThread(this, i));
    }
  }
  // std::cout << "init pool end\n";
}

void TaskSystemParallelThreadPoolSpinning::Shutdown() {
  // std::cout << "shutdown begin\n";
  { m_shutdown = true; }

  for (auto &thread : m_thread_pool) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  {
    m_thread_pool.clear();
    // m_thread_task_queue.clear();
  }
  // std::cout << "shutdown end\n";
}

void TaskSystemParallelThreadPoolSpinning::AddTask(Task &task, int task_id) {
  // std::cout << "AddTask:" << m_num_threads << " " <<  m_roundRobin << " " <<
  // m_roundRobin % m_num_threads << std::endl;
  std::unique_lock<std::mutex> ul(
      m_local_mtx.at(task_id % m_num_threads));
  m_local_task_queue.at(task_id % m_num_threads).push(std::move(task));
  // m_task_cond.notify_one();
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), m_num_threads(num_threads),
      m_local_mtx(m_num_threads), m_local_cond(m_num_threads),
      m_local_task_queue(m_num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  m_num_total_tasks = 0;
  m_task_finish_cnt = 0;
  for (int i = 0; i < m_num_threads; i++) {
    m_thread_pool.emplace_back(WorkerThread(this, i));
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  // std::cout << "~TaskSystemParallelThreadPoolSleeping+\n";
  m_shutdown = true;

  // for (auto &cond : m_local_cond) {
  //   cond.notify_all();
  // }
  for (int i = 0; i < m_num_threads; i++) {
    m_local_cond.at(i).notify_all();
  }

  for (auto &thread : m_thread_pool) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // std::cout << "~TaskSystemParallelThreadPoolSleeping-\n";
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  // for (int i = 0; i < num_total_tasks; i++) {
  //   runnable->runTask(i, num_total_tasks);
  // }

  // InitPool(m_num_threads);
  m_task_finish_cnt = 0;
  m_num_total_tasks = num_total_tasks;

  for (int i = 0; i < num_total_tasks; i++) {
    Task task = [runnable, i, num_total_tasks]() {
      // std::cout << "i=" << i << ", total=" << num_total_tasks << "\n";
      runnable->runTask(i, num_total_tasks);
    };
    AddTask(task, i);
  }

  m_add_done = true;

  {
      // std::this_thread::yield();
      std::unique_lock<std::mutex> ul(m_task_mtx);
      m_finish_cond.wait(ul, [this](){return m_task_finish_cnt == m_num_total_tasks;});
    // while (m_task_finish_cnt != m_num_total_tasks) {
    //   std::this_thread::yield();
    // }
  }

    // for (;;) {
    //   if (m_task_finish_cnt == m_num_total_tasks) {
    //     break;
    //   }
    // }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}

void TaskSystemParallelThreadPoolSleeping::AddTask(Task &task, int task_id) {
  std::unique_lock<std::mutex> ul(
      m_local_mtx.at(task_id % m_num_threads));
  m_local_task_queue.at(task_id % m_num_threads).push_back(std::move(task));
  ul.unlock();
  m_local_cond.at(task_id % m_num_threads).notify_one();
}