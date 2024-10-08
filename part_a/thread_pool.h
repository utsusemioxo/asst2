#pragma once

// #define ENABLE_LOG
#include <condition_variable>
#include <functional>
#include <future>
// #ifdef ENABLE_LOG
#include <iostream>
// #endif
#include <mutex>
#include <queue>
#include <thread>
#include <vector>


class ThreadPool {
public:
  explicit ThreadPool(size_t num_threads) : m_stop(false) {
    // std::cout << "ThreadPool construct begin size = " << num_threads << "\n";
    // Initialize work threads
    for (size_t i = 0; i < num_threads; ++i) {
      m_workers.emplace_back(WorkerThread(this));
    }
    // std::cout << "ThreadPool construct end" << "\n";
  }

  ~ThreadPool() {
    // std::cout << "Thread pool destruct begin\n";
    Shutdown();
    // std::cout << "Thread pool destruct end\n";
  }

  // Shutdown thread pool
  void Shutdown() {
    // std::cout << "shutdown begin\n";
    {
      std::lock_guard<std::mutex> lock(m_queue_mtx);
      m_stop = true;
      
    }

    m_cv.notify_all(); // wake up all threads to finish remaining threads
#ifdef ENABLE_LOG
      std::cout << "notify_all(), size of queue=" << m_tasks.size() << std::endl;
#endif

    for (auto &thread : m_workers) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    // std::cout << "shutdown end\n";
  }

  template <typename F, typename... Args>
  auto AddTask(F &&f, Args &&...args) -> std::future<decltype(f(args...))> {
    // create a function with bounded parameters ready to execute
#ifdef ENABLE_LOG
      std::cout << "add task 1" << std::endl;
#endif
    auto func = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

    // Encapsulate it into a shared ptr in order to be able to copy construct /
    // assign
    auto task_ptr =
        std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);

#ifdef ENABLE_LOG
      std::cout << "add task 2" << std::endl;
#endif
    // wrap the task pointer into a valid lambda
    auto wrapper_func = [task_ptr]() { (*task_ptr)(); };

    {
      std::lock_guard<std::mutex> lock(m_queue_mtx);
      // if (!m_stop) {
      m_tasks.push(wrapper_func);
      // wake up one thread if it's waiting
      m_cv.notify_one();
#ifdef ENABLE_LOG
      std::cout << "notify_one(), size of queue="<< m_tasks.size() << std::endl;
#endif
      // }
    }

#ifdef ENABLE_LOG
      std::cout << "add task 3" << std::endl;
#endif
    // return future from promise
    return task_ptr->get_future();
  }

private:
  std::mutex m_queue_mtx;
  std::condition_variable m_cv;

  std::vector<std::thread> m_workers;
  bool m_stop;

  std::queue<std::function<void()>> m_tasks;

  class WorkerThread {
  public:
    explicit WorkerThread(ThreadPool *pool) : thread_pool(pool) {}

    void operator()() {
      while (!thread_pool->m_stop ||
             (thread_pool->m_stop && !thread_pool->m_tasks.empty())) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(thread_pool->m_queue_mtx);
#ifdef ENABLE_LOG
          std::cout << "Thread[" << std::this_thread::get_id()
                    << "] waiting...()" << std::endl;
#endif
          this->thread_pool->m_cv.wait(lock, [this] {
            return this->thread_pool->m_stop ||
                   !this->thread_pool->m_tasks.empty();
          });
#ifdef ENABLE_LOG
          std::cout << "Thread[" << std::this_thread::get_id() << "] awake()"
                    << std::endl;
#endif
          if (this->thread_pool->m_stop /*&& this->thread_pool->m_tasks.empty()*/) {
            return;
          }
          task = std::move(this->thread_pool->m_tasks.front());
          thread_pool->m_tasks.pop();
        }

#ifdef ENABLE_LOG
        std::cout << "Thread[" << std::this_thread::get_id()
                  << "] executing task begin" << std::endl;
#endif
        task();
#ifdef ENABLE_LOG
        std::cout << "Thread[" << std::this_thread::get_id()
                  << "] executing task end" << std::endl;
#endif
      }
    }

  private:
    ThreadPool *thread_pool;
  };
};
