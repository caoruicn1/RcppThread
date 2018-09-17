// Copyright © 2018 Thomas Nagler
//
// This file is part of the RcppThread and licensed under the terms of
// the MIT license. For a copy, see the LICENSE.md file in the root directory of
// RcppThread or https://github.com/tnagler/RcppThread/blob/master/LICENSE.md.

#pragma once

#include "RcppThread/Thread.hpp"
#include "RcppThread/TaskQueue.hpp"

#include <vector>
#include <condition_variable>
#include <memory>
#include <cmath>
#include <cstddef>
#include <future>
#include <queue>


#include <iostream>


namespace RcppThread {


struct Batch {
    ptrdiff_t begin;
    ptrdiff_t size;
};

inline std::vector<Batch> createBatches(ptrdiff_t begin,
    size_t nTasks,
    size_t nThreads)
    {
        nThreads = std::max(nThreads, static_cast<size_t>(1));
        std::vector<Batch> batches(std::min(nTasks, nThreads));
        ptrdiff_t min_size = nTasks / nThreads;
        ptrdiff_t rem_size = nTasks % nThreads;
        for (ptrdiff_t i = 0, k = 0; i < static_cast<ptrdiff_t>(nTasks); k++) {
            batches[k] = Batch{begin + i, min_size + (rem_size-- > 0)};
            i += batches[k].size;
        }

        return batches;
    }

//! Implemenation of the thread pool pattern based on `Thread`.
class ThreadPool {
public:
    friend class ParallelFor;

    ThreadPool() = default;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool(const ThreadPool&) = delete;

    //! constructs a thread pool with `nThreads` threads.
    //! @param nThreads number of threads to create; if `nThreads = 0`, all
    //!    work pushed to the pool will be done in the main thread.
    ThreadPool(size_t nThreads) : 
        queues_{nThreads}, num_busy_(0), stopped_(false)
    {
        for (size_t i = 0; i < nThreads; i++) {
            workers_.emplace_back([this, i, nThreads] {
                while (!stopped_ || !this->global_queue_.empty()) {
                    std::function<void()> global_job;
                    {
                        std::unique_lock<std::mutex> lk(m_);
                        // wait for new job or stop signal
                        cv_tasks_.wait(lk, [this] {
                            return stopped_ || !this->global_queue_.empty();
                        });
                        // check if there are any jobs left in the queue
                        if (this->global_queue_.empty())
                            continue;
                        global_job = std::move(this->global_queue_.front());
                        global_queue_.pop();
                    }
                    num_busy_++;
                    cv_busy_.notify_one();
                    // work on the global job
                    global_job();
                    
                    // work on local jobs
                    while (true) {
                        std::function<void()> job;
                        if (queues_[i].pop(job)) {
                            job();
                            continue;
                        }
                        for (size_t k = 1; k < nThreads; k++) {
                            if (queues_[(i + k) % nThreads].steal(job))
                                break;
                        }
                        if (!job)
                            break;
                        job();
                    }
                    
                    // done working, notify in case someone's wait()ing
                    num_busy_--;
                    cv_busy_.notify_one();
                }
            });
        }
    }

    ~ThreadPool()
    {
        join();
    }

    size_t size() const
    {
        return workers_.size();
    }

    // assignment operators
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool& operator=(ThreadPool&& other) = default;

    //! pushes new jobs to the thread pool.
    //! @param f a function taking an arbitrary number of arguments.
    //! @param args a comma-seperated list of the other arguments that shall
    //!   be passed to `f`.
    //! @return an `std::shared_future`, where the user can get the result and
    //!   rethrow the catched exceptions.
    template<class F, class... Args>
    auto push(F&& f, Args&&... args) -> std::future<decltype(f(args...))>
    {    
        // create pacakged task on the heap to avoid stack overlows.
        auto job = std::make_shared<std::packaged_task<decltype(f(args...))()>>(
            [&f, args...] { return f(args...); }
        );
        
        // if there are no workers, just do the job in the main thread
        if (workers_.size() == 0) {
            (*job)();
            return job->get_future();
        }
        
        // add job to the global queue
        {
            std::unique_lock<std::mutex> lk(m_);
            global_queue_.emplace([job] () { (*job)();});
        }
        
        // signal a waiting worker that there's a new job in the global queue
        cv_tasks_.notify_one();
        
        // return future result of the job
        return job->get_future();
    }

    //! maps a function on a list of items, possibly running tasks in parallel.
    //! @param f function to be mapped.
    //! @param items an objects containing the items on which `f` shall be mapped;
    //!     must allow for `auto` loops (i.e., `std::begin(I)`/`std::end(I)` must be
    //!     defined).
    template<class F, class I>
    void map(F&& f, I &&items)
    {
        for (auto &&item : items)
            this->push(f, item);
    }

    //! computes an index-based for loop in parallel batches.
    //! @param begin first index of the loop.
    //! @param size the loop runs in the range `[begin, begin + size)`.
    //! @param f a function (the 'loop body').
    //! @details Consider the following code:
    //! ```
    //! std::vector<double> x(10);
    //! for (size_t i = 0; i < x.size(); i++) {
    //!     x[i] = i;
    //! }
    //! ```
    //! The parallel equivalent is given by:
    //! ```
    //! ThreadPool pool(2);
    //! pool.forIndex(0, 10, [&] (size_t i) {
    //!     x[i] = i;
    //! });
    //! ```
    //! **Caution**: if the iterations are not independent from another,
    //! the tasks need to be synchonized manually using mutexes.
    template<class F>
    inline void forIndex(ptrdiff_t begin, size_t size, F&& f)
    {
        static std::function<void(const Batch&)> doBatch;
        doBatch = [&] (const Batch& b) {
            if (b.size > 10) {
                Batch b1{b.begin, b.size / 2};
                Batch b2{b.begin + b.size / 2, (b.size - b.size / 2)};
                doBatch(b1);
                doBatch(b2);
            } else {
                queues_[findThreadQueue()].push([b, &f] {
                    ptrdiff_t i = b.begin;
                    while (i < static_cast<ptrdiff_t>(b.begin + b.size))
                        f(i++);
                });
            }
        };
        auto batches = createBatches(begin, size, workers_.size());
        for (const auto batch : batches)    
            this->push(doBatch, batch);
    }

    //! computes a range-based for loop in parallel batches.
    //! @param items an object allowing for `items.size()` and whose elements
    //!   are accessed by the `[]` operator.
    //! @param f a function (the 'loop body').
    //! @details Consider the following code:
    //! ```
    //! std::vector<double> x(10, 1.0);
    //! for (auto& xx : x) {
    //!     xx *= 2;
    //! }
    //! ```
    //! The parallel `ThreadPool` equivalent is
    //! ```
    //! ThreadPool pool(2);
    //! pool.forEach(x, [&] (double& xx) {
    //!     xx *= 2;
    //! });
    //! ```
    //! **Caution**: if the iterations are not independent from another,
    //! the tasks need to be synchonized manually using mutexes.
    template<class F, class I>
    inline void forEach(I&& items, F&& f)
    {
        static std::function<void(const Batch&)> doBatch;
        doBatch = [&] (const Batch& b) {
            if (b.size > 10) {
                Batch b1{b.begin, b.size / 2};
                Batch b2{b.begin + b.size / 2, (b.size - b.size / 2)};
                doBatch(b1);
                doBatch(b2);
            } else {
                queues_[findThreadQueue()].push([b, &items, &f] {
                    ptrdiff_t i = b.begin;
                    while (i < static_cast<ptrdiff_t>(b.begin + b.size))
                        f(items[i++]);
                });
            }
        };
        auto batches = createBatches(0, items.size(), workers_.size());
        this->map(doBatch, batches);
    }

    //! waits for all jobs to finish, but does not join the threads.
    void wait()
    {
        std::unique_lock<std::mutex> lk(m_);
        cv_busy_.wait(lk, [this] {
            return (num_busy_ == 0) && global_queue_.empty();
        });
    }

    //! waits for all jobs to finish and joins all threads.
    void join()
    {
        {
            std::unique_lock<std::mutex> lk(m_);
            stopped_ = true;
            cv_tasks_.notify_all();
        }
        // join threads if not done already
        if (workers_.size() > 0) {
            if (workers_[0].joinable()) {
                for (auto &worker : workers_) {
                    worker.join();
                }
            }
        }
    }
    
private:
    size_t findThreadQueue() const
    {
        size_t i = 0;
        while (std::this_thread::get_id() != workers_[i].get_id()) 
            i++;
        return i;
    }
    
    std::vector<Thread> workers_;  // worker threads in the pool
    std::vector<TaskQueue> queues_;     // the task queues
    std::queue<std::function<void()>> global_queue_;

    // variables for synchronization between workers
    std::mutex m_;
    std::condition_variable cv_tasks_;
    std::condition_variable cv_busy_;
    std::atomic_uint num_busy_;
    std::atomic_bool stopped_;
};

}
