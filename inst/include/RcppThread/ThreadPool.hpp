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

namespace RcppThread {


struct Batch {
    ptrdiff_t begin;
    size_t size;
};


inline size_t computeBatchSize(size_t nTasks, size_t nThreads)
{
    if (nTasks < nThreads)
    return nTasks;
    return nThreads * (1 + std::floor(std::log(nTasks / nThreads)));
}

inline std::vector<Batch> createBatches(ptrdiff_t begin,
    size_t nTasks,
    size_t nThreads)
    {
        nThreads = std::max(nThreads, static_cast<size_t>(1));
        std::vector<Batch> batches(computeBatchSize(nTasks, nThreads));
        size_t min_size = nTasks / nThreads;
        ptrdiff_t rem_size = nTasks % nThreads;
        for (ptrdiff_t i = 0, k = 0; i < nTasks; k++) {
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
        qi_(0), num_busy_(0), stopped_(false), jobs_{nThreads}
    {
        size_t i = 0;
        for (size_t i = 0; i < nThreads; i++) {
            pool_.emplace_back([this, i, nThreads] {
                while (!stopped_ | !jobs_[i].empty()) {
                    while (true) {
                        std::function<void()> f;
                        for (size_t k = 0; k < nThreads; k++) {
                            if (jobs_[(i + k) % nThreads].tryPop(f))
                                break;
                        }
                        
                        if (!f && !jobs_[0].pop(f))
                            break;
                        f();
                    }
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
        return pool_.size();
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
        if (pool_.size() == 0) {
            (*job)();
            return job->get_future();
        }
        
        size_t i = qi_++;
        size_t nThreads = jobs_.size();
        bool pushed = false;
        for (size_t k = 0; k < nThreads; k++) {
            pushed = jobs_[(i + k) % nThreads].tryPush([job] {(*job)();});
            if (pushed)
                break;
        }
        if (!pushed)
            jobs_[i % jobs_.size()].push([job] {(*job)();});
        
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
        auto doBatch = [&] (const Batch& batch) {
            for (ptrdiff_t i = batch.begin; i < batch.begin + batch.size; i++)
            f(i);
        };
        this->map(doBatch, createBatches(begin, size, pool_.size()));
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
        auto doBatch = [&items, &f] (const Batch& batch) {
            for (size_t i = batch.begin; i < batch.begin + batch.size; i++)
            f(items[i]);
        };
        this->map(doBatch, createBatches(0, items.size(), pool_.size()));
    }

    //! waits for all jobs to finish, but does not join the threads.
    void wait()
    {
        std::unique_lock<std::mutex> lk(m_);
        cv_busy_.wait(lk, [&] {return (num_busy_ == 0) && jobs_.empty();});
    }

    //! waits for all jobs to finish and joins all threads.
    void join()
    {
        stopped_ = true;
        // join threads if not done already
        if (pool_.size() > 0) {
            if (pool_[0].joinable()) {
                for (auto &worker : pool_)
                worker.join();
            }
        }
    }


private:
    std::vector<Thread> pool_;                // worker threads in the pool
    std::vector<TaskQueue> jobs_;             // the task queues

    // variables for synchronization between workers
    std::mutex m_;
    std::vector<std::condition_variable> cv_tasks_;
    std::condition_variable cv_busy_;
    std::atomic_uint num_busy_;
    std::atomic_bool stopped_;
    size_t qi_;
};

}
