// Copyright © 2018 Thomas Nagler
//
// This file is part of the RcppThread and licensed under the terms of
// the MIT license. For a copy, see the LICENSE.md file in the root directory of
// RcppThread or https://github.com/tnagler/RcppThread/blob/master/LICENSE.md.

#pragma once

#include "RcppThread/ThreadPool.hpp"

namespace RcppThread {

//! computes an index-based for loop in parallel batches.
//! @param begin first index of the loop.
//! @param size the loop runs in the range `[begin, begin + size)`.
//! @param f a function (the 'loop body').
//! @param nThreads the number of threads to use; the default uses the number
//!   of cores in the machine;  if `nThreads = 0`, all work will be done in the
//!   main thread.
//! @details Consider the following code:
//! ```
//! std::vector<double> x(10);
//! for (size_t i = 0; i < x.size(); i++) {
//!     x[i] = i;
//! }
//! ```
//! The parallel equivalent is given by:
//! ```
//! parallelFor(0, 10, [&] (size_t i) {
//!     x[i] = i;
//! });
//! ```
//! The function sets up a `ThreadPool` object to do the scheduling. If you
//! want to run multiple parallel for loops, consider creating a `ThreadPool`
//! yourself and using `ThreadPool::forEach()`.
//!
//! **Caution**: if the iterations are not independent from another,
//! the tasks need to be synchonized manually using mutexes.
template<class F>
inline void parallelFor(ptrdiff_t begin, size_t size, F&& f,
                        size_t nThreads = std::thread::hardware_concurrency())
{
    ThreadPool pool(nThreads);
    pool.forIndex(begin, size, f);
    pool.join();
}

//! computes a range-based for loop in parallel batches.
//! @param items an object allowing for `items.size()` and whose elements
//!   are accessed by the `[]` operator.
//! @param f a function (the 'loop body').
//! @param nThreads the number of threads to use; the default uses the number
//!   of cores in the machine;  if `nThreads = 0`, all work will be done in the
//!   main thread.
//! @details Consider the following code:
//! ```
//! std::vector<double> x(10, 1.0);
//! for (auto& xx : x) {
//!     xx *= 2;
//! }
//! ```
//! The parallel `ThreadPool` equivalent is
//! ```
//! parallelFor(x, [&] (double& xx) {
//!     xx *= 2;
//! });
//! ```
//! The function sets up a `ThreadPool` object to do the scheduling. If you
//! want to run multiple parallel for loops, consider creating a `ThreadPool`
//! yourself and using `ThreadPool::forEach()`.
//!
//! **Caution**: if the iterations are not independent from another,
//! the tasks need to be synchonized manually using mutexes.
template<class I, class F>
inline void parallelFor(I&& items, F&& f,
                        size_t nThreads = std::thread::hardware_concurrency())
{
    ThreadPool pool(nThreads);
    pool.forEach(items, f);
    pool.join();
}


}