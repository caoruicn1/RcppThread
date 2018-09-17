// Copyright © 2018 Thomas Nagler
//
// This file is part of the RcppThread and licensed under the terms of
// the MIT license. For a copy, see the LICENSE.md file in the root directory of
// RcppThread or https://github.com/tnagler/RcppThread/blob/master/LICENSE.md.

#pragma once

#include <vector>
#include <atomic>
#include <memory>

class TaskQueue {
public:
    TaskQueue() : top_{0}, bottom_{0}, jobs_{1024}
    {}
    
    template<class F>
    bool pop(F& f) {
        size_t bottom = bottom_.load(std::memory_order_acquire);
        if (bottom > 0)
            bottom--;
        bottom_.store(bottom, std::memory_order_release);
        size_t top = top_.load(std::memory_order_acquire);
        if (top <= bottom) {
            if (top != bottom) {
                f = std::move(jobs_[bottom]);
                return true;
            } 
            f = std::move(jobs_[bottom]);
                
            if (!top_.compare_exchange_weak(top, top + 1,
                std::memory_order_acq_rel)) {
                // Someone already took the last item, abort
                bottom_.store(top + 1, std::memory_order_release);
                return false;
            }
            
            bottom_.store(top + 1, std::memory_order_release);
            return true;
            
        } else {
            bottom_.store(top, std::memory_order_release);
            return false;
        }

        return true;
    }

    template<class F>
    bool push(F&& f)
    {
        size_t bottom = bottom_.load(std::memory_order_acquire);
        if (bottom >= jobs_.size())
            return false;
        jobs_[bottom] = std::move(f);
        bottom_.store(bottom + 1, std::memory_order_release);
        return true;
    }
    
    // called by stealing thread, not owner thread
    template<class F>
    bool steal(F& f)
    {
        size_t top = top_.load(std::memory_order_acquire);
        
        // Release-Acquire ordering, so the stealing thread see new job
        size_t bottom = bottom_.load(std::memory_order_acquire);
        
        if (bottom > top) {
            // check if other stealing thread stealing this work 
            // or owner thread pop this job.
            // no data should do sync, so use relaxed oreder
            if (top_.compare_exchange_weak(top, top + 1, std::memory_order_acq_rel)) {
                f = std::move(jobs_[top]);
                return true;
            }
        }
        return false;
    }

    bool empty() const
    {
        return (top_ == bottom_);
    }

private:
    std::atomic<size_t> top_;
    std::atomic<size_t> bottom_;
    std::vector<std::function<void()>> jobs_;
};
