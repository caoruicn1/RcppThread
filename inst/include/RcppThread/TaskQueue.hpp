// Copyright © 2018 Thomas Nagler
//
// This file is part of the RcppThread and licensed under the terms of
// the MIT license. For a copy, see the LICENSE.md file in the root directory of
// RcppThread or https://github.com/tnagler/RcppThread/blob/master/LICENSE.md.

#pragma once

#include "RcppThread/Thread.hpp"

#include <vector>
#include <deque>
#include <condition_variable>
#include <memory>

class TaskQueue {
public:
    ~TaskQueue()
    {
        std::unique_lock<std::mutex> lk(m_);
    }

public:
    template<class F>
    bool pop(F& f) {
        std::unique_lock<std::mutex> lk{m_};
        if (jobs_.empty())
            return false;
        f = std::move(jobs_.front());
        jobs_.pop_front();
        return true;
    }

    template<class F>
    void push(F&& f)
    {
        std::unique_lock<std::mutex> lk{m_};
        jobs_.emplace_back(std::forward<F>(f));
    }

    template<class F>
    bool tryPop(F& f) {
        if (m_.try_lock()) {
            if (!jobs_.empty()) {
                f = std::move(jobs_.front());
                jobs_.pop_front();
                m_.unlock();
                return true;
            }
            m_.unlock();
        }
        return false;
    }

    template<typename F>
    bool tryPush(F&& f) {
        if (m_.try_lock()) {
            jobs_.emplace_back(std::forward<F>(f));
            m_.unlock();
            return true;
        }
        return false;
    }

    bool empty() const
    {
        return jobs_.empty();
    }

private:
    std::deque<std::function<void()>> jobs_;
    std::mutex m_;
};
