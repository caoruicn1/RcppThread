#include <chrono>
#include <random>
#include <Rcpp.h>
#define RCPP_PARALLEL_USE_TBB 0
// [[Rcpp::depends(RcppParallel)]]
#include <RcppParallel.h>
#include "RcppThread.h"
using namespace RcppParallel;
using namespace Rcpp;

struct SquareRoot : public Worker
{
    // source matrix
    const RVector<double> input;

    // destination matrix
    RVector<double> output;

    // initialize with source and destination
    SquareRoot(const NumericVector input, NumericVector output)
        : input(input), output(output) {}

    // take the square root of the range of elements requested
    void operator()(std::size_t begin, std::size_t end) {
        for (size_t i = begin; i < end; i++) {
            std::vector<size_t> v(1000, 1);
            output[i] = std::accumulate(v.begin(), v.end(), 0);
        }
    }
};

// [[Rcpp::export]]
NumericVector parallelMatrixSqrt(NumericVector x) {
    NumericVector output(x.length());
    SquareRoot squareRoot(x, output);
    parallelFor(0, x.length(), squareRoot);
    return output;
}


// [[Rcpp::export]]
NumericVector parallelMatrixSqrt2(NumericVector x) {
    using namespace RcppThread;
    parallelFor(0, static_cast<size_t>(x.length()), [&] (size_t i) {
        std::vector<size_t> v(1000, 1);
        x[i] = std::accumulate(v.begin(), v.end(), 0);
    });
    return x;
}

// --------------------------------------------------------

struct Sleeper : public Worker
{
    Sleeper( std::default_random_engine e1,
        std::uniform_int_distribution<int> uniform_dist) : e1_(e1), uniform_dist_(uniform_dist) {}

    void operator()(std::size_t begin, std::size_t end) {
        for (size_t i = begin; i < end; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(uniform_dist_(e1_)));
        }
    }

    std::default_random_engine e1_;
    std::uniform_int_distribution<int> uniform_dist_;
};

// [[Rcpp::export]]
void sleep(size_t times) {
    // Seed with a real random value, if available
    std::random_device r;

    // Choose a random mean between 1 and 6
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(1, 10);

    Sleeper sleeper(e1, uniform_dist);
    parallelFor(0, times, sleeper);
}

// [[Rcpp::export]]
void sleep2(size_t times) {
    // Seed with a real random value, if available
    std::random_device r;

    // Choose a random mean between 1 and 6
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(1, 10);

    RcppThread::parallelFor(0, times, [&] (size_t i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(uniform_dist(e1)));
    });
}
