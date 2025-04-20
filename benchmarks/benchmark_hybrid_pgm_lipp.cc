#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
  if (!pareto) {
    // If not in pareto mode, use default parameters
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5}); // 5% threshold
  } else {
    // Test different migration thresholds
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{1});  // 1% threshold
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{5});  // 5% threshold
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{10}); // 10% threshold
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>(std::vector<int>{20}); // 20% threshold
  }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, const std::string& filename) {
  // Use similar configurations as Dynamic PGM but with hybrid-specific optimizations
  if (filename.find("books_100M") != std::string::npos) {
    if (filename.find("0.000000i") != std::string::npos) {
      // Lookup-heavy workload
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
      benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
    } else if (filename.find("mix") == std::string::npos) {
      if (filename.find("0m") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{10});
      } else if (filename.find("1m") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 16>>(std::vector<int>{10});
      }
    } else {
      // Mixed workloads
      if (filename.find("0.900000i") != std::string::npos) {
        // 90% lookup, 10% insert
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
      } else if (filename.find("0.100000i") != std::string::npos) {
        // 10% lookup, 90% insert
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{10});
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{10});
      }
    }
  }
  
  if (filename.find("fb_100M") != std::string::npos) {
    if (filename.find("0.000000i") != std::string::npos) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
    } else if (filename.find("mix") == std::string::npos) {
      if (filename.find("0m") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
      }
    } else {
      if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
      } else if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{10});
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{10});
      }
    }
  }
  
  if (filename.find("osmc_100M") != std::string::npos) {
    if (filename.find("0.000000i") != std::string::npos) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
      benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
    } else if (filename.find("mix") == std::string::npos) {
      if (filename.find("0m") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
      }
    } else {
      if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{5});
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{5});
      } else if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(std::vector<int>{10});
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>(std::vector<int>{10});
      }
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t); 