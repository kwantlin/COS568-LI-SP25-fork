#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>
#include <limits>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
 public:
  HybridPGMLIPP(const std::vector<int>& params) 
      : dpgm_(params), lipp_(params), migration_threshold_(0.05), 
        migration_in_progress_(false), migration_queue_size_(0) {
    if (!params.empty()) {
      migration_threshold_ = params[0] / 100.0; // Convert percentage to decimal
    }
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Initially load all data into LIPP
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // First check DPGM
    size_t dpgm_result = dpgm_.EqualityLookup(lookup_key, thread_id);
    if (dpgm_result != util::NOT_FOUND) {
      return dpgm_result;
    }
    
    // If not found in DPGM, check LIPP
    return lipp_.EqualityLookup(lookup_key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    // Combine results from both indexes
    uint64_t dpgm_result = dpgm_.RangeQuery(lower_key, upper_key, thread_id);
    uint64_t lipp_result = lipp_.RangeQuery(lower_key, upper_key, thread_id);
    return dpgm_result + lipp_result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Insert into DPGM
    dpgm_.Insert(data, thread_id);
    
    // Check if we need to migrate data from DPGM to LIPP
    if (dpgm_.size() > migration_threshold_ * (dpgm_.size() + lipp_.size())) {
      // Start asynchronous migration if not already in progress
      if (!migration_in_progress_.load()) {
        StartAsyncMigration();
      }
    }
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { return dpgm_.size() + lipp_.size(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
    return unique && !multithread;
  }

  std::vector<std::string> variants() const { 
    std::vector<std::string> vec;
    vec.push_back(SearchClass::name());
    vec.push_back(std::to_string(pgm_error));
    vec.push_back(std::to_string(static_cast<int>(migration_threshold_ * 100)));
    return vec;
  }

 private:
  void StartAsyncMigration() {
    migration_in_progress_.store(true);
    std::thread migration_thread([this]() {
      MigrateDPGMToLIPP();
      migration_in_progress_.store(false);
    });
    migration_thread.detach();
  }

  void MigrateDPGMToLIPP() {
    // Extract all data from DPGM
    std::vector<KeyValue<KeyType>> dpgm_data;
    ExtractDPGMData(dpgm_data);
    
    // Sort the data for efficient bulk loading
    std::sort(dpgm_data.begin(), dpgm_data.end(), 
              [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                return a.key < b.key;
              });
    
    // Bulk load into LIPP
    {
      std::lock_guard<std::mutex> lock(mutex_);
      lipp_.Build(dpgm_data, 1); // Use single thread for migration
      
      // Clear DPGM by creating a new empty instance
      dpgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
    }
  }

  void ExtractDPGMData(std::vector<KeyValue<KeyType>>& output) {
    // Use RangeQuery to get all keys in DPGM
    KeyType min_key = std::numeric_limits<KeyType>::min();
    KeyType max_key = std::numeric_limits<KeyType>::max();
    
    // Get all keys in DPGM using RangeQuery
    uint64_t count = dpgm_.RangeQuery(min_key, max_key, 0);
    
    // If we have any keys, we need to find them
    if (count > 0) {
      // Use binary search to find all keys
      KeyType current = min_key;
      while (current <= max_key) {
        size_t value = dpgm_.EqualityLookup(current, 0);
        if (value != util::NOT_FOUND) {
          output.emplace_back(KeyValue<KeyType>{current, value});
        }
        // Increment current key
        if (current == max_key) break;
        current++;
      }
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
  Lipp<KeyType> lipp_;
  double migration_threshold_;
  std::mutex mutex_;
  std::atomic<bool> migration_in_progress_;
  std::atomic<size_t> migration_queue_size_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H 