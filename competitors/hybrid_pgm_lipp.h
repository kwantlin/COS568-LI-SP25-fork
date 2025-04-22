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
#include <chrono>
#include <condition_variable>
#include <future>
#include <shared_mutex>
#include <unordered_map>
#include <list>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Base<KeyType> {
 public:
  HybridPGMLIPP(const std::vector<int>& params) 
      : dpgm_(params), lipp_(params), migration_threshold_(0.05), 
        migration_in_progress_(false), migration_queue_size_(0),
        batch_size_(10000), stop_migration_(false),
        dpgm_min_key_(std::numeric_limits<KeyType>::max()),
        dpgm_max_key_(std::numeric_limits<KeyType>::min()),
        lipp_min_key_(std::numeric_limits<KeyType>::max()),
        lipp_max_key_(std::numeric_limits<KeyType>::min()),
        dpgm_hits_(0), lipp_hits_(0), total_queries_(0),
        last_adaptation_time_(std::chrono::steady_clock::now()),
        cache_size_(1000000), // 1M entries cache
        parallel_migration_batch_size_(100000) { // 100K entries per parallel batch
    if (!params.empty()) {
      migration_threshold_ = params[0] / 100.0;
    }
    // Initialize migration thread pool
    for (size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
      migration_threads_.emplace_back(&HybridPGMLIPP::MigrationWorker, this);
    }
  }

  ~HybridPGMLIPP() {
    {
      std::lock_guard<std::shared_mutex> lock(migration_mutex_);
      stop_migration_ = true;
    }
    migration_cv_.notify_all();
    for (auto& thread : migration_threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Initially load all data into LIPP
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // Check cache first
    {
      std::shared_lock<std::shared_mutex> cache_lock(cache_mutex_);
      auto cache_it = cache_.find(lookup_key);
      if (cache_it != cache_.end()) {
        // Update LRU
        cache_lru_.splice(cache_lru_.begin(), cache_lru_, cache_it->second.second);
        return cache_it->second.first;
      }
    }

    // Smart routing based on key ranges
    if (lookup_key >= dpgm_min_key_ && lookup_key <= dpgm_max_key_) {
      size_t dpgm_result = dpgm_.EqualityLookup(lookup_key, thread_id);
      if (dpgm_result != util::NOT_FOUND) {
        UpdateCache(lookup_key, dpgm_result);
        dpgm_hits_++;
        total_queries_++;
        return dpgm_result;
      }
    }
    
    if (lookup_key >= lipp_min_key_ && lookup_key <= lipp_max_key_) {
      size_t lipp_result = lipp_.EqualityLookup(lookup_key, thread_id);
      if (lipp_result != util::NOT_FOUND) {
        UpdateCache(lookup_key, lipp_result);
        lipp_hits_++;
        total_queries_++;
      }
      return lipp_result;
    }
    
    total_queries_++;
    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    uint64_t result = 0;
    
    // Only query DPGM if range overlaps with its key range
    if (!(upper_key < dpgm_min_key_ || lower_key > dpgm_max_key_)) {
      result += dpgm_.RangeQuery(lower_key, upper_key, thread_id);
    }
    
    // Only query LIPP if range overlaps with its key range
    if (!(upper_key < lipp_min_key_ || lower_key > lipp_max_key_)) {
      result += lipp_.RangeQuery(lower_key, upper_key, thread_id);
    }
    
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Update key ranges
    {
      std::lock_guard<std::shared_mutex> lock(mutex_);
      dpgm_min_key_ = std::min(dpgm_min_key_, data.key);
      dpgm_max_key_ = std::max(dpgm_max_key_, data.key);
    }
    
    dpgm_.Insert(data, thread_id);
    
    // Update cache
    UpdateCache(data.key, data.value);
    
    static size_t insert_count = 0;
    if (++insert_count % 110000 == 0) {
      AdaptMigrationThreshold();
      
      std::lock_guard<std::shared_mutex> lock(mutex_);
      if (dpgm_.size() > migration_threshold_ * (dpgm_.size() + lipp_.size())) {
        if (!migration_in_progress_.load()) {
          StartAsyncMigration();
        }
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
    bool expected = false;
    if (!migration_in_progress_.compare_exchange_strong(expected, true)) {
      return;
    }
    
    // Queue migration task
    {
      std::lock_guard<std::shared_mutex> lock(migration_mutex_);
      migration_queue_size_++;
      migration_cv_.notify_one();
    }
  }

  void MigrationWorker() {
    while (true) {
      std::unique_lock<std::shared_mutex> lock(migration_mutex_);
      migration_cv_.wait(lock, [this]() {
        return stop_migration_ || migration_queue_size_ > 0;
      });

      if (stop_migration_) break;

      migration_queue_size_--;
      lock.unlock();

      try {
        ProcessMigrationBatch();
      } catch (const std::exception& e) {
        std::cerr << "Migration error: " << e.what() << std::endl;
        migration_in_progress_.store(false);
      }
    }
  }

  void ProcessMigrationBatch() {
    std::vector<std::vector<KeyValue<KeyType>>> parallel_batches;
    size_t total_size = 0;
    
    {
      std::lock_guard<std::shared_mutex> lock(mutex_);
      if (dpgm_.size() == 0) {
        migration_in_progress_.store(false);
        return;
      }

      total_size = dpgm_.size();
      size_t num_batches = (total_size + parallel_migration_batch_size_ - 1) / parallel_migration_batch_size_;
      parallel_batches.resize(num_batches);
      
      // Extract data in parallel batches
      const auto& pgm = dpgm_.GetInternalData();
      auto it = pgm.lower_bound(std::numeric_limits<KeyType>::min());
      
      for (size_t i = 0; i < num_batches && it != pgm.end(); ++i) {
        parallel_batches[i].reserve(std::min(parallel_migration_batch_size_, total_size - i * parallel_migration_batch_size_));
        for (size_t j = 0; j < parallel_migration_batch_size_ && it != pgm.end(); ++j) {
          parallel_batches[i].push_back(KeyValue<KeyType>{it->key(), it->value()});
          ++it;
        }
      }
    }

    if (parallel_batches.empty()) {
      migration_in_progress_.store(false);
      return;
    }

    // Process batches in parallel
    std::vector<std::future<void>> futures;
    for (auto& batch : parallel_batches) {
      if (batch.empty()) continue;
      
      futures.push_back(std::async(std::launch::async, [this, &batch]() {
        // Sort the batch
        std::sort(batch.begin(), batch.end(), 
                  [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                    return a.key < b.key;
                  });

        // Update LIPP key ranges and build
        KeyType batch_min = std::numeric_limits<KeyType>::max();
        KeyType batch_max = std::numeric_limits<KeyType>::min();
        for (const auto& kv : batch) {
          batch_min = std::min(batch_min, kv.key);
          batch_max = std::max(batch_max, kv.key);
        }

        {
          std::lock_guard<std::shared_mutex> lock(mutex_);
          lipp_.Build(batch, 1);
          lipp_min_key_ = std::min(lipp_min_key_, batch_min);
          lipp_max_key_ = std::max(lipp_max_key_, batch_max);
          
          // Clear migrated data from DPGM
          auto& pgm = dpgm_.GetInternalData();
          for (const auto& kv : batch) {
            auto it = pgm.find(kv.key);
            if (it != pgm.end()) {
              pgm.erase(it);
            }
          }
        }
      }));
    }

    // Wait for all batches to complete
    for (auto& future : futures) {
      future.wait();
    }

    if (dpgm_.size() > 0) {
      std::lock_guard<std::shared_mutex> lock(migration_mutex_);
      migration_queue_size_++;
      migration_cv_.notify_one();
    } else {
      migration_in_progress_.store(false);
    }
  }

  void UpdateCache(const KeyType& key, size_t value) const {
    std::lock_guard<std::shared_mutex> cache_lock(cache_mutex_);
    
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      // Update existing entry
      it->second.first = value;
      cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second.second);
    } else {
      // Add new entry
      if (cache_.size() >= cache_size_) {
        // Remove least recently used
        auto lru_it = cache_lru_.back();
        cache_.erase(lru_it);
        cache_lru_.pop_back();
      }
      
      cache_lru_.push_front(key);
      cache_[key] = {value, cache_lru_.begin()};
    }
  }

  void AdaptMigrationThreshold() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_adaptation_time_).count();
    
    if (elapsed >= 60 && total_queries_ > 1000) {  // Adapt every minute if we have enough queries
      double dpgm_hit_rate = static_cast<double>(dpgm_hits_) / total_queries_;
      double lipp_hit_rate = static_cast<double>(lipp_hits_) / total_queries_;
      
      // Adjust threshold based on hit rates
      if (dpgm_hit_rate > lipp_hit_rate * 1.5) {
        // DPGM is performing better, increase its size
        migration_threshold_ = std::min(0.2, migration_threshold_ * 1.1);
      } else if (lipp_hit_rate > dpgm_hit_rate * 1.5) {
        // LIPP is performing better, decrease DPGM size
        migration_threshold_ = std::max(0.01, migration_threshold_ * 0.9);
      }
      
      // Reset counters
      dpgm_hits_ = 0;
      lipp_hits_ = 0;
      total_queries_ = 0;
      last_adaptation_time_ = now;
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dpgm_;
  Lipp<KeyType> lipp_;
  double migration_threshold_;
  mutable std::shared_mutex mutex_;
  mutable std::shared_mutex migration_mutex_;
  std::condition_variable_any migration_cv_;
  std::atomic<bool> migration_in_progress_;
  std::atomic<size_t> migration_queue_size_;
  std::vector<std::thread> migration_threads_;
  size_t batch_size_;
  bool stop_migration_;
  
  // Key range tracking
  KeyType dpgm_min_key_;
  KeyType dpgm_max_key_;
  KeyType lipp_min_key_;
  KeyType lipp_max_key_;

  // Performance tracking
  mutable std::atomic<uint64_t> dpgm_hits_;
  mutable std::atomic<uint64_t> lipp_hits_;
  mutable std::atomic<uint64_t> total_queries_;
  std::chrono::steady_clock::time_point last_adaptation_time_;

  // Cache-related members
  mutable std::shared_mutex cache_mutex_;
  mutable std::unordered_map<KeyType, std::pair<size_t, typename std::list<KeyType>::iterator>> cache_;
  mutable std::list<KeyType> cache_lru_;
  size_t cache_size_;

  // Parallel migration members
  size_t parallel_migration_batch_size_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H 