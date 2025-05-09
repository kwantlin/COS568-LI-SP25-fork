#ifndef TLI_DYNAMIC_PGM_H
#define TLI_DYNAMIC_PGM_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"

template <class KeyType, class SearchClass, size_t pgm_error>
class DynamicPGM : public Competitor<KeyType, SearchClass> {
 public:
  DynamicPGM(const std::vector<int>& params){}
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    uint64_t build_time =
        util::timing([&] { pgm_ = decltype(pgm_)(loading_data.begin(), loading_data.end()); });

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    auto it = pgm_.find(lookup_key);

    uint64_t guess;
    if (it == pgm_.end()) {
      guess = util::OVERFLOW;
    } else {
      guess = it->value();
    }

    return guess;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    auto it = pgm_.lower_bound(lower_key);
    uint64_t result = 0;
    while(it != pgm_.end() && it->key() <= upper_key){
      result += it->value();
      ++it;
    }
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    pgm_.insert(data.key, data.value);
  }

  std::string name() const { return "DynamicPGM"; }

  std::size_t size() const { return pgm_.size_in_bytes(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
    std::string name = SearchClass::name();
    return name != "LinearAVX" && !multithread;
  }

  std::vector<std::string> variants() const { 
    std::vector<std::string> vec;
    vec.push_back(SearchClass::name());
    vec.push_back(std::to_string(pgm_error));
    return vec;
  }

  // Get direct access to the internal data structure
  const auto& GetInternalData() const { return pgm_; }

  // Get all non-deleted items without blocking
  std::vector<KeyValue<KeyType>> GetAllItemsNonBlocking() const {
    std::vector<KeyValue<KeyType>> result;
    const auto& pgm = GetInternalData();
    
    // Pre-allocate space for efficiency - use a reasonable default size
    result.reserve(1000000);  // 1M entries as default capacity
    
    // Get a snapshot of the current state
    auto snapshot = pgm;
    
    // Get the first key
    auto it = snapshot.lower_bound(std::numeric_limits<KeyType>::min());
    if (it == snapshot.end()) return result;
    
    // Iterate through all keys in the snapshot
    while (it != snapshot.end()) {
      // Use EqualityLookup to check if item exists and get its value
      size_t value = EqualityLookup(it->key(), 0);
      if (value != util::NOT_FOUND) {
        result.emplace_back(it->key(), value);
      }
      ++it;
    }
    
    return result;
  }

 private:
  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_;
};

#endif  // TLI_DYNAMIC_PGM_H
