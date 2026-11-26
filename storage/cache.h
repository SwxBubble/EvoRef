#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace Delta {

class Chunk;

// Decoded-chunk cache shared by normal LRU recovery and OARS. OARS uses a
// reconstruction-aware GreedyDual priority and pins the roots of the current
// per-file dependency forest while it is traversed.
class ChunkCache {
public:
  enum class Policy { kLRU, kOars };

  explicit ChunkCache(size_t cache_size_mb);
  ~ChunkCache();

  std::shared_ptr<Chunk> get(uint32_t chunk_id);
  void add(uint32_t chunk_id, std::shared_ptr<Chunk> chunk);
  void clear(bool reset_stats = false);
  void reset_statistics();

  void configure_oars(std::vector<double> reconstruction_benefit);
  void note_oars_file_boundary();
  bool pin_oars(uint32_t chunk_id);
  std::shared_ptr<Chunk> get_oars_pinned(uint32_t chunk_id);
  void release_oars_pins();

  uint64_t hits() const { return cache_hits_; }
  uint64_t misses() const { return cache_misses_; }
  uint64_t evictions() const { return evictions_; }
  uint64_t rejected_admissions() const { return rejected_admissions_; }
  size_t priority_metadata_bytes() const {
    return reconstruction_benefit_.capacity() * sizeof(double);
  }

private:
  struct Node {
    Node *prev = nullptr;
    Node *next = nullptr;
    uint32_t chunk_id = 0;
    std::shared_ptr<Chunk> chunk;
    double priority = 0.0;
    uint64_t sequence = 0;
    uint32_t frequency = 1;
    bool pinned = false;
  };

  void insertToHead(Node *node);
  void detach(Node *node);
  void eraseNode(Node *node);
  void touchPriority(Node *node);
  double BenefitFor(uint32_t chunk_id) const;
  bool IsOars() const { return policy_ == Policy::kOars; }

  const size_t max_memory_usage_;
  size_t current_memory_ = 0;
  size_t peak_memory_ = 0;
  std::unordered_map<uint32_t, Node *> map_;
  Node *dummy_head_ = nullptr;
  Node *dummy_tail_ = nullptr;

  uint64_t cache_hits_ = 0;
  uint64_t cache_misses_ = 0;
  uint64_t evictions_ = 0;
  uint64_t rejected_admissions_ = 0;
  Policy policy_ = Policy::kLRU;

  std::vector<double> reconstruction_benefit_;
  uint64_t access_sequence_ = 0;
  using PriorityKey = std::tuple<double, uint64_t, uint32_t>;
  std::set<PriorityKey> priority_order_;

  uint64_t file_boundary_events_ = 0;
  uint64_t cross_file_retained_entries_total_ = 0;
  uint64_t cross_file_retained_entries_max_ = 0;
  uint64_t cross_file_retained_bytes_total_ = 0;
  uint64_t cross_file_retained_bytes_max_ = 0;
  uint64_t pinned_hits_ = 0;
  uint64_t peak_pinned_entries_ = 0;
  uint64_t current_pinned_entries_ = 0;
  uint64_t oars_admissions_ = 0;
  uint64_t zero_utility_skips_ = 0;
  double inflation_ = 0.0;
};

} // namespace Delta
