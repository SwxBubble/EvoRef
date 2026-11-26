#include "storage/cache.h"

#include "chunk/chunk.h"

#include <algorithm>

namespace Delta {

ChunkCache::ChunkCache(size_t cache_size_mb)
    : max_memory_usage_(cache_size_mb * 1024 * 1024),
      dummy_head_(new Node), dummy_tail_(new Node) {
  dummy_head_->next = dummy_tail_;
  dummy_tail_->prev = dummy_head_;
}

ChunkCache::~ChunkCache() {
  Node *node = dummy_head_;
  while (node != nullptr) {
    Node *next = node->next;
    delete node;
    node = next;
  }
}

std::shared_ptr<Chunk> ChunkCache::get(uint32_t chunk_id) {
  const auto found = map_.find(chunk_id);
  if (found == map_.end()) {
    cache_misses_++;
    return nullptr;
  }
  cache_hits_++;
  Node *node = found->second;
  if (IsOars()) {
    touchPriority(node);
  } else {
    detach(node);
    insertToHead(node);
  }
  return node->chunk;
}

void ChunkCache::add(uint32_t chunk_id, std::shared_ptr<Chunk> chunk) {
  if (chunk->len() > max_memory_usage_ || max_memory_usage_ == 0) {
    rejected_admissions_++;
    return;
  }

  const auto existing = map_.find(chunk_id);
  if (existing != map_.end()) {
    if (!IsOars()) {
      detach(existing->second);
      insertToHead(existing->second);
    } else if (!existing->second->pinned) {
      touchPriority(existing->second);
    }
    return;
  }

  const double static_utility =
      BenefitFor(chunk_id) / std::max<size_t>(1, chunk->len());
  if (IsOars() && static_utility <= 0.0) {
    zero_utility_skips_++;
    return;
  }
  double incoming_priority =
      IsOars() ? inflation_ + static_utility : static_utility;

  while (!map_.empty() &&
         current_memory_ + chunk->len() > max_memory_usage_) {
    Node *evict = dummy_tail_->prev;
    if (IsOars()) {
      if (priority_order_.empty()) {
        rejected_admissions_++;
        return;
      }
      const auto &lowest = *priority_order_.begin();
      if (incoming_priority < std::get<0>(lowest)) {
        rejected_admissions_++;
        return;
      }
      inflation_ = std::max(inflation_, std::get<0>(lowest));
      incoming_priority = inflation_ + static_utility;
      evict = map_.at(std::get<2>(lowest));
    }
    eraseNode(evict);
    evictions_++;
  }

  auto *node = new Node;
  node->chunk_id = chunk_id;
  node->chunk = std::move(chunk);
  node->priority = incoming_priority;
  node->sequence = ++access_sequence_;
  map_[chunk_id] = node;
  insertToHead(node);
  current_memory_ += node->chunk->len();
  peak_memory_ = std::max(peak_memory_, current_memory_);
  if (IsOars()) {
    priority_order_.emplace(node->priority, node->sequence, node->chunk_id);
    oars_admissions_++;
  }
}

void ChunkCache::insertToHead(Node *node) {
  Node *head_next = dummy_head_->next;
  dummy_head_->next = node;
  node->prev = dummy_head_;
  head_next->prev = node;
  node->next = head_next;
}

void ChunkCache::detach(Node *node) {
  node->prev->next = node->next;
  node->next->prev = node->prev;
  node->prev = nullptr;
  node->next = nullptr;
}

void ChunkCache::eraseNode(Node *node) {
  if (IsOars()) {
    priority_order_.erase(
        PriorityKey{node->priority, node->sequence, node->chunk_id});
  }
  detach(node);
  current_memory_ -= node->chunk->len();
  map_.erase(node->chunk_id);
  delete node;
}

void ChunkCache::touchPriority(Node *node) {
  priority_order_.erase(
      PriorityKey{node->priority, node->sequence, node->chunk_id});
  node->sequence = ++access_sequence_;
  priority_order_.emplace(node->priority, node->sequence, node->chunk_id);
}

double ChunkCache::BenefitFor(uint32_t chunk_id) const {
  if (!IsOars() || chunk_id >= reconstruction_benefit_.size()) return 0.0;
  return reconstruction_benefit_[chunk_id];
}

void ChunkCache::clear(bool reset_stats) {
  Node *node = dummy_head_->next;
  while (node != dummy_tail_) {
    Node *next = node->next;
    delete node;
    node = next;
  }
  dummy_head_->next = dummy_tail_;
  dummy_tail_->prev = dummy_head_;
  map_.clear();
  priority_order_.clear();
  current_memory_ = 0;
  if (reset_stats) reset_statistics();
}

void ChunkCache::reset_statistics() {
  cache_hits_ = 0;
  cache_misses_ = 0;
  evictions_ = 0;
  rejected_admissions_ = 0;
  peak_memory_ = current_memory_;
  file_boundary_events_ = 0;
  cross_file_retained_entries_total_ = 0;
  cross_file_retained_entries_max_ = 0;
  cross_file_retained_bytes_total_ = 0;
  cross_file_retained_bytes_max_ = 0;
  pinned_hits_ = 0;
  peak_pinned_entries_ = 0;
  current_pinned_entries_ = 0;
  oars_admissions_ = 0;
  zero_utility_skips_ = 0;
  inflation_ = 0.0;
}

void ChunkCache::configure_oars(
    std::vector<double> reconstruction_benefit) {
  clear(true);
  reconstruction_benefit_ = std::move(reconstruction_benefit);
  policy_ = Policy::kOars;
  inflation_ = 0.0;
}

void ChunkCache::note_oars_file_boundary() {
  if (!IsOars()) return;
  file_boundary_events_++;
  cross_file_retained_entries_total_ += map_.size();
  cross_file_retained_entries_max_ =
      std::max<uint64_t>(cross_file_retained_entries_max_, map_.size());
  cross_file_retained_bytes_total_ += current_memory_;
  cross_file_retained_bytes_max_ =
      std::max<uint64_t>(cross_file_retained_bytes_max_, current_memory_);
}

bool ChunkCache::pin_oars(uint32_t chunk_id) {
  if (!IsOars()) return false;
  const auto found = map_.find(chunk_id);
  if (found == map_.end()) {
    cache_misses_++;
    return false;
  }
  cache_hits_++;
  pinned_hits_++;
  Node *node = found->second;
  if (!node->pinned) {
    priority_order_.erase(
        PriorityKey{node->priority, node->sequence, node->chunk_id});
    node->frequency++;
    const double static_utility =
        BenefitFor(node->chunk_id) /
        std::max<size_t>(1, node->chunk->len());
    node->priority = inflation_ + node->frequency * static_utility;
    node->pinned = true;
    current_pinned_entries_++;
    peak_pinned_entries_ =
        std::max(peak_pinned_entries_, current_pinned_entries_);
  }
  return true;
}

std::shared_ptr<Chunk> ChunkCache::get_oars_pinned(uint32_t chunk_id) {
  if (!IsOars()) return nullptr;
  const auto found = map_.find(chunk_id);
  if (found == map_.end() || !found->second->pinned) return nullptr;
  return found->second->chunk;
}

void ChunkCache::release_oars_pins() {
  if (!IsOars()) return;
  for (auto &entry : map_) {
    Node *node = entry.second;
    if (!node->pinned) continue;
    node->pinned = false;
    node->sequence = ++access_sequence_;
    priority_order_.emplace(node->priority, node->sequence, node->chunk_id);
  }
  current_pinned_entries_ = 0;
}

} // namespace Delta
