#pragma once

#include "feature/features.h"
#include <array>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace Delta {
using chunk_id = uint32_t;

// Compact Progressive Adaptive Conditional Subfeature Index (P-ACSI).
//
// It keeps the 12 independent ODESS sub-feature entry points, but refines only
// a hot primary bucket.  Once a posting list grows beyond kSplitThreshold, the
// index chooses the companion sub-feature that most evenly partitions the
// existing IDs and turns that bucket into a conditional second-level map.
class AdaptiveOdessSubfeatureIndex {
public:
  static constexpr int kFeatureCount = 12;
  // Conditional refinement can hide one primary-only vote.  Admit a
  // three-observed-match candidate to the real xdelta validation stage; large
  // deltas are still rejected by the writer policy.
  static constexpr uint32_t kMinMatchCount = 3;
  static constexpr size_t kSplitThreshold = 32;
  static constexpr size_t kFallbackRepresentatives = 48;
  static constexpr float kCompactMaxLoadFactor = 2.0f;

  explicit AdaptiveOdessSubfeatureIndex(
      uint32_t confident_match_count = 4,
      uint32_t confidence_margin = 1, size_t fallback_initial_budget = 8,
      size_t fallback_max_budget = kFallbackRepresentatives,
      bool depth_aware_tiebreak = false,
      uint32_t depth_match_tolerance = 0,
      size_t split_threshold = kSplitThreshold)
      : confident_match_count_(confident_match_count),
        confidence_margin_(confidence_margin),
        fallback_initial_budget_(fallback_initial_budget),
        fallback_max_budget_(fallback_max_budget),
        depth_aware_tiebreak_(depth_aware_tiebreak),
        depth_match_tolerance_(depth_match_tolerance),
        split_threshold_(split_threshold) {
    for (auto &table : compact_tables_) {
      table.max_load_factor(kCompactMaxLoadFactor);
    }
  }
  ~AdaptiveOdessSubfeatureIndex();

  std::vector<std::pair<chunk_id, uint32_t>>
  GetTopKCandidates(const Feature &feat, int k);
  void AddFeature(const Feature &feat, chunk_id id);
  void SetChainDepthView(const std::vector<uint16_t> *chain_depths) {
    chain_depths_ = chain_depths;
  }

private:
  using FeatureKey = uint32_t;
  using RefSlot = uint32_t;

  // OdessSubfeatures currently derives each transform from uint32_t
  // arithmetic; retain that actual domain instead of duplicating it as 64-bit
  // values in the auxiliary split registry.
  using StoredFeature = std::array<uint32_t, kFeatureCount>;

  // Most P-ACSI leaves contain only one or two references. std::vector uses a
  // 24-byte object plus a heap allocation even for those leaves. This 16-byte
  // posting keeps up to two dense reference slots inline and promotes to a
  // vector only when a third slot arrives.
  class CompactPosting {
  public:
    CompactPosting() = default;
    ~CompactPosting();
    CompactPosting(const CompactPosting &) = delete;
    CompactPosting &operator=(const CompactPosting &) = delete;
    CompactPosting(CompactPosting &&other) noexcept;
    CompactPosting &operator=(CompactPosting &&other) noexcept;

    void push_back(RefSlot slot);
    size_t size() const { return size_; }
    size_t capacity() const;
    size_t dynamic_bytes() const;
    bool uses_heap() const { return heap_; }
    const RefSlot *begin() const;
    const RefSlot *end() const;
    RefSlot operator[](size_t index) const;
    bool has_external_pointer() const;
    void *external_pointer() const;
    void set_external_pointer(void *pointer);
    void *release_external_pointer();

  private:
    static constexpr size_t kInlineCapacity = 2;
    static constexpr uint32_t kExternalPointerSize =
        std::numeric_limits<uint32_t>::max();
    union Storage {
      RefSlot inline_slots[kInlineCapacity];
      std::vector<RefSlot> *overflow;
      void *external;
      Storage() : inline_slots{0, 0} {}
    } storage_;
    uint32_t size_ = 0;
    bool heap_ = false;
  };

  struct CompactConditionalBucket;

  class CompactBucket {
  public:
    CompactBucket() = default;
    ~CompactBucket();
    CompactBucket(const CompactBucket &) = delete;
    CompactBucket &operator=(const CompactBucket &) = delete;
    CompactBucket(CompactBucket &&other) noexcept;
    CompactBucket &operator=(CompactBucket &&other) noexcept;

    bool is_conditional() const {
      return storage_.has_external_pointer();
    }
    CompactPosting &flat_slots() { return storage_; }
    const CompactPosting &flat_slots() const { return storage_; }
    CompactConditionalBucket *conditional();
    const CompactConditionalBucket *conditional() const;
    void SetConditional(
        std::unique_ptr<CompactConditionalBucket> conditional);

  private:
    CompactPosting storage_;
  };

  struct CompactConditionalBucket {
    uint8_t secondary_feature = 0;
    uint64_t seen_slots = 0;
    std::unordered_map<FeatureKey, CompactPosting> partitions;
    std::vector<RefSlot> fallback_slots;
  };

  void SplitCompactBucket(int primary_feature, CompactBucket &bucket);
  const StoredFeature &StoredFeaturesForSlot(RefSlot slot) const;
  static FeatureKey ToFeatureKey(uint64_t feature);
  chunk_id ChunkIdForCandidate(chunk_id candidate) const;
  uint16_t ChainDepthForCandidate(chunk_id candidate) const;
  void AddCompactFeature(const std::vector<uint64_t> &features, chunk_id id);
  void InsertCompactReference(const StoredFeature &stored, chunk_id id);
  void IndexCompactReference(RefSlot slot, const StoredFeature &stored,
                             chunk_id id);
  void StartVoteEpoch();
  void AddVote(chunk_id id);
  std::vector<std::pair<chunk_id, uint32_t>> SelectTopK(int k);
  uint16_t ChainDepthFor(chunk_id id) const;
  bool HasConfidentWinner() const;
  std::vector<std::pair<chunk_id, uint32_t>>
  GetCompactProgressiveCandidates(const std::vector<uint64_t> &features,
                                  int k);
  std::array<std::unordered_map<FeatureKey, CompactBucket>, kFeatureCount>
      compact_tables_;

  // Compact mode stores one dense slot per admitted reference. All postings
  // and vote arrays use the slot; only this vector maps back to storage IDs.
  // Deque avoids vector's power-of-two spare capacity for the 48-byte feature
  // records while retaining constant-time dense-slot lookup.
  std::deque<StoredFeature> compact_stored_features_;
  std::vector<chunk_id> compact_chunk_ids_;

  // Compact P-ACSI progressive query policy.
  uint32_t confident_match_count_ = 4;
  uint32_t confidence_margin_ = 1;
  size_t fallback_initial_budget_ = 8;
  size_t fallback_max_budget_ = kFallbackRepresentatives;
  bool depth_aware_tiebreak_ = false;
  uint32_t depth_match_tolerance_ = 0;
  size_t split_threshold_ = kSplitThreshold;
  const std::vector<uint16_t> *chain_depths_ = nullptr;

  // Allocation-free, generation-stamped vote accumulator used only by the
  // progressive path. Votes fit in uint8_t because there are 12 tables.
  std::vector<uint8_t> vote_counts_;
  std::vector<uint32_t> vote_epochs_;
  std::vector<chunk_id> touched_ids_;
  uint32_t vote_epoch_ = 0;

};

} // namespace Delta
